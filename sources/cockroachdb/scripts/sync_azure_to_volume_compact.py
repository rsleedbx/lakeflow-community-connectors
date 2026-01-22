#!/usr/bin/env python3
"""
Sync Azure Blob Storage to Databricks Volume (Compact Version)

Supports both Parquet and JSON CDC files.
Uses persistent local cache to avoid re-downloading files.

Cache Strategy:
- Downloads to: $GIT_ROOT/.cache/cdc_test_data/{prefix}/
- Reuses cached files for subsequent syncs and diagnostics
- Cache is preserved across runs for efficiency

Usage:
    python3 sync_azure_to_volume_compact.py [--prefix test-parquet_usertable_with_split]
    python3 sync_azure_to_volume_compact.py [--prefix json/defaultdb/public/test-json_usertable_with_split]
    python3 sync_azure_to_volume_compact.py [--no-cache]  # Force re-download
"""

import argparse
import json
import os
import sys
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from rich.console import Console
from rich.progress import track, Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


def load_config():
    """Load configs from JSON."""
    git_root = Path(__file__).resolve().parents[3]
    azure_json = git_root / "sources/cockroachdb/.env/cockroachdb_cdc_azure.json"
    pipeline_json = git_root / "sources/cockroachdb/.env/cockroachdb_pipelines.json"
    
    if not azure_json.exists() or not pipeline_json.exists():
        console.print("[red]❌ Missing config files. Run: 01_azure_storage.sh[/red]")
        sys.exit(1)
    
    return json.load(open(azure_json)), json.load(open(pipeline_json)), git_root


def get_cache_dir(git_root: Path, prefix: str) -> Path:
    """
    Get persistent cache directory for this prefix.
    
    Cache location: $GIT_ROOT/.cache/cdc_test_data/{prefix}/
    
    Example:
        prefix: json/defaultdb/public/test-json_usertable_no_split/1767823340
        cache:  .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/
    """
    cache_dir = git_root / ".cache" / "cdc_test_data" / prefix
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_blob_client(azure_config):
    """Create Azure blob client."""
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={azure_config['azure_storage_account']};"
        f"AccountKey={azure_config['azure_storage_key']};"
        f"EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(conn_str)


def ensure_catalog_resources(w, catalog, schema, volume, volume_type):
    """Create schema and volume if needed."""
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
        progress.add_task("Creating schema and volume...", total=None)
        
        try:
            w.schemas.create(catalog_name=catalog, name=schema)
        except Exception as e:
            if "ALREADY_EXISTS" not in str(e):
                console.print(f"[yellow]Schema: {e}[/yellow]")
        
        try:
            vol_type = VolumeType.MANAGED if volume_type.upper() == "MANAGED" else VolumeType.EXTERNAL
            w.volumes.create(catalog_name=catalog, schema_name=schema, name=volume, volume_type=vol_type)
        except Exception as e:
            if "ALREADY_EXISTS" not in str(e):
                console.print(f"[yellow]Volume: {e}[/yellow]")


def list_azure_files(azure_config, prefix):
    """List CDC files (both Parquet and JSON) from Azure."""
    blob_service = get_blob_client(azure_config)
    container = blob_service.get_container_client(azure_config['azure_storage_container'])
    
    return [
        blob.name for blob in container.list_blobs(name_starts_with=f"{prefix}/")
        if blob.name.endswith(('.parquet', '.ndjson', '.json'))
    ]


def list_volume_files(w, volume_path):
    """
    List CDC files (both Parquet and JSON) in Volume.
    Returns a set of filenames (without directory path) for backward compatibility.
    This allows the existing check to work with both flat and nested structures.
    """
    try:
        def list_recursive(path):
            """Recursively list all files in directory and subdirectories."""
            filenames = set()
            try:
                items = w.files.list_directory_contents(path)
                for item in items:
                    if item.is_dir:
                        # Recursively list subdirectories
                        filenames.update(list_recursive(item.path))
                    elif item.path.endswith(('.parquet', '.ndjson', '.json')):
                        # Add filename (not full path) for backward compat
                        filenames.add(Path(item.path).name)
            except Exception:
                pass  # Directory might not exist yet
            return filenames
        
        return list_recursive(volume_path)
    except:
        return set()


def sync_files(azure_config, w, blobs, volume_path, existing, cache_dir: Path, use_cache: bool = True):
    """
    Download from Azure (with caching), upload to Volume.
    
    Args:
        azure_config: Azure storage configuration
        w: Databricks workspace client
        blobs: List of blob names to sync
        volume_path: Target volume path
        existing: Set of existing filenames in volume (flat list for backward compat)
        cache_dir: Persistent cache directory
        use_cache: Whether to use cache (default: True)
    
    Returns:
        Tuple of (copied, skipped, failed, cached) counts
    """
    blob_service = get_blob_client(azure_config)
    container = blob_service.get_container_client(azure_config['azure_storage_container'])
    
    copied = skipped = failed = cached = 0
    
    for blob_name in track(blobs, description="Syncing files..."):
        # Extract relative path from blob name (after the timestamp directory)
        # Example: json/defaultdb/public/test-json_usertable_with_split/1769028478/_metadata/schema.json
        #       -> _metadata/schema.json
        blob_path = Path(blob_name)
        parts = blob_path.parts
        
        # Find the index after the timestamp directory (10-digit number)
        timestamp_idx = None
        for i, part in enumerate(parts):
            if part.isdigit() and len(part) == 10:
                timestamp_idx = i
                break
        
        if timestamp_idx is not None and timestamp_idx + 1 < len(parts):
            # Get relative path after timestamp (preserves _metadata/ subdirectory)
            relative_path = Path(*parts[timestamp_idx + 1:])
        else:
            # Fallback: just use filename (backward compat)
            relative_path = Path(blob_path.name)
        
        # For backward compatibility with existing check
        filename = blob_path.name
        
        if filename in existing:
            skipped += 1
            continue
        
        try:
            # Check cache first (use flat filename for cache to keep it simple)
            cached_file = cache_dir / filename
            
            if use_cache and cached_file.exists():
                # Use cached file (no download needed!)
                cached += 1
                console.print(f"  [cyan]⚡ {relative_path} (from cache)[/cyan]", highlight=False)
            else:
                # Download from Azure and save to cache
                blob_client = container.get_blob_client(blob_name)
                cached_file.write_bytes(blob_client.download_blob().readall())
                console.print(f"  [green]⬇️  {relative_path} (downloaded)[/green]", highlight=False)
            
            # Upload to Volume preserving directory structure
            target_path = f"{volume_path}/{relative_path}"
            
            # Create parent directory if needed (for _metadata/ subdirectory)
            if relative_path.parent != Path('.'):
                parent_dir = f"{volume_path}/{relative_path.parent}"
                try:
                    w.files.create_directory(parent_dir)
                except Exception:
                    pass  # Directory might already exist
            
            # Upload file
            with open(cached_file, 'rb') as f:
                w.files.upload(target_path, f)
            
            copied += 1
        except Exception as e:
            console.print(f"[red]✗ {relative_path}: {e}[/red]")
            failed += 1
    
    return copied, skipped, failed, cached


def main():
    parser = argparse.ArgumentParser(description="Sync Azure to Databricks Volume")
    parser.add_argument("--prefix", help="Blob prefix (e.g., test-parquet_usertable_with_split)")
    parser.add_argument("--subdir", help="Volume subdirectory for this test (e.g., test-parquet_usertable_with_split)")
    parser.add_argument("--no-cache", action="store_true", help="Force re-download (skip cache)")
    args = parser.parse_args()
    
    console.rule("[bold blue]Azure → Databricks Volume Sync[/bold blue]")
    
    # Load config
    azure_config, pipeline_config, git_root = load_config()
    prefix = args.prefix or pipeline_config['blob_prefix']
    use_cache = not args.no_cache
    
    catalog = pipeline_config['catalog']
    schema = pipeline_config['schema']
    volume = pipeline_config['volume_name']
    volume_type = pipeline_config.get('volume_type', 'MANAGED')
    
    # Get persistent cache directory
    cache_dir = get_cache_dir(git_root, prefix)
    
    # Add subdirectory if specified (for test scenarios)
    if args.subdir:
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{args.subdir}"
    else:
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    
    # Show config
    config_table = Table(title="Configuration", show_header=False)
    config_table.add_column("Key", style="cyan")
    config_table.add_column("Value", style="green")
    config_table.add_row("Azure Account", azure_config['azure_storage_account'])
    config_table.add_row("Container", azure_config['azure_storage_container'])
    config_table.add_row("Prefix", prefix)
    config_table.add_row("Volume", f"{catalog}.{schema}.{volume}")
    config_table.add_row("Path", volume_path)
    config_table.add_row("Cache", str(cache_dir) if use_cache else "[red]Disabled[/red]")
    console.print(config_table)
    
    # Initialize clients
    w = WorkspaceClient()
    
    # Ensure resources exist
    ensure_catalog_resources(w, catalog, schema, volume, volume_type)
    
    # List files
    console.print(f"\n[bold]Listing Azure files...[/bold]")
    blobs = list_azure_files(azure_config, prefix)
    
    if not blobs:
        console.print(f"[red]❌ No files found: {prefix}/[/red]")
        sys.exit(1)
    
    console.print(f"[green]✓ Found {len(blobs)} CDC files[/green]")
    
    existing = list_volume_files(w, volume_path)
    console.print(f"[dim]({len(existing)} already in Volume)[/dim]")
    
    # Sync
    copied, skipped, failed, cached = sync_files(
        azure_config, w, blobs, volume_path, existing, cache_dir, use_cache
    )
    
    # Summary
    summary = Table(title="Sync Summary", show_header=False)
    summary.add_column("Metric", style="cyan")
    summary.add_column("Count", style="green")
    summary.add_row("Copied to Volume", str(copied))
    summary.add_row("Skipped (already in Volume)", str(skipped))
    summary.add_row("Used from Cache", f"[cyan]{cached}[/cyan]" if cached > 0 else str(cached))
    summary.add_row("Downloaded from Azure", f"[green]{copied - cached}[/green]" if copied > cached else str(copied - cached))
    summary.add_row("Failed", str(failed) if failed == 0 else f"[red]{failed}[/red]")
    summary.add_row("Total in Volume", str(len(existing) + copied))
    console.print(summary)
    
    if failed > 0:
        console.print("[red]⚠️  Some files failed[/red]")
        sys.exit(1)
    
    console.print(f"\n[bold green]✅ Sync Complete![/bold green]")
    console.print(f"[dim]Next: Use {volume_path} in notebooks[/dim]\n")


if __name__ == "__main__":
    main()

