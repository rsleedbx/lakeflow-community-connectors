#!/usr/bin/env python3
"""
Helper script to list files in Unity Catalog Volumes.

Usage:
    python3 list_volume_files.py /Volumes/catalog/schema/volume/path/
    python3 list_volume_files.py --count --pattern "*.parquet" /Volumes/catalog/schema/volume/
    python3 list_volume_files.py --validate /Volumes/catalog/schema/volume/

The databricks CLI 'databricks fs ls' command does NOT work with Unity Catalog Volumes.
This script uses the Databricks SDK instead.
"""

import argparse
import json
import re
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient


def list_volume_files(volume_path: str, pattern: str = None, count_only: bool = False, profile: str = "DEFAULT"):
    """
    List files in a Unity Catalog Volume using the Databricks SDK.
    
    Args:
        volume_path: Full path like /Volumes/catalog/schema/volume/subdir/
        pattern: Optional glob pattern (e.g., "*.parquet", "*.ndjson")
        count_only: If True, only return count instead of listing files
        profile: Databricks profile to use
    """
    try:
        w = WorkspaceClient(profile=profile)
        files = list(w.files.list_directory_contents(volume_path))
        
        # Filter by pattern if provided
        if pattern:
            import fnmatch
            files = [f for f in files if fnmatch.fnmatch(Path(f.path).name, pattern)]
        
        if count_only:
            print(len(files))
        else:
            for f in files:
                # Output format similar to 'databricks fs ls'
                # but simpler (just the path)
                print(f.path)
        
        return 0
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def validate_volume(volume_path: str, profile: str = "DEFAULT"):
    """
    Validate volume structure for CockroachDB CDC data.
    
    Returns JSON with structure:
    {
        "accessible": true/false,
        "error": "error message if not accessible",
        "has_timestamped_dirs": true/false,
        "timestamp_dirs": ["2024-01-22_10-15-30", ...],
        "timestamp_dir_count": 0,
        "sample_timestamp_dir": "2024-01-22_10-15-30",
        "sample_dir_file_count": 0,
        "root_file_count": 0,
        "has_metadata_dir": true/false,
        "has_schema_file": true/false,
        "data_file_pattern": "\\.parquet$|\\.json$|\\.ndjson$"
    }
    
    Args:
        volume_path: Full path like /Volumes/catalog/schema/volume/
        profile: Databricks profile to use
    """
    result = {
        "accessible": False,
        "error": None,
        "has_timestamped_dirs": False,
        "timestamp_dirs": [],
        "timestamp_dir_count": 0,
        "sample_timestamp_dir": None,
        "sample_dir_file_count": 0,
        "root_file_count": 0,
        "has_metadata_dir": False,
        "has_schema_file": False,
        "data_file_pattern": r"\.(parquet|json|ndjson)$"
    }
    
    try:
        w = WorkspaceClient(profile=profile)
        
        # Test volume access
        try:
            root_files = list(w.files.list_directory_contents(volume_path))
            result["accessible"] = True
        except Exception as e:
            result["error"] = str(e)
            return result
        
        # Pattern for timestamped directories (YYYY-MM-DD_HH-MM-SS)
        timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$')
        data_file_pattern = re.compile(result["data_file_pattern"])
        
        # Analyze root directory contents
        timestamp_dirs = []
        metadata_found = False
        root_data_files = 0
        
        for item in root_files:
            item_name = Path(item.path).name
            
            # Check for timestamped directories
            if item.is_directory and timestamp_pattern.match(item_name):
                timestamp_dirs.append(item_name)
            
            # Check for _metadata directory
            if item.is_directory and item_name == "_metadata":
                metadata_found = True
            
            # Check for data files in root
            if item.is_file and data_file_pattern.search(item_name):
                root_data_files += 1
        
        result["timestamp_dirs"] = sorted(timestamp_dirs, reverse=True)  # Newest first
        result["timestamp_dir_count"] = len(timestamp_dirs)
        result["has_timestamped_dirs"] = len(timestamp_dirs) > 0
        result["root_file_count"] = root_data_files
        result["has_metadata_dir"] = metadata_found
        
        # If we have timestamped directories, check the first one for files
        if result["has_timestamped_dirs"]:
            first_timestamp_dir = result["timestamp_dirs"][0]
            result["sample_timestamp_dir"] = first_timestamp_dir
            
            sample_dir_path = f"{volume_path.rstrip('/')}/{first_timestamp_dir}/"
            try:
                sample_files = list(w.files.list_directory_contents(sample_dir_path))
                sample_data_files = [
                    f for f in sample_files
                    if f.is_file and data_file_pattern.search(Path(f.path).name)
                ]
                result["sample_dir_file_count"] = len(sample_data_files)
            except Exception as e:
                result["error"] = f"Cannot read sample directory {first_timestamp_dir}: {e}"
        
        # Check for _metadata/schema.json
        if metadata_found:
            metadata_path = f"{volume_path.rstrip('/')}/_metadata/"
            try:
                metadata_files = list(w.files.list_directory_contents(metadata_path))
                result["has_schema_file"] = any(
                    Path(f.path).name == "schema.json" for f in metadata_files
                )
            except Exception:
                pass  # Metadata dir exists but couldn't read it
        
        return result
    
    except Exception as e:
        result["error"] = f"Unexpected error: {e}"
        return result


def main():
    parser = argparse.ArgumentParser(
        description="List files in Unity Catalog Volumes (replacement for 'databricks fs ls')"
    )
    parser.add_argument("volume_path", help="Volume path like /Volumes/catalog/schema/volume/")
    parser.add_argument("--pattern", help="Glob pattern to filter files (e.g., '*.parquet')")
    parser.add_argument("--count", action="store_true", help="Only output count of files")
    parser.add_argument("--validate", action="store_true", help="Validate volume structure for CDC data (outputs JSON)")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks profile (default: DEFAULT)")
    
    args = parser.parse_args()
    
    # Handle validation mode
    if args.validate:
        result = validate_volume(args.volume_path, args.profile)
        print(json.dumps(result, indent=2))
        sys.exit(0 if result["accessible"] else 1)
    
    # Handle standard listing mode
    sys.exit(list_volume_files(args.volume_path, args.pattern, args.count, args.profile))


if __name__ == "__main__":
    main()
