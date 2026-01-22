#!/usr/bin/env python3
"""
Helper script to list files in Unity Catalog Volumes.

Usage:
    python3 list_volume_files.py /Volumes/catalog/schema/volume/path/
    python3 list_volume_files.py --count --pattern "*.parquet" /Volumes/catalog/schema/volume/

The databricks CLI 'databricks fs ls' command does NOT work with Unity Catalog Volumes.
This script uses the Databricks SDK instead.
"""

import argparse
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


def main():
    parser = argparse.ArgumentParser(
        description="List files in Unity Catalog Volumes (replacement for 'databricks fs ls')"
    )
    parser.add_argument("volume_path", help="Volume path like /Volumes/catalog/schema/volume/")
    parser.add_argument("--pattern", help="Glob pattern to filter files (e.g., '*.parquet')")
    parser.add_argument("--count", action="store_true", help="Only output count of files")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks profile (default: DEFAULT)")
    
    args = parser.parse_args()
    
    sys.exit(list_volume_files(args.volume_path, args.pattern, args.count, args.profile))


if __name__ == "__main__":
    main()
