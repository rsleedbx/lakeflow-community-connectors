#!/usr/bin/env python3
"""
Migrate existing volume files to scenario-based structure.

This script moves parquet files from the base volume directory into a scenario subdirectory
to be compatible with the test_cdc_scenario.ipynb notebook.

Usage:
    # In Databricks notebook:
    %run ./migrate_volume_to_scenarios.py

    # Or as standalone Python (requires databricks-connect):
    python migrate_volume_to_scenarios.py
"""

import json
import os
from pathlib import Path

def migrate_volume_files(
    catalog: str,
    schema: str,
    volume: str,
    scenario_name: str = "existing-parquet-files",
    dry_run: bool = True
):
    """
    Migrate parquet files from base volume to scenario subdirectory.
    
    Args:
        catalog: Databricks catalog name
        schema: Databricks schema name
        volume: Volume name
        scenario_name: Name for the scenario subdirectory
        dry_run: If True, only print what would be done (don't actually move files)
    """
    try:
        # Try to import dbutils (Databricks environment)
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        in_databricks = True
    except:
        in_databricks = False
        print("⚠️  Not running in Databricks environment")
        print("   This script requires Databricks dbutils")
        return
    
    # Construct paths
    base_path = f"dbfs:/Volumes/{catalog}/{schema}/{volume}"
    scenario_path = f"{base_path}/{scenario_name}"
    
    print("=" * 80)
    print("VOLUME MIGRATION")
    print("=" * 80)
    print(f"Base path: {base_path}")
    print(f"Scenario path: {scenario_path}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will move files)'}")
    print("=" * 80)
    print()
    
    # Create scenario directory
    if not dry_run:
        try:
            dbutils.fs.mkdirs(scenario_path)
            print(f"✅ Created directory: {scenario_path}")
        except Exception as e:
            print(f"ℹ️  Directory exists or created: {scenario_path}")
    else:
        print(f"[DRY RUN] Would create: {scenario_path}")
    
    print()
    
    # List files in base directory
    try:
        files = dbutils.fs.ls(base_path)
    except Exception as e:
        print(f"❌ Error listing files: {e}")
        return
    
    # Filter for parquet files (exclude subdirectories and special files)
    parquet_files = [
        f for f in files 
        if f.name.endswith('.parquet') and not f.name.startswith('_')
    ]
    
    if not parquet_files:
        print("⚠️  No parquet files found in base directory")
        print("   Files may already be in subdirectories")
        return
    
    print(f"Found {len(parquet_files)} parquet files to migrate:")
    print()
    
    # Move files
    moved_count = 0
    failed_count = 0
    
    for file in parquet_files:
        source = file.path
        target = f"{scenario_path}/{file.name}"
        
        if dry_run:
            print(f"[DRY RUN] Would move:")
            print(f"  FROM: {file.name}")
            print(f"  TO:   {scenario_name}/{file.name}")
            moved_count += 1
        else:
            try:
                dbutils.fs.mv(source, target)
                print(f"✅ Moved: {file.name}")
                moved_count += 1
            except Exception as e:
                print(f"❌ Failed: {file.name}")
                print(f"   Error: {e}")
                failed_count += 1
    
    # Summary
    print()
    print("=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)
    if dry_run:
        print(f"DRY RUN: Would migrate {moved_count} files")
        print()
        print("To actually migrate, run with dry_run=False:")
        print(f"  migrate_volume_files(")
        print(f"    catalog='{catalog}',")
        print(f"    schema='{schema}',")
        print(f"    volume='{volume}',")
        print(f"    scenario_name='{scenario_name}',")
        print(f"    dry_run=False  # ⭐ Change this to False")
        print(f"  )")
    else:
        print(f"✅ Successfully moved: {moved_count} files")
        if failed_count > 0:
            print(f"❌ Failed to move: {failed_count} files")
        print()
        print(f"Files are now in: {scenario_path}")
        print()
        print("To test with the notebook, set in Cell 4:")
        print(f'  TEST_SCENARIO = "{scenario_name}"')
    print("=" * 80)


def load_config_and_migrate(dry_run: bool = True):
    """
    Load configuration from pipeline JSON and run migration.
    """
    # Try to find pipeline config
    possible_paths = [
        "../.env/cockroachdb_pipelines.json",
        "../../.env/cockroachdb_pipelines.json",
        "../../../sources/cockroachdb/.env/cockroachdb_pipelines.json"
    ]
    
    config = None
    for path in possible_paths:
        if os.path.exists(path):
            with open(path, 'r') as f:
                config = json.load(f)
            break
    
    if not config:
        print("⚠️  Could not find cockroachdb_pipelines.json")
        print("   Please run migrate_volume_files() with explicit parameters")
        return
    
    # Extract configuration
    catalog = config['catalog']
    schema = config['schema']
    volume = config['volume_name']
    
    # Run migration
    migrate_volume_files(
        catalog=catalog,
        schema=schema,
        volume=volume,
        scenario_name="existing-parquet-files",
        dry_run=dry_run
    )


# Example usage
if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                     Volume Migration Script                               ║
║                                                                           ║
║  This script migrates parquet files from:                                ║
║    dbfs:/Volumes/catalog/schema/volume/*.parquet                         ║
║                                                                           ║
║  To:                                                                      ║
║    dbfs:/Volumes/catalog/schema/volume/existing-parquet-files/*.parquet  ║
║                                                                           ║
║  Run in Databricks notebook with:                                        ║
║    %run ./migrate_volume_to_scenarios.py                                 ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝
    """)
    
    # Load config and run in dry-run mode by default
    load_config_and_migrate(dry_run=True)
    
    print()
    print("💡 To actually migrate files, run:")
    print("   load_config_and_migrate(dry_run=False)")


