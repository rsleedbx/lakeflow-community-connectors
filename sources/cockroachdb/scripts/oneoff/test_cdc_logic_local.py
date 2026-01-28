#!/usr/bin/env python3
"""
Test CDC Logic Locally Using Cached Data

This script allows rapid iteration on CDC transformation logic without
needing to run full Databricks notebook tests.

Usage:
    python test_cdc_logic_local.py [path_to_cached_json_files]
    
    # Auto-find cached files
    python test_cdc_logic_local.py
    
    # Specific scenario
    python test_cdc_logic_local.py .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def find_cached_files(base_dir=None):
    """Find cached JSON CDC files."""
    if base_dir:
        cache_dir = Path(base_dir)
    else:
        # Find git root
        git_root = Path(__file__).resolve().parents[3]
        cache_dir = git_root / ".cache" / "cdc_test_data"
    
    if not cache_dir.exists():
        print(f"❌ Cache directory not found: {cache_dir}")
        print("   Run sync_azure_to_volume_compact.py first to populate cache")
        sys.exit(1)
    
    # Find JSON files
    json_files = list(cache_dir.rglob("*.ndjson"))
    
    if not json_files:
        print(f"❌ No .ndjson files found in: {cache_dir}")
        sys.exit(1)
    
    return json_files

def detect_snapshot_cutoff(spark, files):
    """Detect snapshot cutoff from first file (sequence 00000000)."""
    # Find snapshot files (sequence 00000000)
    snapshot_files = [f for f in files if '-00000000-' in f.name]
    
    if not snapshot_files:
        # Fallback: use first file
        snapshot_files = sorted(files)[:1]
    
    max_timestamp = None
    for file_path in snapshot_files:
        try:
            df_sample = spark.read.json(str(file_path))
            if 'updated' in df_sample.columns:
                file_max = df_sample.agg({"updated": "max"}).collect()[0][0]
                if file_max:
                    file_max_str = str(file_max)
                    if max_timestamp is None or file_max_str > max_timestamp:
                        max_timestamp = file_max_str
        except Exception as e:
            print(f"   ⚠️  Could not read {file_path.name}: {e}")
            continue
    
    return max_timestamp

def apply_cdc_logic(df, snapshot_cutoff):
    """Apply CDC transformation logic (copied from cockroachdb.py)."""
    
    print("\n" + "=" * 80)
    print("APPLYING CDC LOGIC")
    print("=" * 80)
    
    # Detect format
    schema_columns = df.columns
    is_json_format = 'before' in schema_columns and 'after' in schema_columns
    
    if not is_json_format:
        print("❌ Not JSON format (missing 'before'/'after' columns)")
        return df
    
    print(f"✅ Detected JSON format")
    print(f"📍 Snapshot cutoff: {snapshot_cutoff}")
    print()
    
    # Flatten 'after' struct
    if 'after' in schema_columns:
        after_fields = df.schema['after'].dataType.fieldNames() if hasattr(df.schema['after'].dataType, 'fieldNames') else []
        print(f"📦 Flattening {len(after_fields)} fields from 'after' struct")
        for field in after_fields:
            df = df.withColumn(field, F.col(f"after.{field}"))
    
    # Apply CDC operation logic
    if snapshot_cutoff:
        try:
            snapshot_cutoff_double = float(snapshot_cutoff)
        except (ValueError, TypeError):
            snapshot_cutoff_double = None
        
        if snapshot_cutoff_double is not None:
            print(f"🔢 Snapshot cutoff (double): {snapshot_cutoff_double}")
            print()
            
            # Add helper columns
            df = df.withColumn("_after_json", F.to_json(F.col("after")))
            df = df.withColumn("_before_json", F.to_json(F.col("before")))
            
            # DEBUG: Sample the JSON strings
            print("🐛 DEBUG: Sample _after_json/_before_json values:")
            sample = df.select("_after_json", "_before_json", "updated").limit(5).collect()
            for i, row in enumerate(sample):
                after_json = row['_after_json']
                before_json = row['_before_json']
                updated = row['updated']
                print(f"   Row {i}:")
                print(f"      after_json:  {repr(after_json[:80] if after_json else None)}...")
                print(f"      before_json: {repr(before_json[:80] if before_json else None)}...")
                print(f"      updated:     {updated}")
            print()
            
            # Check if after/before are "empty"
            after_empty = (F.col("_after_json") == F.lit("null")) | (F.col("_after_json") == F.lit("{}"))
            after_not_empty = ~after_empty
            before_empty = (F.col("_before_json") == F.lit("null")) | (F.col("_before_json") == F.lit("{}"))
            before_not_empty = ~before_empty
            
            # Apply CDC operation classification
            df = df.withColumn("_cdc_operation",
                F.when(
                    after_not_empty & before_empty & 
                    (F.col("updated").cast("double") <= F.lit(snapshot_cutoff_double)),
                    F.lit("SNAPSHOT")
                )
                .when(
                    after_not_empty & before_empty & 
                    (F.col("updated").cast("double") > F.lit(snapshot_cutoff_double)),
                    F.lit("INSERT")
                )
                .when(after_not_empty & before_not_empty, F.lit("UPDATE"))
                .when(after_empty & before_not_empty, F.lit("DELETE"))
                .otherwise(F.lit("UNKNOWN"))
            )
            
            # Show operation counts
            print("📊 CDC Operation Counts (after classification):")
            op_counts = df.groupBy("_cdc_operation").count().orderBy("_cdc_operation").collect()
            total_rows = sum(row['count'] for row in op_counts)
            for row in op_counts:
                op = row['_cdc_operation']
                count = row['count']
                pct = (count / total_rows * 100) if total_rows > 0 else 0
                print(f"   {op:12s}: {count:5d} ({pct:5.1f}%)")
            print(f"   {'TOTAL':12s}: {total_rows:5d}")
            print()
            
            # Show sample UNKNOWN rows if any
            unknown_count = sum(row['count'] for row in op_counts if row['_cdc_operation'] == 'UNKNOWN')
            if unknown_count > 0:
                print("🔍 Sample UNKNOWN rows (investigating issue):")
                unknown_sample = df.filter(F.col("_cdc_operation") == "UNKNOWN").select(
                    "_after_json", "_before_json", "updated", "_cdc_operation"
                ).limit(3).collect()
                for i, row in enumerate(unknown_sample):
                    print(f"   Row {i}:")
                    print(f"      _after_json:  {repr(row['_after_json'][:100] if row['_after_json'] else None)}...")
                    print(f"      _before_json: {repr(row['_before_json'][:100] if row['_before_json'] else None)}...")
                    print(f"      updated:      {row['updated']}")
                print()
            
            # Clean up helper columns
            df = df.drop("_after_json", "_before_json")
    
    return df

def main():
    # Parse arguments
    cache_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    print("=" * 80)
    print("LOCAL CDC LOGIC TEST")
    print("=" * 80)
    print()
    
    # Find cached files
    print("📂 Finding cached JSON files...")
    files = find_cached_files(cache_path)
    print(f"   ✅ Found {len(files)} file(s)")
    for f in files[:5]:  # Show first 5
        print(f"      - {f.name}")
    if len(files) > 5:
        print(f"      ... and {len(files) - 5} more")
    print()
    
    # Create Spark session
    print("⚡ Starting local Spark session...")
    spark = SparkSession.builder \
        .appName("CDC Logic Test") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("   ✅ Spark started")
    print()
    
    # Load JSON files
    print("📥 Loading JSON files...")
    file_paths = [str(f) for f in files]
    df_raw = spark.read.json(file_paths)
    
    row_count = df_raw.count()
    print(f"   ✅ Loaded {row_count:,} rows")
    print(f"   📋 Columns: {', '.join(df_raw.columns[:10])}{'...' if len(df_raw.columns) > 10 else ''}")
    print()
    
    # Detect snapshot cutoff
    print("🔍 Detecting snapshot cutoff...")
    snapshot_cutoff = detect_snapshot_cutoff(spark, files)
    if snapshot_cutoff:
        print(f"   ✅ Detected: {snapshot_cutoff}")
    else:
        print(f"   ⚠️  No snapshot cutoff detected")
    print()
    
    # Apply CDC logic
    df_enriched = apply_cdc_logic(df_raw, snapshot_cutoff)
    
    # Show results
    print("=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)
    print()
    
    if '_cdc_operation' in df_enriched.columns:
        print("✅ CDC operation column added successfully")
        
        # Final counts
        print("\n📊 Final Operation Counts:")
        final_counts = df_enriched.groupBy("_cdc_operation").count().orderBy("_cdc_operation").collect()
        total = sum(row['count'] for row in final_counts)
        for row in final_counts:
            op = row['_cdc_operation']
            count = row['count']
            pct = (count / total * 100) if total > 0 else 0
            
            # Color code results
            if op == 'UNKNOWN' and count > 100:
                status = "❌"
            elif op in ['SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE']:
                status = "✅"
            else:
                status = "⚠️ "
            
            print(f"   {status} {op:12s}: {count:5d} ({pct:5.1f}%)")
        
        print(f"\n   Total rows: {total:,}")
    else:
        print("❌ CDC operation column not added")
    
    spark.stop()

if __name__ == "__main__":
    main()

