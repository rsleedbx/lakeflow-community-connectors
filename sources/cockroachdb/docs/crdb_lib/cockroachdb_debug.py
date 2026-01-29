"""
CockroachDB CDC Debugging Utilities

This module provides debugging functions for investigating CDC pipeline issues,
particularly for multi-column family scenarios.

Usage in notebook:
    from cockroachdb_debug import *
    
    # Diagnose column family issues
    diagnose_column_family_sync(conn, source_table, target_df, primary_keys)
"""

import psycopg2
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import List, Dict, Any, Optional


def get_column_families(conn, table_name: str) -> Dict[str, List[str]]:
    """
    Get column family assignments for a CockroachDB table.
    
    Args:
        conn: psycopg2 connection
        table_name: Table name (e.g., 'usertable_update_delete_multi_cf')
    
    Returns:
        Dict mapping column family names to lists of columns
    """
    schema, table = 'public', table_name
    if '.' in table_name:
        parts = table_name.split('.')
        if len(parts) == 2:
            schema, table = parts
        elif len(parts) == 3:
            schema, table = parts[1], parts[2]
    
    query = f"""
        SELECT 
            column_name,
            COALESCE(
                (SELECT family_name 
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.constraint_column_usage ccu
                   ON tc.constraint_name = ccu.constraint_name
                 WHERE tc.table_schema = '{schema}'
                   AND tc.table_name = '{table}'
                   AND ccu.column_name = c.column_name
                   AND tc.constraint_type = 'PRIMARY KEY'
                 LIMIT 1),
                'default'
            ) as family_name
        FROM information_schema.columns c
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    # Group by family
    families = {}
    for col_name, family_name in rows:
        if family_name not in families:
            families[family_name] = []
        families[family_name].append(col_name)
    
    return families


def compare_row_by_row(
    conn,
    source_table: str,
    target_df: DataFrame,
    primary_keys: List[str],
    columns_to_check: List[str],
    limit: int = 10
) -> None:
    """
    Compare specific rows between source and target to identify discrepancies.
    
    Args:
        conn: CockroachDB connection
        source_table: Source table name
        target_df: Target Spark DataFrame
        primary_keys: List of primary key columns
        columns_to_check: Columns to compare (e.g., ['field3', 'field4', ...])
        limit: Number of rows to compare
    """
    print("=" * 80)
    print("ROW-BY-ROW COMPARISON")
    print("=" * 80)
    
    # Get sample keys from target
    sample_keys = target_df.select(*primary_keys).limit(limit).collect()
    
    for row in sample_keys:
        key_values = {pk: row[pk] for pk in primary_keys}
        key_str = ", ".join(f"{k}={v}" for k, v in key_values.items())
        
        # Get source row
        where_clause = " AND ".join(f"{k} = {v}" for k, v in key_values.items())
        source_query = f"SELECT {', '.join(columns_to_check)} FROM {source_table} WHERE {where_clause}"
        
        with conn.cursor() as cur:
            cur.execute(source_query)
            source_row = cur.fetchone()
        
        # Get target row
        target_filter = " AND ".join(f"{k} == {v}" for k, v in key_values.items())
        target_row = target_df.filter(target_filter).select(*columns_to_check).first()
        
        if not source_row or not target_row:
            print(f"\n⚠️  Key ({key_str}): Row missing in {'source' if not source_row else 'target'}")
            continue
        
        # Compare columns
        mismatches = []
        for i, col in enumerate(columns_to_check):
            source_val = source_row[i]
            target_val = target_row[col]
            
            # Strip non-numeric for comparison
            if source_val and target_val:
                import re
                source_num = int(re.sub(r'[^0-9]', '', str(source_val)) or '0')
                target_num = int(re.sub(r'[^0-9]', '', str(target_val)) or '0')
                
                if source_num != target_num:
                    mismatches.append(f"{col}: {source_num} vs {target_num}")
        
        if mismatches:
            print(f"\n❌ Key ({key_str}):")
            for m in mismatches:
                print(f"   {m}")
        else:
            print(f"\n✅ Key ({key_str}): All columns match")


def analyze_cdc_events_by_column_family(
    spark,
    azure_path: str,
    table_name: str,
    primary_keys: List[str]
) -> None:
    """
    Analyze CDC events from Azure to check column family fragment distribution.
    
    Args:
        spark: Spark session
        azure_path: Path to Azure changefeed files
        table_name: Table name to filter
        primary_keys: List of primary key columns
    """
    print("=" * 80)
    print("CDC EVENT ANALYSIS (Column Family Distribution)")
    print("=" * 80)
    
    try:
        # Read raw Parquet files
        df_raw = spark.read.format("parquet").load(azure_path)
        
        # Count total events
        total_events = df_raw.count()
        print(f"\n📊 Total CDC events: {total_events:,}")
        
        # Analyze by operation
        print("\n📊 Events by Operation:")
        df_raw.groupBy("__crdb__event_type").count().orderBy("__crdb__event_type").show()
        
        # Check for null columns (indicator of column family fragments)
        print("\n📊 Column Completeness (NULL count per column):")
        
        # Get all columns except metadata
        data_cols = [c for c in df_raw.columns if not c.startswith('__crdb__')]
        
        null_counts = []
        for col in data_cols:
            null_count = df_raw.filter(F.col(col).isNull()).count()
            null_counts.append((col, null_count, f"{null_count/total_events*100:.1f}%"))
        
        for col, count, pct in sorted(null_counts, key=lambda x: x[1], reverse=True):
            indicator = "⚠️ " if count > 0 else "✅"
            print(f"  {indicator} {col:15s}: {count:6,} NULL ({pct})")
        
        # Analyze UPDATE events specifically
        print("\n📊 UPDATE Events Analysis:")
        df_updates = df_raw.filter(F.col("__crdb__event_type") == "c")
        update_count = df_updates.count()
        print(f"  Total UPDATE events: {update_count:,}")
        
        if update_count > 0:
            print(f"\n  Column completeness in UPDATE events:")
            for col in data_cols:
                null_in_updates = df_updates.filter(F.col(col).isNull()).count()
                pct = null_in_updates / update_count * 100
                indicator = "⚠️ " if null_in_updates > 0 else "✅"
                print(f"    {indicator} {col:15s}: {null_in_updates:6,} NULL ({pct:.1f}%)")
        
        # Check for fragmentation (multiple events with same PK and timestamp)
        print("\n📊 Column Family Fragmentation Check:")
        if primary_keys and primary_keys[0] in df_raw.columns:
            pk_col = primary_keys[0]
            
            # Group by PK and timestamp, count fragments
            df_grouped = df_raw.groupBy(pk_col, "__crdb__updated").count()
            df_multi_fragments = df_grouped.filter(F.col("count") > 1)
            
            fragmented_keys = df_multi_fragments.count()
            if fragmented_keys > 0:
                print(f"  ⚠️  Found {fragmented_keys} keys with multiple fragments")
                print(f"  Sample fragmented keys:")
                df_multi_fragments.orderBy(F.col("count").desc()).show(5)
            else:
                print(f"  ✅ No fragmentation detected (all keys have single events)")
        
    except Exception as e:
        print(f"❌ Error analyzing CDC events: {e}")


def check_merge_completeness(
    spark,
    staging_table: str,
    primary_keys: List[str]
) -> None:
    """
    Check if column family fragments were properly merged in staging table.
    
    Args:
        spark: Spark session
        staging_table: Fully qualified staging table name
        primary_keys: List of primary key columns
    """
    print("=" * 80)
    print("MERGE COMPLETENESS CHECK (Staging Table)")
    print("=" * 80)
    
    try:
        df = spark.read.table(staging_table)
        total_rows = df.count()
        
        print(f"\n📊 Total rows in staging: {total_rows:,}")
        
        # Check for NULL values in each column
        print(f"\n📊 NULL values by column:")
        
        all_cols = df.columns
        data_cols = [c for c in all_cols if not c.startswith('_cdc_')]
        
        for col in sorted(data_cols):
            null_count = df.filter(F.col(col).isNull()).count()
            pct = null_count / total_rows * 100
            indicator = "⚠️ " if null_count > 0 else "✅"
            print(f"  {indicator} {col:15s}: {null_count:6,} NULL ({pct:.1f}%)")
        
        # Check for duplicate primary keys (merge incomplete)
        if primary_keys:
            print(f"\n📊 Duplicate primary key check:")
            df_dup_check = df.groupBy(*primary_keys).count()
            df_dups = df_dup_check.filter(F.col("count") > 1)
            
            dup_count = df_dups.count()
            if dup_count > 0:
                print(f"  ⚠️  Found {dup_count} duplicate primary keys!")
                print(f"  Sample duplicates:")
                df_dups.orderBy(F.col("count").desc()).show(5)
            else:
                print(f"  ✅ No duplicate primary keys (merge successful)")
        
    except Exception as e:
        print(f"❌ Error checking merge completeness: {e}")


def diagnose_column_family_sync(
    conn,
    source_table: str,
    target_df: DataFrame,
    primary_keys: List[str],
    mismatched_columns: List[str] = None
) -> None:
    """
    Comprehensive diagnosis of column family sync issues.
    
    Args:
        conn: CockroachDB connection
        source_table: Source table name
        target_df: Target Spark DataFrame
        primary_keys: List of primary key columns
        mismatched_columns: Columns with sum mismatches (e.g., ['field3', 'field4', ...])
    """
    print("\n" + "=" * 80)
    print("COLUMN FAMILY SYNC DIAGNOSIS")
    print("=" * 80)
    
    # 1. Get column family assignments
    print("\n1️⃣  Column Family Assignments:")
    print("-" * 80)
    try:
        families = get_column_families(conn, source_table)
        for family_name, columns in families.items():
            print(f"\n  📁 Family '{family_name}':")
            for col in columns:
                indicator = "❌" if mismatched_columns and col in mismatched_columns else "✅"
                print(f"     {indicator} {col}")
    except Exception as e:
        print(f"  ⚠️  Could not retrieve column families: {e}")
    
    # 2. Compare sample rows
    if mismatched_columns:
        print("\n2️⃣  Sample Row Comparison:")
        print("-" * 80)
        compare_row_by_row(
            conn,
            source_table,
            target_df,
            primary_keys,
            mismatched_columns[:3],  # Check first 3 mismatched columns
            limit=5
        )
    
    # 3. Check for pattern in mismatches
    print("\n3️⃣  Mismatch Pattern Analysis:")
    print("-" * 80)
    
    if mismatched_columns:
        # Get sum differences
        print(f"\n  Analyzing {len(mismatched_columns)} mismatched columns...")
        print(f"  Columns: {', '.join(mismatched_columns)}")
        
        # Hypothesis: Missing UPDATE events
        source_count = target_df.count()
        print(f"\n  💡 Hypothesis: Missing UPDATE events for column families")
        print(f"     If some UPDATE events didn't merge properly, those columns")
        print(f"     would retain old values instead of updated values.")


def inspect_raw_cdc_files(
    spark,
    azure_path: str,
    primary_key: str,
    key_value: Any,
    show_content: bool = True
) -> None:
    """
    Inspect raw CDC files for a specific key to see all fragments.
    
    Args:
        spark: Spark session
        azure_path: Path to Azure changefeed files
        primary_key: Primary key column name
        key_value: Specific key value to inspect
        show_content: Whether to show full column values
    """
    print("=" * 80)
    print(f"RAW CDC FILE INSPECTION (Key: {primary_key}={key_value})")
    print("=" * 80)
    
    try:
        df_raw = spark.read.format("parquet").load(azure_path)
        df_key = df_raw.filter(F.col(primary_key) == key_value)
        
        count = df_key.count()
        print(f"\n📊 Found {count} CDC events for this key")
        
        if count > 0:
            # Show events ordered by timestamp
            df_sorted = df_key.orderBy("__crdb__updated")
            
            print(f"\n📄 Events (ordered by timestamp):")
            if show_content:
                df_sorted.show(truncate=False, vertical=True)
            else:
                # Show summary
                summary_cols = [primary_key, "__crdb__event_type", "__crdb__updated"] + \
                               [c for c in df_key.columns if c.startswith('field')]
                df_sorted.select(*summary_cols).show(truncate=40)
            
            # Analyze fragments
            print(f"\n📊 Fragment Analysis:")
            fragments = df_sorted.collect()
            
            for i, row in enumerate(fragments, 1):
                event_type = row["__crdb__event_type"]
                timestamp = row["__crdb__updated"]
                
                # Count non-null data columns
                data_cols = [c for c in row.asDict().keys() 
                            if not c.startswith('__crdb__') and c != primary_key]
                non_null = sum(1 for c in data_cols if row[c] is not None)
                
                print(f"  Fragment {i}: {event_type} @ {timestamp} - {non_null}/{len(data_cols)} columns populated")
                
    except Exception as e:
        print(f"❌ Error inspecting raw CDC files: {e}")


# Convenience function to run all diagnostics
def run_full_diagnosis(
    conn,
    spark,
    source_table: str,
    target_df: DataFrame,
    staging_table: str,
    azure_path: str,
    primary_keys: List[str],
    mismatched_columns: List[str]
) -> None:
    """
    Run comprehensive diagnosis of CDC sync issues.
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Source table name
        target_df: Target Spark DataFrame
        staging_table: Staging table name
        azure_path: Azure changefeed path
        primary_keys: List of primary key columns
        mismatched_columns: Columns with sum mismatches
    """
    print("\n" + "█" * 80)
    print("🔍 FULL CDC SYNC DIAGNOSIS")
    print("█" * 80)
    
    # 1. Column family diagnosis
    diagnose_column_family_sync(conn, source_table, target_df, primary_keys, mismatched_columns)
    
    # 2. Check merge completeness
    if staging_table:
        check_merge_completeness(spark, staging_table, primary_keys)
    
    # 3. Analyze raw CDC events
    if azure_path:
        analyze_cdc_events_by_column_family(spark, azure_path, source_table, primary_keys)
    
    print("\n" + "█" * 80)
    print("✅ DIAGNOSIS COMPLETE")
    print("█" * 80)
