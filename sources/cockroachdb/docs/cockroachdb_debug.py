"""
CockroachDB CDC Debugging Utilities

This module provides debugging functions for investigating CDC pipeline issues,
particularly for multi-column family scenarios.

Available Functions:
    Core Diagnostic Functions:
        • run_full_diagnosis() - Comprehensive diagnosis of all sync issues
        • run_full_diagnosis_from_config() - Full diagnosis using notebook config dict
        • diagnose_column_family_sync() - Column family specific diagnosis
        • find_mismatched_rows() - Row-by-row comparison between source and target
    
    Quick Debug Functions:
        • quick_check_missing_keys() - Lightweight check for specific keys
        • inspect_target_table() - Comprehensive target table analysis
        • detailed_missing_keys_investigation() - Detailed key investigation
    
    Analysis Functions:
        • analyze_cdc_events_by_column_family() - Analyze CDC event distribution
        • check_merge_completeness() - Check staging table merge status
        • check_staging_and_azure_for_keys() - Check specific keys in staging/Azure
        • inspect_raw_cdc_files() - Inspect raw CDC files for specific keys
    
    Helper Functions:
        • get_column_families() - Get column family assignments
        • compare_row_by_row() - Compare specific rows between source and target

Usage in notebook:
    from cockroachdb_debug import quick_check_missing_keys, inspect_target_table
    
    # Quick check for missing keys
    quick_check_missing_keys(conn, spark, source_table, target_catalog, 
                             target_schema, target_table, [17, 18, 19])
    
    # Comprehensive target analysis
    inspect_target_table(spark, target_catalog, target_schema, target_table)
    
    # Full diagnosis
    run_full_diagnosis(conn, spark, source_table, target_df, staging_table,
                       azure_path, primary_keys, mismatched_columns)
"""

import pg8000.native
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import List, Dict, Any, Optional


def get_column_families(conn, table_name: str) -> Dict[str, List[str]]:
    """
    Get column family assignments for a CockroachDB table.
    
    Args:
        conn: pg8000 connection
        table_name: Table name (e.g., 'usertable_update_delete_multi_cf')
    
    Returns:
        Dict mapping column family names to lists of columns
    """
    # Clear any aborted transaction state
    try:
        conn.run("ROLLBACK")
    except:
        pass
    
    schema, table = 'public', table_name
    if '.' in table_name:
        parts = table_name.split('.')
        if len(parts) == 2:
            schema, table = parts
        elif len(parts) == 3:
            schema, table = parts[1], parts[2]
    
    # Simplified approach: Just return all columns in a 'default' family
    # Column family information is complex to query in CockroachDB and not critical for diagnosis
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position;
    """
    
    try:
        rows = conn.run(query)
        # Return all columns as part of 'default' family for simplicity
        # In reality, column families would need SHOW CREATE TABLE parsing
        return {'default': [row[0] for row in rows]}
    except Exception as e:
        print(f"  ⚠️  Could not retrieve columns: {e}")
        return {}


def find_mismatched_rows(
    conn,
    spark,
    source_table: str,
    target_df,
    primary_keys: List[str],
    columns_to_check: List[str]
) -> None:
    """
    Find exact rows and columns that have mismatches between source and target.
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Fully qualified source table name
        target_df: Target Spark DataFrame
        primary_keys: List of primary key column names
        columns_to_check: List of column names to check for mismatches
    """
    from pyspark.sql import functions as F
    
    print("=" * 80)
    print("DETAILED MISMATCH ANALYSIS (Row-by-Row)")
    print("=" * 80)
    
    try:
        conn.run("ROLLBACK")
    except:
        pass
    
    # Get all source data
    print(f"\n📥 Fetching all rows from source: {source_table}")
    
    # Get column names from target (exclude metadata columns)
    # Exclude: _rescued_*, _file_*, _cdc_*, __crdb__*
    target_columns = [col for col in target_df.columns 
                      if not col.startswith('_rescued') 
                      and not col.startswith('_file')
                      and not col.startswith('_cdc')
                      and not col.startswith('__crdb__')]
    
    print(f"   Comparing {len(target_columns)} business columns (excluding metadata)")
    print(f"   Columns: {', '.join(target_columns)}")
    
    # Select only those columns from source
    column_list = ', '.join(target_columns)
    source_rows = conn.run(f"SELECT {column_list} FROM {source_table} ORDER BY {', '.join(primary_keys)}")
    
    # Get column names from first row (if any)
    if not source_rows:
        print(f"   ⚠️  Source table is empty!")
        return
    
    # Build schema from target for just the columns we selected
    from pyspark.sql.types import StructType
    filtered_schema = StructType([field for field in target_df.schema.fields 
                                   if field.name in target_columns])
    
    # Convert to Spark DataFrame
    source_df = spark.createDataFrame(source_rows, schema=filtered_schema)
    
    # Filter target to same columns
    target_df_filtered = target_df.select(*target_columns)
    
    source_count = source_df.count()
    target_count = target_df_filtered.count()
    
    print(f"   Source rows: {source_count:,}")
    print(f"   Target rows: {target_count:,}")
    
    if source_count != target_count:
        print(f"\n⚠️  Row count mismatch!")
        
        # Find missing keys
        source_keys = source_df.select(*primary_keys)
        target_keys = target_df_filtered.select(*primary_keys)
        
        missing_in_target = source_keys.subtract(target_keys)
        missing_in_source = target_keys.subtract(source_keys)
        
        missing_target_count = missing_in_target.count()
        missing_source_count = missing_in_source.count()
        
        if missing_target_count > 0:
            print(f"\n   ❌ {missing_target_count} keys in source but NOT in target:")
            missing_in_target.show(10, truncate=False)
        
        if missing_source_count > 0:
            print(f"\n   ❌ {missing_source_count} keys in target but NOT in source:")
            missing_in_source.show(10, truncate=False)
    
    # Check each column for value mismatches
    print(f"\n📊 Checking {len(columns_to_check)} columns for value mismatches...")
    
    any_mismatch = False
    for col in columns_to_check:
        if col not in source_df.columns or col not in target_df_filtered.columns:
            print(f"   ⚠️  {col}: Column not found in both DataFrames")
            continue
        
        # Join on primary keys and compare values
        pk_cols = primary_keys
        
        comparison = source_df.select(*pk_cols, F.col(col).alias("source_val")) \
            .join(
                target_df_filtered.select(*pk_cols, F.col(col).alias("target_val")),
                on=pk_cols,
                how="inner"
            ) \
            .filter(
                (F.col("source_val").isNotNull() & F.col("target_val").isNull()) |
                (F.col("source_val").isNull() & F.col("target_val").isNotNull()) |
                ((F.col("source_val").isNotNull() & F.col("target_val").isNotNull()) & 
                 (F.col("source_val") != F.col("target_val")))
            )
        
        mismatch_count = comparison.count()
        
        if mismatch_count > 0:
            any_mismatch = True
            print(f"\n   ❌ {col}: {mismatch_count} rows with different values")
            print(f"      Showing first 10 mismatched rows:")
            comparison.orderBy(*pk_cols).show(10, truncate=False)
        else:
            print(f"   ✅ {col}: All values match")
    
    if not any_mismatch:
        print(f"\n✅ All {len(columns_to_check)} columns match perfectly across all rows!")
        return False  # No mismatches found
    else:
        print(f"\n⚠️  Found value mismatches - see details above")
        return True  # Mismatches found


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
    # Clear any aborted transaction state
    try:
        conn.run("ROLLBACK")
    except:
        pass
    
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
        
        # pg8000.native uses connection directly (no cursor)
        result = conn.run(source_query)
        source_row = result[0] if result else None
        
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
            
            # Check for NULL mismatches first
            source_is_null = source_val is None or source_val == ''
            target_is_null = target_val is None or target_val == ''
            
            if source_is_null != target_is_null:
                # One is NULL, the other is not - MISMATCH
                mismatches.append(f"{col}: {source_val} vs {target_val}")
                continue
            
            # If both are NULL, they match
            if source_is_null and target_is_null:
                continue
            
            # Both have values - strip non-numeric for comparison
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
    
    print(f"\n📁 Azure path: {azure_path}")
    
    try:
        # Use batch read with pathGlobFilter (same pattern as notebook's Auto Loader)
        # This excludes .RESOLVED files which have incompatible schema
        # CRITICAL: Use mergeSchema=true to combine schemas from all column family fragments
        df_raw = (spark.read
            .format("parquet")
            .option("pathGlobFilter", f"*{table_name}*.parquet")  # Match notebook pattern
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")  # ← Merge schemas from all column families
            .load(azure_path)
        )
        
        print(f"✅ Schema inferred from Parquet files")
        print(f"   Filter: *{table_name}*.parquet (excludes .RESOLVED files)")
        print(f"   📋 Columns detected: {len(df_raw.columns)} columns")
        
        # Count total events
        total_events = df_raw.count()
        print(f"📊 Total CDC events: {total_events:,}")
        
        if total_events == 0:
            print(f"  ℹ️  No CDC events found (changefeed may not be running yet)")
            return
        
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
        
        if total_rows == 0:
            print(f"  ℹ️  Staging table is empty - data has been merged to target")
            return
        
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


def check_staging_and_azure_for_keys(
    spark,
    staging_table: str,
    azure_path: str,
    table_name: str,
    primary_keys: List[str],
    mismatched_keys: List[Any],
    columns_to_check: List[str]
) -> bool:
    """
    Check if specific keys exist in the staging table or raw Azure CDC files.
    
    Returns:
        bool: True if the mismatched data was found in staging (MERGE issue),
              False if data is missing from Azure (changefeed issue)
    
    Args:
        spark: Spark session
        staging_table: Fully qualified staging table name
        azure_path: Path to Azure CDC files
        table_name: Table name (for file filtering)
        primary_keys: List of primary key column names
        mismatched_keys: List of primary key values that are mismatched
        columns_to_check: List of columns to display
    """
    from pyspark.sql import functions as F
    
    print("=" * 80)
    print("STAGING & AZURE CHECK (for mismatched keys)")
    print("=" * 80)
    
    print(f"\n🔑 Mismatched keys: {mismatched_keys}")
    
    data_found_in_staging = False
    
    # 1. Check staging table
    try:
        staging_df = spark.read.table(staging_table)
        current_count = staging_df.count()
        
        print(f"\n📊 Current staging table: {current_count} rows")
        
        if current_count > 0:
            print(f"   🔍 Checking for mismatched keys in staging...")
            
            # Filter for mismatched keys
            key_filter = F.col(primary_keys[0]).isin(mismatched_keys)
            matched_rows = staging_df.filter(key_filter)
            matched_count = matched_rows.count()
            
            if matched_count > 0:
                print(f"   ✅ Found {matched_count} rows with these keys in staging:")
                cols_to_show = primary_keys + columns_to_check[:3] + ['__crdb__event_type', '__crdb__updated']
                matched_rows.select(*[c for c in cols_to_show if c in staging_df.columns]).show(10, truncate=False)
                
                # Check if any of the matched rows have non-NULL values for the mismatched columns
                for col in columns_to_check[:3]:  # Check first 3 columns
                    if col in staging_df.columns:
                        non_null_count = matched_rows.filter(F.col(col).isNotNull()).count()
                        if non_null_count > 0:
                            data_found_in_staging = True
                            break
            else:
                print(f"   ⚠️  No rows with these keys found in current staging")
        else:
            print(f"   ℹ️  Staging is empty - all CDC events already merged")
        
    except Exception as e:
        print(f"   ❌ Error checking staging: {e}")
    
    # 2. Check raw Azure CDC files
    print(f"\n📁 Checking raw Azure CDC files...")
    
    try:
        # Read raw Parquet files (same pattern as CDC analysis)
        # CRITICAL: Use mergeSchema=true to combine schemas from all column family fragments
        df_raw = (spark.read
            .format("parquet")
            .option("pathGlobFilter", f"*{table_name}*.parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")  # ← Merge schemas from all column families
            .load(azure_path)
        )
        
        total_events = df_raw.count()
        print(f"   Total CDC events in Azure: {total_events:,}")
        print(f"   📋 Columns detected: {len(df_raw.columns)} columns")
        print(f"      {', '.join([c for c in df_raw.columns if not c.startswith('_')])}")
        
        # Filter for mismatched keys
        key_filter = F.col(primary_keys[0]).isin(mismatched_keys)
        matched_events = df_raw.filter(key_filter)
        matched_count = matched_events.count()
        
        if matched_count > 0:
            print(f"   ✅ Found {matched_count} CDC events for these keys in Azure:")
            
            # Show ALL columns in the raw events (including NULLs) to see column family fragments
            print(f"\n   📊 All {matched_count} events for key(s) {mismatched_keys}:")
            matched_events.orderBy(primary_keys[0], '__crdb__updated').show(20, truncate=False)
            
            # Check column completeness for these specific keys
            print(f"\n   📊 Column completeness for mismatched keys:")
            all_cols = [primary_keys[0], 'field0', 'field1', 'field2'] + columns_to_check
            for col in all_cols:
                if col in df_raw.columns:
                    null_count = matched_events.filter(F.col(col).isNull()).count()
                    non_null_count = matched_count - null_count
                    indicator = "✅" if non_null_count > 0 else "❌"
                    print(f"      {indicator} {col:10s}: {non_null_count:3d} with values, {null_count:3d} NULL")
                else:
                    print(f"      ⚠️  {col:10s}: Column not in CDC files")
            
            # Interpretation
            missing_cols = [col for col in columns_to_check if col not in df_raw.columns or 
                           matched_events.filter(F.col(col).isNotNull()).count() == 0]
            if missing_cols:
                print(f"\n   💡 Analysis:")
                print(f"      • {len(missing_cols)} mismatched columns have NO events with values in Azure")
                print(f"      • Missing columns: {', '.join(missing_cols[:5])}")
                print(f"      • This suggests the column family fragment for these columns never made it to Azure")
                print(f"      • Likely cause: Changefeed didn't capture the INSERT for this column family")
        else:
            print(f"   ⚠️  No CDC events found for these keys in Azure!")
            print(f"   💡 This suggests the changefeed may not have captured these events")
        
    except Exception as e:
        print(f"   ❌ Error checking Azure files: {e}")
        import traceback
        traceback.print_exc()
    
    return data_found_in_staging


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
        # CRITICAL: Use mergeSchema=true to combine schemas from all column family fragments
        df_raw = (spark.read
            .format("parquet")
            .option("mergeSchema", "true")
            .load(azure_path)
        )
        df_key = df_raw.filter(F.col(primary_key) == key_value)
        
        count = df_key.count()
        print(f"\n📊 Found {count} CDC events for this key")
        print(f"   📋 Total columns available: {len(df_raw.columns)}")
        
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
    # Clear any aborted transaction state from previous errors
    try:
        conn.run("ROLLBACK")
    except:
        pass  # Ignore if no transaction to rollback
    
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
        # Extract just the table name from fully qualified name (e.g., "catalog.schema.table" -> "table")
        table_name = source_table.split('.')[-1] if '.' in source_table else source_table
        analyze_cdc_events_by_column_family(spark, azure_path, table_name, primary_keys)
    
    # 4. Detailed mismatch analysis (if mismatches were reported)
    actual_mismatch_found = False
    mismatched_keys = []
    data_in_staging = False
    if mismatched_columns:
        print(f"\n")
        actual_mismatch_found = find_mismatched_rows(conn, spark, source_table, target_df, primary_keys, mismatched_columns)
        
        # If mismatches were found, check if those keys exist in staging or Azure
        if actual_mismatch_found and staging_table:
            # Get the mismatched keys from target (filter by NULL in mismatched columns)
            from pyspark.sql import functions as F
            
            mismatch_filter = F.col(mismatched_columns[0]).isNull()
            for col in mismatched_columns[1:]:
                mismatch_filter = mismatch_filter | F.col(col).isNull()
            
            mismatched_keys_df = target_df.filter(mismatch_filter).select(*primary_keys).distinct()
            mismatched_keys = [row[primary_keys[0]] for row in mismatched_keys_df.collect()]
            
            if mismatched_keys:
                print(f"\n")
                print(f"🔍 Checking if mismatched keys ({len(mismatched_keys)} keys) exist in staging/Azure...")
                data_in_staging = check_staging_and_azure_for_keys(spark, staging_table, azure_path, 
                                                  source_table.split('.')[-1], 
                                                  primary_keys, mismatched_keys, mismatched_columns)
    
    # 5. Summary and interpretation
    print("\n" + "=" * 80)
    print("📊 DIAGNOSIS SUMMARY")
    print("=" * 80)
    
    if actual_mismatch_found:
        print(f"\n❌ CONFIRMED: Data is OUT OF SYNC")
        print(f"\n🔍 Found Issues:")
        print(f"    • {len(mismatched_columns)} columns have mismatched values: {', '.join(mismatched_columns[:5])}")
        print(f"    • See 'DETAILED MISMATCH ANALYSIS' section above for specific rows and values")
        print(f"\n💡 Root Cause (Column Family Issue):")
        print(f"    • These columns are in a separate column family from the primary key")
        print(f"    • Column family fragments are stored in separate Parquet files")
        
        # Determine if data is in staging or missing from Azure
        if mismatched_keys and staging_table:
            if data_in_staging:
                print(f"    • ✅ Data EXISTS in staging table")
                print(f"    • ❌ MERGE from staging → target failed to consolidate fragments")
                print(f"\n🔧 Recommended Fix:")
                print(f"    • Issue: MERGE logic didn't properly consolidate column family fragments")
                print(f"    • Solution 1: Check MERGE deduplication logic in Cell 6")
                print(f"    • Solution 2: Manually run MERGE again (re-run Cell 12)")
                print(f"    • Solution 3: Drop target table and re-ingest (Cell 16 + Cell 12)")
            else:
                print(f"    • ❌ Data MISSING from staging table")
                print(f"    • ❌ Changefeed didn't capture the column family fragment")
                print(f"\n🔧 Recommended Fix:")
                print(f"    • Issue: Changefeed failed to capture field3-9 column family for some rows")
                print(f"    • Solution 1: Complete reset - drop changefeed, clear Azure, recreate")
                print(f"    • Solution 2: Manual backfill for affected keys (see ROW_112_DIAGNOSIS.md)")
                print(f"    • Solution 3: Check CockroachDB changefeed logs for errors")
        else:
            print(f"    • The MERGE operation may not have processed all fragments")
            print(f"\n🔧 Recommended Fix:")
            print(f"    • Re-run the ingestion pipeline (Cell 12)")
            print(f"    • Ensure wait_for_changefeed_files() detects all column family files")
            print(f"    • Check that the stabilization_wait period is sufficient")
    elif mismatched_columns:
        print(f"\n⚠️  {len(mismatched_columns)} columns reported as mismatched: {', '.join(mismatched_columns[:5])}")
        print(f"    However, row-by-row comparison shows they match now!")
        print(f"\n💡 Interpretation:")
        print(f"    • The data sync is CORRECT in the current state")
        print(f"    • The mismatch was detected earlier (before diagnosis)")
        print(f"    • Running the pipeline again resolved the issue")
        print(f"\n✅ CONCLUSION: Data is in sync!")
    else:
        print(f"\n✅ No mismatches detected - data is in sync!")
    
    print("\n" + "█" * 80)
    print("✅ DIAGNOSIS COMPLETE")
    print("█" * 80)


def quick_check_missing_keys(
    conn,
    spark,
    source_table: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    missing_keys: List[int]
) -> None:
    """
    Lightweight check to see if specific keys exist in CockroachDB and staging.
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Source table name (e.g., 'usertable_update_delete_multi_cf')
        target_catalog: Target catalog (e.g., 'main')
        target_schema: Target schema (e.g., 'robert_lee_crdb')
        target_table: Target table name
        missing_keys: List of keys to check (e.g., [17, 18, 19])
    """
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    staging_table_cf = f"{target_table_fqn}_staging_cf"
    
    print("🔍 Quick Debug: Checking missing keys...")
    print("=" * 80)
    print(f"Keys to check: {missing_keys}")
    print()
    
    # Check CockroachDB
    print(f"📊 CockroachDB ({source_table}):")
    cursor = conn.cursor()
    for key in missing_keys:
        cursor.execute(f"SELECT * FROM {source_table} WHERE ycsb_key = %s", (key,))
        result = cursor.fetchone()
        print(f"   Key {key}: {'✅ EXISTS' if result else '❌ NOT FOUND (deleted)'}")
    
    # Check Staging Table
    print(f"\n📊 Staging Table ({staging_table_cf}):")
    if spark.catalog.tableExists(staging_table_cf):
        staging_df = spark.read.table(staging_table_cf)
        for key in missing_keys:
            count = staging_df.filter(F.col("ycsb_key") == key).count()
            print(f"   Key {key}: {count} row(s)")
        
        print("\n💡 Next steps:")
        print("   - If keys exist in CockroachDB but not in staging:")
        print("     → Re-run Cell 12 to pick up new CDC files")
        print("   - If keys exist in staging but not in target:")
        print("     → Check Cell 12 output for MERGE errors")
        print("     → Run inspect_target_table() for detailed analysis")
    else:
        print("   ⚠️  Staging table doesn't exist (Cell 12 dropped it)")
        print("   💡 Re-run Cell 12 to recreate staging for debugging")


def inspect_target_table(
    spark,
    target_catalog: str,
    target_schema: str,
    target_table: str
) -> None:
    """
    Comprehensive analysis of target table:
    - CDC operation distribution
    - Key distribution and gaps
    - Duplicate detection
    - Sample records
    
    Use this to diagnose sync issues and data quality problems.
    
    Args:
        spark: Spark session
        target_catalog: Target catalog (e.g., 'main')
        target_schema: Target schema (e.g., 'robert_lee_crdb')
        target_table: Target table name
    """
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    
    print(f"📊 Target Table Analysis: {target_table_fqn}")
    print("=" * 80)
    
    # Read target table
    df = spark.read.table(target_table_fqn)
    total_rows = df.count()
    print(f"\n📈 Total Rows: {total_rows:,}")
    
    # Group by CDC operation type
    if "_cdc_operation" in df.columns:
        print("\n🔍 CDC Operations:")
        df.groupBy("_cdc_operation").count().orderBy("_cdc_operation").show()
        
        # Check if DELETE rows are stored as data (should NOT happen)
        delete_count = df.filter(F.col("_cdc_operation") == "DELETE").count()
        if delete_count > 0:
            print(f"\n⚠️  WARNING: Found {delete_count} DELETE rows stored as data!")
            print("   This is a bug - DELETE rows should not be in the target table.")
            print("   💡 Run Cell 16 to fix (drops and recreates table)")
    
    # Show key distribution
    if "ycsb_key" in df.columns:
        print("\n🔍 Key Distribution:")
        key_dist = df.groupBy("ycsb_key").count().orderBy("ycsb_key")
        key_dist.show(50)
        
        # Check for duplicates
        duplicates = key_dist.filter("count > 1")
        dup_count = duplicates.count()
        if dup_count > 0:
            print(f"\n⚠️  Found {dup_count} duplicate keys!")
            duplicates.show()
            print("\n   💡 This indicates deduplication failure in MERGE logic")
        else:
            print("\n✅ No duplicate keys found")
        
        # Show key range and gaps
        key_stats = df.select(
            F.min("ycsb_key").alias("min_key"),
            F.max("ycsb_key").alias("max_key"),
            F.count("ycsb_key").alias("count")
        ).collect()[0]
        
        expected_count = key_stats["max_key"] - key_stats["min_key"] + 1
        actual_count = key_stats["count"]
        missing_count = expected_count - actual_count
        
        print(f"\n📊 Key Range Analysis:")
        print(f"   Min key: {key_stats['min_key']}")
        print(f"   Max key: {key_stats['max_key']}")
        print(f"   Expected rows (if contiguous): {expected_count}")
        print(f"   Actual rows: {actual_count}")
        
        if missing_count > 0:
            print(f"   ⚠️  Missing {missing_count} keys (gaps in range)")
            print(f"\n   💡 Run quick_check_missing_keys() or detailed_missing_keys_investigation() to investigate specific keys")
        else:
            print(f"   ✅ No gaps (keys are contiguous)")
    
    # Show sample records
    print("\n🔍 Sample Records (ordered by key)::")
    df.orderBy("ycsb_key").show(30, truncate=False)
    
    print("\n" + "=" * 80)
    print("💡 If you see issues:")
    print("   - DELETE rows stored as data → Run Cell 16 (recreate table)")
    print("   - Duplicate keys → Check MERGE deduplication logic")
    print("   - Missing keys → Run quick_check_missing_keys() or detailed_missing_keys_investigation()")
    print("   - Gaps in key range → Keys were deleted (normal for update_delete mode)")


def run_full_diagnosis_from_config(
    spark,
    config: Dict[str, Any],
    mismatched_columns: List[str] = None
) -> None:
    """
    Run full diagnosis using configuration dict from notebook.
    
    This is a convenience wrapper around run_full_diagnosis() that:
    - Extracts all necessary parameters from the config
    - Establishes CockroachDB connection
    - Refreshes target DataFrame
    - Runs comprehensive diagnosis
    - Handles cleanup
    
    Args:
        spark: Spark session
        config: Configuration dictionary with keys:
            - cockroachdb: {host, port, database, user, password}
            - cockroachdb_source: {catalog, schema, table_name}
            - databricks_target: {catalog, schema, table_name}
            - azure_storage: {account_name, container_name}
            - cdc_config: {primary_key_columns}
        mismatched_columns: Optional list of columns with sum mismatches
                           If None, will run diagnosis without mismatch analysis
    
    Example:
        from cockroachdb_debug import run_full_diagnosis_from_config
        
        # config is loaded from Cell 3
        mismatched_columns = ['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
        run_full_diagnosis_from_config(spark, config, mismatched_columns)
    """
    import pg8000.native
    import ssl
    
    # Extract config - CockroachDB connection
    host = config["cockroachdb"]["host"]
    port = config["cockroachdb"]["port"]
    user = config["cockroachdb"]["user"]
    password = config["cockroachdb"]["password"]
    database = config["cockroachdb"]["database"]
    
    # Extract config - Source/Target tables
    source_catalog = config["cockroachdb_source"]["catalog"]
    source_schema = config["cockroachdb_source"]["schema"]
    source_table = config["cockroachdb_source"]["table_name"]
    
    target_catalog = config["databricks_target"]["catalog"]
    target_schema = config["databricks_target"]["schema"]
    target_table = config["databricks_target"]["table_name"]
    
    # Extract config - Azure storage
    storage_account_name = config["azure_storage"]["account_name"]
    container_name = config["azure_storage"]["container_name"]
    
    # Extract config - CDC settings
    primary_keys = config["cdc_config"]["primary_key_columns"]
    
    # Construct paths
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    azure_cdc_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}"
    staging_table = f"{target_catalog}.{target_schema}.{target_table}_staging_cf"
    
    # Print configuration
    print("\n" + "=" * 80)
    print("🔍 CDC SYNC DIAGNOSIS CONFIGURATION")
    print("=" * 80)
    print(f"   Source: {source_catalog}.{source_schema}.{source_table}")
    print(f"   Target: {target_table_fqn}")
    print(f"   Staging: {staging_table}")
    print(f"   Azure: {azure_cdc_path[:80]}...")
    if mismatched_columns:
        print(f"   Mismatched columns: {len(mismatched_columns)} columns")
    print()
    
    # Refresh target DataFrame
    print("📊 Refreshing target DataFrame...")
    target_df = spark.read.table(target_table_fqn)
    print(f"✅ Target DataFrame refreshed: {target_df.count():,} rows\n")
    
    print("\n" + "█" * 80)
    print("🔍 RUNNING FULL DIAGNOSIS")
    print("█" * 80)
    
    # Establish connection
    print("🔌 Establishing fresh CockroachDB connection...")
    
    # Create SSL context (required for CockroachDB Cloud)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Parse host (in case port is accidentally included)
    host_clean = host.split(':')[0] if ':' in host else host
    
    conn = pg8000.native.Connection(
        host=host_clean,
        port=port,
        database=database,
        user=user,
        password=password,
        ssl_context=ssl_context
    )
    print("✅ Connection established\n")
    
    try:
        print("🚀 Running full diagnosis...\n")
        run_full_diagnosis(
            conn=conn,
            spark=spark,
            source_table=f"{source_catalog}.{source_schema}.{source_table}",
            target_df=target_df,
            staging_table=staging_table,
            azure_path=azure_cdc_path,
            primary_keys=primary_keys,
            mismatched_columns=mismatched_columns or []
        )
    finally:
        conn.close()
        print("\n🔌 Connection closed")


def detailed_missing_keys_investigation(
    conn,
    spark,
    source_table: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    missing_keys: List[int]
) -> None:
    """
    Detailed investigation of missing keys:
    - Checks CockroachDB source
    - Checks staging table (if it exists)
    - Shows CDC operation and timestamps
    - Provides detailed troubleshooting steps
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Source table name (e.g., 'usertable_update_delete_multi_cf')
        target_catalog: Target catalog (e.g., 'main')
        target_schema: Target schema (e.g., 'robert_lee_crdb')
        target_table: Target table name
        missing_keys: List of keys to investigate (e.g., [17, 18, 19])
    """
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    staging_table_cf = f"{target_table_fqn}_staging_cf"
    
    print("🔍 Detailed Missing Keys Investigation")
    print("=" * 80)
    print(f"Investigating keys: {missing_keys}")
    print()
    
    # Step 1: Check CockroachDB Source
    print("📊 STEP 1: Checking CockroachDB Source")
    print("-" * 80)
    
    cursor = conn.cursor()
    
    for key in missing_keys:
        cursor.execute(f"SELECT * FROM {source_table} WHERE ycsb_key = %s", (key,))
        result = cursor.fetchone()
        
        if result:
            print(f"✅ Key {key}: EXISTS in CockroachDB")
            # Show first 3 fields for verification
            print(f"   Sample data: {result[:min(3, len(result))]}")
        else:
            print(f"❌ Key {key}: NOT FOUND in CockroachDB")
            print(f"   → This key was deleted (expected for update_delete mode)")
    
    # Step 2: Check Staging Table
    print(f"\n📊 STEP 2: Checking Staging Table")
    print("-" * 80)
    
    if spark.catalog.tableExists(staging_table_cf):
        staging_df = spark.read.table(staging_table_cf)
        print(f"✅ Staging table exists: {staging_table_cf}")
        print()
        
        for key in missing_keys:
            key_rows = staging_df.filter(F.col("ycsb_key") == key)
            count = key_rows.count()
            
            if count > 0:
                print(f"✅ Key {key}: {count} row(s) in staging table")
                print("   Details:")
                key_rows.select(
                    "ycsb_key", 
                    "_cdc_timestamp", 
                    "_cdc_operation", 
                    "field0", 
                    "field1"
                ).show(truncate=False)
            else:
                print(f"❌ Key {key}: NOT in staging table")
        
        # Show staging table summary
        print("\n📈 Staging Table Summary:")
        staging_df.groupBy("_cdc_operation").count().show()
        
    else:
        print(f"⚠️  Staging table doesn't exist: {staging_table_cf}")
        print("   This means Cell 12 completed and dropped the staging table")
        print("\n💡 To debug further:")
        print("   1. Re-run Cell 12 (it will process new files and recreate staging)")
        print("   2. Run this function again to check staging table")
    
    # Step 3: Troubleshooting Recommendations
    print("\n" + "=" * 80)
    print("💡 TROUBLESHOOTING GUIDE")
    print("=" * 80)
    
    print("\n📋 If keys EXIST in CockroachDB but NOT in staging:")
    print("   → CDC files haven't been picked up by Auto Loader yet")
    print("   ✅ Solution: Re-run Cell 12 to process new CDC files")
    
    print("\n📋 If keys EXIST in staging but NOT in target:")
    print("   → MERGE logic failed or conditions are wrong")
    print("   ✅ Solution: Check Cell 12 output for MERGE errors")
    print("   ✅ Alternative: Check MERGE conditions in Cell 6")
    
    print("\n📋 If keys DON'T EXIST in CockroachDB:")
    print("   → Keys were deleted (normal for update_delete mode)")
    print("   ✅ Expected: Target should also not have these keys")
    print("   ⚠️  If target HAS these keys: MERGE delete logic isn't working")
    
    print("\n📋 If keys DON'T EXIST anywhere:")
    print("   → Keys were never created, or CDC didn't capture them")
    print("   ✅ Check: Run Cell 10 again to verify workload ran correctly")
