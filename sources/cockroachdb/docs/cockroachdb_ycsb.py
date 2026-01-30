"""
CockroachDB YCSB Table Management

This module provides functions for creating and populating YCSB (Yahoo! Cloud Serving Benchmark)
tables in CockroachDB, which are commonly used for CDC testing.

YCSB Schema:
    - Primary key: ycsb_key (INT)
    - Data columns: field0-9 (TEXT)
    - Column families: Configurable (single or multiple)
"""

import time
import random
from typing import Optional, Dict, Any, List


def create_ycsb_table(
    conn,
    table_name: str,
    column_family_mode: str = "single_cf"
) -> None:
    """
    Create a YCSB table with configurable column families.
    
    Args:
        conn: Database connection (pg8000)
        table_name: Name of the table to create
        column_family_mode: Either 'single_cf' or 'multi_cf'
            - 'single_cf': Standard table (1 column family, default)
            - 'multi_cf': Multiple column families (for testing split_column_families)
    
    Column Family Configuration (multi_cf mode):
        - frequently_read: ycsb_key, field0, field1, field2
        - medium_read: field3, field4, field5
        - rarely_read: field6, field7, field8, field9
    """
    if column_family_mode == "multi_cf":
        # Create table with MULTIPLE column families for testing split_column_families=true
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ycsb_key INT PRIMARY KEY,
            -- Family 1: Frequently accessed fields
            field0 TEXT,
            field1 TEXT,
            field2 TEXT,
            FAMILY frequently_read (ycsb_key, field0, field1, field2),
            
            -- Family 2: Medium-frequency fields
            field3 TEXT,
            field4 TEXT,
            field5 TEXT,
            FAMILY medium_read (field3, field4, field5),
            
            -- Family 3: Rarely accessed fields
            field6 TEXT,
            field7 TEXT,
            field8 TEXT,
            field9 TEXT,
            FAMILY rarely_read (field6, field7, field8, field9)
        )
        """
        family_info = "3 column families (frequently_read, medium_read, rarely_read)"
    else:
        # Create table with SINGLE column family (default)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ycsb_key INT PRIMARY KEY,
            field0 TEXT,
            field1 TEXT,
            field2 TEXT,
            field3 TEXT,
            field4 TEXT,
            field5 TEXT,
            field6 TEXT,
            field7 TEXT,
            field8 TEXT,
            field9 TEXT
        )
        """
        family_info = "1 column family (default primary)"
    
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        conn.commit()
    
    print(f"✅ Table '{table_name}' created (or already exists)")
    print(f"   Column Family Mode: {column_family_mode}")
    print(f"   Column families: {family_info}")


def get_table_stats(conn, table_name: str) -> Dict[str, Any]:
    """
    Get min key, max key, and count for a table.
    
    Args:
        conn: Database connection (pg8000.native.Connection)
        table_name: Name of the table
    
    Returns:
        dict with 'min_key', 'max_key', 'count', 'is_empty'
    """
    # pg8000.native uses conn.run() directly (no cursor needed)
    result = conn.run(f"SELECT MIN(ycsb_key), MAX(ycsb_key), COUNT(*) FROM {table_name}")
    min_key, max_key, count = result[0]
    
    return {
        'min_key': min_key,
        'max_key': max_key,
        'count': count,
        'is_empty': min_key is None and max_key is None
    }


def get_table_stats_spark(spark, table_fqn: str) -> Dict[str, Any]:
    """
    Get min key, max key, and count for a Spark table.
    
    Args:
        spark: SparkSession
        table_fqn: Fully qualified table name (catalog.schema.table)
    
    Returns:
        dict with 'min_key', 'max_key', 'count', 'is_empty'
    
    Example:
        # Get stats from Databricks Delta table
        target_stats = get_table_stats_spark(
            spark, 
            'main.robert_lee_cockroachdb.usertable_update_delete_multi_cf'
        )
        print(f"Target has {target_stats['count']} rows")
    """
    from pyspark.sql import functions as F
    
    target_df = spark.read.table(table_fqn)
    
    # Calculate stats using Spark aggregation
    stats_df = target_df.agg(
        F.min("ycsb_key").alias("min_key"),
        F.max("ycsb_key").alias("max_key"),
        F.count("*").alias("count")
    ).collect()[0]
    
    return {
        'min_key': stats_df['min_key'],
        'max_key': stats_df['max_key'],
        'count': stats_df['count'],
        'is_empty': stats_df['min_key'] is None and stats_df['max_key'] is None
    }


def get_column_sum(conn, table_name: str, column_name: str) -> int:
    """
    Get the sum of a numeric column in a table.
    Text columns have non-numeric characters stripped before casting.
    
    Args:
        conn: Database connection (pg8000.native.Connection)
        table_name: Name of the table
        column_name: Name of the column to sum
    
    Returns:
        Sum of the column (handles mixed text/numeric values)
    
    Example:
        # Sum a text column with mixed values like 'updated_at_1234567890'
        total = get_column_sum(conn, 'usertable_update_delete_multi_cf', 'field0')
    """
    # pg8000.native uses conn.run() directly (no cursor needed)
    # Strip non-numeric chars, handle empty strings, cast to BIGINT
    result = conn.run(f"""
        SELECT SUM(
            CASE 
                WHEN regexp_replace({column_name}::TEXT, '[^0-9]', '', 'g') = '' THEN 0
                ELSE regexp_replace({column_name}::TEXT, '[^0-9]', '', 'g')::BIGINT
            END
        ) 
        FROM {table_name}
    """)
    return result[0][0] if result and result[0][0] is not None else 0


def deduplicate_to_latest(
    df,
    primary_keys: List[str],
    timestamp_col: str = None,
    verbose: bool = False
):
    """
    Deduplicate a Spark DataFrame to keep only the latest row per primary key.
    
    This is essential for APPEND_ONLY mode where the target contains all CDC events
    (multiple versions of the same key). After deduplication, you get the current
    state equivalent to the source table.
    
    Args:
        df: Spark DataFrame to deduplicate
        primary_keys: List of primary key column names
        timestamp_col: Timestamp column name for ordering. If None, auto-detects
                      (_cdc_timestamp or __crdb__updated)
        verbose: Print deduplication progress messages
    
    Returns:
        Deduplicated Spark DataFrame with latest row per key
    
    Example:
        # For append_only mode comparison
        target_df = spark.read.table("catalog.schema.table")
        target_df_latest = deduplicate_to_latest(
            target_df, 
            primary_keys=['ycsb_key'],
            verbose=True
        )
        # Now target_df_latest has 1 row per key (latest state)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Auto-detect timestamp column if not provided
    if timestamp_col is None:
        if '_cdc_timestamp' in df.columns:
            timestamp_col = '_cdc_timestamp'
        elif '__crdb__updated' in df.columns:
            timestamp_col = '__crdb__updated'
        else:
            raise ValueError("No timestamp column found. Provide timestamp_col explicitly or ensure DataFrame has _cdc_timestamp or __crdb__updated")
    
    if verbose:
        original_count = df.count()
        print(f"   Deduplicating using {timestamp_col} (keeping latest row per key)...")
    
    # Create window spec: partition by primary keys, order by timestamp descending
    window_spec = Window.partitionBy(*primary_keys).orderBy(F.col(timestamp_col).desc())
    
    # Add row number and keep only the first row (latest) for each key
    df_with_rank = df.withColumn("_row_num", F.row_number().over(window_spec))
    df_deduplicated = df_with_rank.filter(F.col("_row_num") == 1).drop("_row_num")
    
    if verbose:
        deduplicated_count = df_deduplicated.count()
        print(f"   ✅ Deduplicated: {original_count} rows → {deduplicated_count} rows (latest per key)")
    
    return df_deduplicated


def get_column_sum_spark(df, column_name: str) -> int:
    """
    Get the sum of a numeric column in a Spark DataFrame.
    Text columns have non-numeric characters stripped before casting.
    
    Args:
        df: Spark DataFrame
        column_name: Name of the column to sum
    
    Returns:
        Sum of the column (handles mixed text/numeric values)
    
    Example:
        # Sum a text column with mixed values like 'updated_at_1234567890'
        target_df = spark.read.table("catalog.schema.table")
        total = get_column_sum_spark(target_df, 'field0')
    """
    from pyspark.sql import functions as F
    
    result = df.select(
        F.sum(
            F.when(
                F.regexp_replace(F.col(column_name).cast('string'), '[^0-9]', '') == '',
                0
            ).otherwise(
                F.regexp_replace(F.col(column_name).cast('string'), '[^0-9]', '').cast('bigint')
            )
        ).alias('sum')
    ).collect()[0]['sum']
    
    return result if result is not None else 0


def get_column_sum_spark_deduplicated(
    df,
    column_name: str,
    primary_keys: List[str],
    timestamp_col: str = None,
    verbose: bool = False
) -> int:
    """
    Get the sum of a numeric column after deduplicating to latest row per key.
    
    This is the correct way to compare append_only target tables with source tables:
    1. Deduplicate target to latest state per key
    2. Calculate sum from deduplicated data
    
    Args:
        df: Spark DataFrame (possibly with duplicate keys in append_only mode)
        column_name: Name of the column to sum
        primary_keys: List of primary key column names
        timestamp_col: Timestamp column for ordering (auto-detected if None)
        verbose: Print deduplication progress messages
    
    Returns:
        Sum of the column after deduplication
    
    Example:
        # For append_only mode verification (Cell 14)
        target_df = spark.read.table("catalog.schema.table")
        
        # Compare source vs target (deduplicated)
        source_sum = get_column_sum(conn, source_table, 'field0')
        target_sum = get_column_sum_spark_deduplicated(
            target_df, 
            'field0',
            primary_keys=['ycsb_key'],
            verbose=True
        )
        
        if source_sum == target_sum:
            print("✅ Sums match!")
    """
    # Deduplicate first
    df_deduplicated = deduplicate_to_latest(df, primary_keys, timestamp_col, verbose)
    
    # Then calculate sum
    return get_column_sum_spark(df_deduplicated, column_name)


def insert_ycsb_snapshot(
    conn,
    table_name: str,
    snapshot_count: int
) -> bool:
    """
    Insert initial snapshot data into a YCSB table.
    
    Only inserts if the table is empty. If table already contains data,
    this function skips the insert and returns False.
    
    Args:
        conn: Database connection
        table_name: Name of the table
        snapshot_count: Number of rows to insert (starting from key 0)
    
    Returns:
        True if data was inserted, False if table already had data
    """
    # Check if table is empty
    stats = get_table_stats(conn, table_name)
    
    if stats['is_empty']:
        # Table is empty - insert snapshot data
        print(f"📊 Table is empty. Inserting {snapshot_count} initial rows (snapshot phase)...")
        
        with conn.cursor() as cur:
            # Use generate_series for efficient bulk insert
            insert_sql = f"""
            INSERT INTO {table_name} 
            (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
            SELECT 
                i AS ycsb_key,
                'snapshot_value_' || i || '_0' AS field0,
                'snapshot_value_' || i || '_1' AS field1,
                'snapshot_value_' || i || '_2' AS field2,
                'snapshot_value_' || i || '_3' AS field3,
                'snapshot_value_' || i || '_4' AS field4,
                'snapshot_value_' || i || '_5' AS field5,
                'snapshot_value_' || i || '_6' AS field6,
                'snapshot_value_' || i || '_7' AS field7,
                'snapshot_value_' || i || '_8' AS field8,
                'snapshot_value_' || i || '_9' AS field9
            FROM generate_series(0, %s - 1) AS i
            """
            
            cur.execute(insert_sql, (snapshot_count,))
            conn.commit()
        
        print(f"✅ Sample data inserted using generate_series")
        print(f"   Rows inserted: {snapshot_count} (keys 0 to {snapshot_count - 1})")
        return True
    else:
        # Table already has data - skip insert
        print(f"ℹ️  Table already contains data - skipping snapshot insert")
        print(f"   Current key range: {stats['min_key']} to {stats['max_key']}")
        print(f"   Tip: If you want to re-run the snapshot, drop the table first (see Cleanup cells)")
        return False


def run_ycsb_workload(
    conn,
    table_name: str,
    insert_count: int,
    update_count: int,
    delete_count: int
) -> Dict[str, Any]:
    """
    Run a YCSB-style workload with INSERT/UPDATE/DELETE operations.
    
    This function:
    1. Inserts new rows starting from max_key + 1
    2. Updates existing rows starting from min_key (modifies field0 with timestamp)
    3. Deletes oldest rows starting from min_key
    
    Args:
        conn: Database connection
        table_name: Name of the table
        insert_count: Number of rows to insert
        update_count: Number of rows to update
        delete_count: Number of rows to delete
    
    Returns:
        dict with 'before' and 'after' stats, including row counts and key ranges
    """
    # Get current table state
    stats_before = get_table_stats(conn, table_name)
    min_key = stats_before['min_key']
    max_key = stats_before['max_key']
    count_before = stats_before['count']
    
    print(f"📊 Current table state:")
    print(f"   Min key: {min_key}, Max key: {max_key}, Total rows: {count_before}")
    print()
    
    with conn.cursor() as cur:
        
        # 1. INSERT: Add new rows starting from max_key + 1 (using generate_series)
        print(f"➕ Running {insert_count} INSERTs (keys {max_key + 1} to {max_key + insert_count})...")
        insert_sql = f"""
        INSERT INTO {table_name} 
        (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
        SELECT 
            i AS ycsb_key,
            'inserted_value_' || i || '_0' AS field0,
            'inserted_value_' || i || '_1' AS field1,
            'inserted_value_' || i || '_2' AS field2,
            'inserted_value_' || i || '_3' AS field3,
            'inserted_value_' || i || '_4' AS field4,
            'inserted_value_' || i || '_5' AS field5,
            'inserted_value_' || i || '_6' AS field6,
            'inserted_value_' || i || '_7' AS field7,
            'inserted_value_' || i || '_8' AS field8,
            'inserted_value_' || i || '_9' AS field9
        FROM generate_series(%s, %s) AS i
        """
        cur.execute(insert_sql, (max_key + 1, max_key + insert_count))
        
        # 2. UPDATE: Update existing rows starting from min_key (single UPDATE statement)
        print(f"📝 Running {update_count} UPDATEs (keys {min_key} to {min_key + update_count - 1})...")
        timestamp = int(time.time())
        cur.execute(f"""
            UPDATE {table_name}
            SET field0 = %s
            WHERE ycsb_key >= %s AND ycsb_key < %s
        """, (f"updated_at_{timestamp}", min_key, min_key + update_count))
        
        # 3. DELETE: Delete oldest rows starting from min_key (single DELETE)
        delete_max = min_key + delete_count - 1
        print(f"🗑️  Running {delete_count} DELETEs (keys {min_key} to {delete_max})...")
        cur.execute(f"""
            DELETE FROM {table_name}
            WHERE ycsb_key >= %s AND ycsb_key <= %s
        """, (min_key, delete_max))
        
        conn.commit()
    
    # Get final table state
    stats_after = get_table_stats(conn, table_name)
    min_key_after = stats_after['min_key']
    max_key_after = stats_after['max_key']
    count_after = stats_after['count']
    
    print(f"\n✅ Workload complete")
    print(f"   Inserts: {insert_count}")
    print(f"   Updates: {update_count}")
    print(f"   Deletes: {delete_count}")
    print(f"   Before: {count_before} rows (keys {min_key}-{max_key})")
    print(f"   After:  {count_after} rows (keys {min_key_after}-{max_key_after})")
    print(f"   Net change: {count_after - count_before:+d} rows")
    
    return {
        'before': stats_before,
        'after': stats_after,
        'inserts': insert_count,
        'updates': update_count,
        'deletes': delete_count
    }


def insert_ycsb_snapshot_with_random_nulls(
    conn,
    table_name: str,
    snapshot_count: int,
    null_probability: float = 0.3,
    columns_to_randomize: Optional[List[str]] = None,
    seed: Optional[int] = None,
    force_all_null_row: bool = True
) -> bool:
    """
    Insert initial snapshot data with random NULL values for testing column family handling.
    
    This function is specifically designed to test the NULL coalescing logic in CDC
    ingestion, particularly for column family scenarios where different columns may
    be NULL in different fragments.
    
    Args:
        conn: Database connection
        table_name: Name of the table
        snapshot_count: Number of rows to insert (starting from key 0)
        null_probability: Probability (0.0 to 1.0) that any given column will be NULL
                         Default: 0.3 (30% of columns will be NULL)
        columns_to_randomize: List of column names to randomize (e.g., ['field3', 'field4', 'field5'])
                            If None, all data columns (field0-9) will be randomized
                            The primary key (ycsb_key) is never NULL
        seed: Random seed for reproducibility (default: None for random)
        force_all_null_row: If True, guarantees that the first row (key 0) will have ALL
                           randomized columns set to NULL. This tests the edge case of
                           rows with no data in column families. Default: True
    
    Returns:
        True if data was inserted, False if table already had data
    
    Example:
        # Insert 10 rows with 40% NULL probability for field3-9 only
        insert_ycsb_snapshot_with_random_nulls(
            conn=conn,
            table_name='usertable_update_delete_multi_cf',
            snapshot_count=10,
            null_probability=0.4,
            columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
        )
        
        # This simulates the case where column family 2 and 3 have random NULL values,
        # which tests if the CDC ingestion correctly preserves non-NULL values when
        # merging fragments across time.
    """
    # Check if table is empty
    stats = get_table_stats(conn, table_name)
    
    if not stats['is_empty']:
        print(f"ℹ️  Table already contains data - skipping snapshot insert")
        print(f"   Current key range: {stats['min_key']} to {stats['max_key']}")
        print(f"   Tip: If you want to re-run the snapshot, drop the table first")
        return False
    
    # Set random seed for reproducibility
    if seed is not None:
        random.seed(seed)
    
    # Default: randomize all data columns (field0-9)
    if columns_to_randomize is None:
        columns_to_randomize = [f'field{i}' for i in range(10)]
    
    print(f"📊 Inserting {snapshot_count} initial rows with random NULLs...")
    print(f"   NULL probability: {null_probability:.1%}")
    print(f"   Columns to randomize: {', '.join(columns_to_randomize)}")
    if seed is not None:
        print(f"   Random seed: {seed} (reproducible)")
    if force_all_null_row:
        print(f"   ⚠️  Row 0 will have ALL randomized columns as NULL (edge case testing)")
    
    # Insert rows one by one with random NULLs
    # Note: We use individual INSERTs instead of generate_series because we need
    # to randomize NULL values per row, which can't be done easily in bulk SQL
    inserted_count = 0
    null_count_by_column = {f'field{i}': 0 for i in range(10)}
    
    with conn.cursor() as cur:
        for i in range(snapshot_count):
            # Build column values
            values = {'ycsb_key': i}
            
            # Force all NULLs for the first row if requested (edge case testing)
            force_null_this_row = (i == 0 and force_all_null_row)
            
            for field_num in range(10):
                field_name = f'field{field_num}'
                
                if field_name in columns_to_randomize:
                    # Force NULL for edge case testing, or randomly decide
                    if force_null_this_row or random.random() < null_probability:
                        values[field_name] = None
                        null_count_by_column[field_name] += 1
                    else:
                        values[field_name] = f'snapshot_value_{i}_{field_num}'
                else:
                    # Not randomized - always has value
                    values[field_name] = f'snapshot_value_{i}_{field_num}'
            
            # Insert the row
            cur.execute(f"""
                INSERT INTO {table_name} 
                (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                values['ycsb_key'],
                values['field0'], values['field1'], values['field2'],
                values['field3'], values['field4'], values['field5'],
                values['field6'], values['field7'], values['field8'], values['field9']
            ))
            inserted_count += 1
        
        conn.commit()
    
    # Print summary
    print(f"\n✅ Sample data inserted with random NULLs")
    print(f"   Rows inserted: {inserted_count} (keys 0 to {inserted_count - 1})")
    print(f"\n   NULL count by column:")
    for field_num in range(10):
        field_name = f'field{field_num}'
        null_count = null_count_by_column[field_name]
        null_pct = (null_count / snapshot_count * 100) if snapshot_count > 0 else 0
        
        if field_name in columns_to_randomize:
            print(f"      {field_name}: {null_count:3d} NULLs ({null_pct:5.1f}%) [randomized]")
        else:
            print(f"      {field_name}:   0 NULLs (  0.0%) [not randomized]")
    
    return True


def run_ycsb_workload_with_random_nulls(
    conn,
    table_name: str,
    insert_count: int,
    update_count: int,
    delete_count: int,
    null_probability: float = 0.5,
    columns_to_randomize: Optional[List[str]] = None,
    seed: Optional[int] = None,
    force_all_null_update: bool = True
) -> Dict[str, Any]:
    """
    Run a YCSB-style workload with INSERT/UPDATE/DELETE operations, including random NULL updates.
    
    This function is similar to run_ycsb_workload() but UPDATE operations will randomly
    set columns to NULL, testing the NULL coalescing logic in CDC ingestion.
    
    Args:
        conn: Database connection
        table_name: Name of the table
        insert_count: Number of rows to insert
        update_count: Number of rows to update
        delete_count: Number of rows to delete
        null_probability: Probability (0.0 to 1.0) that any column will be NULL in UPDATE
                         Default: 0.5 (50% chance of NULL)
        columns_to_randomize: List of column names to randomize in UPDATEs
                            If None, all data columns (field0-9) will be randomized
        seed: Random seed for reproducibility (default: None for random)
        force_all_null_update: If True, guarantees that the first updated row will have ALL
                              randomized columns set to NULL. This tests the edge case where
                              an UPDATE completely wipes out column family data. Default: True
    
    Returns:
        dict with 'before' and 'after' stats, including row counts and key ranges
    
    Example:
        # Run workload with 50% NULL probability for field3-9
        run_ycsb_workload_with_random_nulls(
            conn=conn,
            table_name='usertable_update_delete_multi_cf',
            insert_count=5,
            update_count=3,
            delete_count=2,
            null_probability=0.5,
            columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
            force_all_null_update=True
        )
        
        This simulates a real-world scenario where UPDATEs may set column family values to NULL,
        testing if the CDC ingestion correctly preserves earlier non-NULL values from snapshot.
    """
    # Set random seed for reproducibility
    if seed is not None:
        random.seed(seed)
    
    # Default: randomize all data columns (field0-9)
    if columns_to_randomize is None:
        columns_to_randomize = [f'field{i}' for i in range(10)]
    
    # Get current table state
    stats_before = get_table_stats(conn, table_name)
    min_key = stats_before['min_key']
    max_key = stats_before['max_key']
    count_before = stats_before['count']
    
    print(f"📊 Current table state:")
    print(f"   Min key: {min_key}, Max key: {max_key}, Total rows: {count_before}")
    print()
    
    with conn.cursor() as cur:
        
        # 1. INSERT: Add new rows starting from max_key + 1 (using generate_series)
        print(f"➕ Running {insert_count} INSERTs (keys {max_key + 1} to {max_key + insert_count})...")
        insert_sql = f"""
        INSERT INTO {table_name} 
        (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
        SELECT 
            i AS ycsb_key,
            'inserted_value_' || i || '_0' AS field0,
            'inserted_value_' || i || '_1' AS field1,
            'inserted_value_' || i || '_2' AS field2,
            'inserted_value_' || i || '_3' AS field3,
            'inserted_value_' || i || '_4' AS field4,
            'inserted_value_' || i || '_5' AS field5,
            'inserted_value_' || i || '_6' AS field6,
            'inserted_value_' || i || '_7' AS field7,
            'inserted_value_' || i || '_8' AS field8,
            'inserted_value_' || i || '_9' AS field9
        FROM generate_series(%s, %s) AS i
        """
        cur.execute(insert_sql, (max_key + 1, max_key + insert_count))
        
        # 2. UPDATE: Update existing rows with random NULLs
        print(f"📝 Running {update_count} UPDATEs with random NULLs...")
        print(f"   NULL probability: {null_probability:.1%}")
        print(f"   Columns to randomize: {', '.join(columns_to_randomize)}")
        if force_all_null_update:
            print(f"   ⚠️  First updated row (key {min_key}) will have ALL randomized columns as NULL")
        
        timestamp = int(time.time())
        
        # Update rows individually to control NULL randomization
        for idx in range(update_count):
            key = min_key + idx
            
            # Force all NULLs for the first update if requested (edge case testing)
            force_null_this_update = (idx == 0 and force_all_null_update)
            
            # Build UPDATE statement with random NULLs
            update_parts = []
            for field_num in range(10):
                field_name = f'field{field_num}'
                
                if field_name in columns_to_randomize:
                    # Force NULL for edge case, or randomly decide
                    if force_null_this_update or random.random() < null_probability:
                        update_parts.append(f"{field_name} = NULL")
                    else:
                        update_parts.append(f"{field_name} = 'updated_at_{timestamp}_{field_num}'")
                else:
                    # Not randomized - always update with value
                    if field_name == 'field0':
                        # Special handling for field0 (timestamp marker)
                        update_parts.append(f"{field_name} = 'updated_at_{timestamp}'")
                    else:
                        update_parts.append(f"{field_name} = 'updated_at_{timestamp}_{field_num}'")
            
            update_sql = f"""
                UPDATE {table_name}
                SET {', '.join(update_parts)}
                WHERE ycsb_key = %s
            """
            cur.execute(update_sql, (key,))
        
        # 3. DELETE: Delete oldest rows starting from min_key (single DELETE)
        delete_max = min_key + delete_count - 1
        print(f"🗑️  Running {delete_count} DELETEs (keys {min_key} to {delete_max})...")
        cur.execute(f"""
            DELETE FROM {table_name}
            WHERE ycsb_key >= %s AND ycsb_key <= %s
        """, (min_key, delete_max))
        
        conn.commit()
    
    # Get final table state
    stats_after = get_table_stats(conn, table_name)
    min_key_after = stats_after['min_key']
    max_key_after = stats_after['max_key']
    count_after = stats_after['count']
    
    print(f"\n✅ Workload complete")
    print(f"   Inserts: {insert_count}")
    print(f"   Updates: {update_count} (with random NULLs)")
    print(f"   Deletes: {delete_count}")
    print(f"   Before: {count_before} rows (keys {min_key}-{max_key})")
    print(f"   After:  {count_after} rows (keys {min_key_after}-{max_key_after})")
    print(f"   Net change: {count_after - count_before:+d} rows")
    
    return {
        'before': stats_before,
        'after': stats_after,
        'inserts': insert_count,
        'updates': update_count,
        'deletes': delete_count,
        'null_probability': null_probability
    }
