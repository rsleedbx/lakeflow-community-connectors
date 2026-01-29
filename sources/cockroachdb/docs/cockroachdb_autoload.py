"""
CockroachDB CDC Auto Loader Functions

These are the original 4 CDC ingestion functions that were previously defined 
in the notebook but were later removed. This file restores them from the backup
(cockroachdb-cdc-tutorial-2-after-dup-removal.ipynb).

History:
- These functions existed in the notebook's Cell 6
- They were later deleted/removed
- This file recreates them based on the backup file
- They represent the code BEFORE the column family NULL fix was applied

Note: For the updated column family NULL fix (deduplicate_to_latest_state parameter),
see merge_column_family_fragments() in cockroachdb.py
"""

from pyspark.sql import functions as F


def ingest_cdc_append_only_single_family(
    storage_account_name, container_name, 
    source_catalog, source_schema, source_table, 
    target_catalog, target_schema, target_table,
    spark
):
    """
    Ingest CDC events in APPEND-ONLY mode for single column family tables.
    
    This function:
    - Reads Parquet CDC files from Azure using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - Writes all events (INSERT/UPDATE/DELETE) as rows to Delta table
    - Does NOT apply deletes or deduplicate updates (append_only)
    
    Use this for:
    - Audit logs and full history tracking
    - Tables WITHOUT column families (split_column_families=false)
    - Simple CDC pipelines without MERGE logic
    
    Args:
        storage_account_name: Azure storage account name
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_catalog: Databricks catalog
        target_schema: Databricks schema
        target_table: Target table name
        spark: SparkSession
    
    Returns:
        StreamingQuery object
    """
    # Build paths
    source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
    checkpoint_path = f"/checkpoints/{target_schema}_{target_table}"
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    
    print("📖 Ingesting CDC events (Append-Only Mode)")
    print("=" * 80)
    print(f"Mode: APPEND-ONLY (Single Column Family)")
    print(f"Source: {source_catalog}.{source_schema}.{source_table} (CockroachDB)")
    print(f"Target: {target_table_fqn} (Databricks Delta)")
    print(f"Source path: {source_path}/ (all dates, recursively)")
    print(f"File filter: *{source_table}*.parquet")
    print(f"   ✅ Includes: Data files")
    print(f"   ❌ Excludes: .RESOLVED, _metadata/, _SUCCESS, etc.")
    print()
    
    # Read with Auto Loader (production-grade filtering)
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("pathGlobFilter", f"*{source_table}*.parquet")
        .option("recursiveFileLookup", "true")
        .load(source_path)
    )
    
    print("✅ Schema inferred from data files")
    print("   (Filtering matches cockroachdb.py production code)")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    df = raw_df.select(
        "*",
        # Convert __crdb__updated (nanoseconds) to timestamp
        F.from_unixtime(
            F.col("__crdb__updated").cast("double").cast("bigint") / 1000000000
        ).cast("timestamp").alias("_cdc_timestamp"),
        # Map event type
        F.when(F.col("__crdb__event_type") == "d", "DELETE")
         .otherwise("UPSERT")
         .alias("_cdc_operation")
    ).drop("__crdb__updated", "__crdb__event_type")
    
    # Write to Delta table (append_only)
    query = (df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table_fqn)
    )
    
    print("⏳ Processing CDC events...")
    return query


def ingest_cdc_with_merge_single_family(
    storage_account_name, container_name,
    source_catalog, source_schema, source_table,
    target_catalog, target_schema, target_table,
    primary_key_columns,
    spark
):
    """
    Ingest CDC events with MERGE logic for single column family tables.
    
    This function:
    - Reads Parquet CDC files from Azure using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - Deduplicates events within each microbatch (handles column family fragments)
    - Applies MERGE logic to target Delta table:
      * UPDATE: When key exists and timestamp is newer
      * DELETE: When key exists and operation is DELETE
      * INSERT: When key doesn't exist and operation is UPSERT
    - Preserves _cdc_operation column for monitoring and observability
    
    Use this for:
    - Applications needing current state (not history)
    - Tables WITHOUT column families (split_column_families=false)
    - Production CDC pipelines with UPDATE/DELETE support
    - Lower storage requirements (only latest state)
    
    Target table will contain:
    - All data columns from source
    - _cdc_operation: "UPSERT" (shows last operation on each row)
    - _cdc_timestamp: Timestamp of last CDC event
    
    Args:
        storage_account_name: Azure storage account name
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_catalog: Databricks catalog
        target_schema: Databricks schema
        target_table: Target table name
        primary_key_columns: List of primary key column names (e.g., ['ycsb_key'])
        spark: SparkSession
    
    Returns:
        Dict with query, staging_table, target_table, raw_count, deduped_count, merged
    """
    from pyspark.sql import functions as F, Window
    from delta.tables import DeltaTable
    
    # Build paths
    source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
    checkpoint_path = f"/checkpoints/{target_schema}_{target_table}_merge"
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    
    print("📖 Ingesting CDC events (MERGE Mode)")
    print("=" * 80)
    print(f"Mode: MERGE (Apply UPDATE/DELETE)")
    print(f"Source: {source_catalog}.{source_schema}.{source_table} (CockroachDB)")
    print(f"Target: {target_table_fqn} (Databricks Delta)")
    print(f"Primary keys: {primary_key_columns}")
    print(f"Source path: {source_path}/ (all dates, recursively)")
    print(f"File filter: *{source_table}*.parquet")
    print(f"   ✅ Includes: Data files")
    print(f"   ❌ Excludes: .RESOLVED, _metadata/, _SUCCESS, etc.")
    print()
    
    # Read with Auto Loader (production-grade filtering)
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("pathGlobFilter", f"*{source_table}*.parquet")
        .option("recursiveFileLookup", "true")
        .load(source_path)
    )
    
    print("✅ Schema inferred from data files")
    print("   (Filtering matches cockroachdb.py production code)")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    transformed_df = raw_df.select(
        "*",
        # Convert __crdb__updated (nanoseconds) to timestamp
        F.from_unixtime(
            F.col("__crdb__updated").cast("double").cast("bigint") / 1000000000
        ).cast("timestamp").alias("_cdc_timestamp"),
        # Map event type
        F.when(F.col("__crdb__event_type") == "d", "DELETE")
         .otherwise("UPSERT")
         .alias("_cdc_operation")
    ).drop("__crdb__event_type", "__crdb__updated")
    
    print("✅ CDC transformations applied (streaming compatible)")
    print("   ℹ️  Deduplication will happen in Stage 2 (batch mode)")
    print()
    
    # ========================================================================
    # STAGE 1: Stream to Staging Table (Serverless Compatible - No Python UDFs)
    # ========================================================================
    staging_table_fqn = f"{target_table_fqn}_staging"
    
    print("🔷 STAGE 1: Streaming to staging table (no Python UDFs)")
    print(f"   Staging: {staging_table_fqn}")
    print()
    
    # Write to staging table (pure Spark, no foreachBatch, no window functions)
    query = (transformed_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(staging_table_fqn)
    )
    
    print("⏳ Streaming CDC events to staging table...")
    query.awaitTermination()
    print("✅ Stream completed\n")
    
    # ========================================================================
    # STAGE 2: Batch MERGE from Staging to Target (Runs on Driver)
    # ========================================================================
    print("🔷 STAGE 2: Applying MERGE logic (batch operation)")
    print(f"   Source: {staging_table_fqn}")
    print(f"   Target: {target_table_fqn}")
    print()
    
    # Read staging table (batch mode - window functions allowed!)
    staging_df_raw = spark.read.table(staging_table_fqn)
    staging_count_raw = staging_df_raw.count()
    print(f"   📊 Raw staging events: {staging_count_raw}")
    
    if staging_count_raw == 0:
        print("   ℹ️  No new events to process")
        return {"query": query, "staging_table": staging_table_fqn, "merged": 0}
    
    # Deduplicate by primary key (batch mode, matches cockroachdb.py logic)
    # Keep only LATEST event per primary key based on timestamp
    from pyspark.sql import Window
    
    print(f"   🔄 Deduplicating by primary keys: {primary_key_columns}...")
    window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
    staging_df = (staging_df_raw
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    
    staging_count = staging_df.count()
    duplicates_removed = staging_count_raw - staging_count
    print(f"   ✅ Deduplicated: {staging_count} unique events ({duplicates_removed} duplicates removed)")
    
    if staging_count == 0:
        print("   ℹ️  All events were duplicates")
        return {"query": query, "staging_table": staging_table_fqn, "merged": 0}
    
    # Create target table if it doesn't exist
    if not spark.catalog.tableExists(target_table_fqn):
        print(f"   📝 Creating new target table: {target_table_fqn}")
        
        # CRITICAL: Proper DELETE handling for initial table creation
        # This matches cockroachdb.py reference implementation
        
        # 1. Get keys that have DELETE events
        delete_keys = staging_df.filter(F.col("_cdc_operation") == "DELETE") \
            .select(*primary_key_columns) \
            .distinct()
        delete_count = delete_keys.count()
        
        # 2. Get all non-DELETE rows
        active_rows = staging_df.filter(F.col("_cdc_operation") != "DELETE")
        active_count = active_rows.count()
        
        # 3. Exclude rows with keys that are deleted (left anti join)
        # This handles case where key has UPSERT at T1, DELETE at T2
        rows_after_delete = active_rows.join(
            delete_keys,
            on=primary_key_columns,
            how="left_anti"
        )
        after_delete_count = rows_after_delete.count()
        
        # Note: staging_df is already deduplicated, so final_rows = rows_after_delete
        final_rows = rows_after_delete
        final_count = after_delete_count
        
        if delete_count > 0:
            print(f"   ℹ️  Found {delete_count} DELETE events")
            print(f"   ℹ️  Active rows before DELETE: {active_count}")
            print(f"   ℹ️  Active rows after DELETE: {after_delete_count}")
            print(f"   ℹ️  Rows removed by DELETE: {active_count - after_delete_count}")
        
        # Keep ALL columns including _cdc_operation for monitoring
        final_rows.write.format("delta").saveAsTable(target_table_fqn)
        merged_count = final_count
        print(f"   ✅ Created table with {merged_count} initial rows")
        print(f"      Schema includes _cdc_operation for observability\n")
    else:
        # Get Delta table and apply MERGE
        delta_table = DeltaTable.forName(spark, target_table_fqn)
        
        # Check if _cdc_operation exists in target (might be missing from old tables)
        target_columns = set(spark.read.table(target_table_fqn).columns)
        if "_cdc_operation" not in target_columns:
            print(f"   ⚠️  Target table missing _cdc_operation column (old schema)")
            print(f"   🔧 Adding _cdc_operation column for observability...")
            spark.sql(f"""
                ALTER TABLE {target_table_fqn} 
                ADD COLUMN _cdc_operation STRING
            """)
            print(f"   ✅ Column added\n")
        
        # Build join condition dynamically
        join_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key_columns])
        
        # Get all data columns (KEEP _cdc_operation for observability!)
        data_columns = [col for col in staging_df.columns]
        
        # Build UPDATE/INSERT clauses dynamically
        update_set = {col: f"source.{col}" for col in data_columns}
        insert_values = {col: f"source.{col}" for col in data_columns}
        
        print(f"   🔄 Executing MERGE...")
        print(f"      Join: {join_condition}")
        print(f"      ℹ️  _cdc_operation will be preserved for monitoring")
        
        # Apply MERGE (runs on driver, not workers)
        (delta_table.alias("target").merge(
            staging_df.alias("source"),
            join_condition
        )
        .whenMatchedUpdate(
            condition="source._cdc_operation = 'UPSERT' AND source._cdc_timestamp > target._cdc_timestamp",
            set=update_set
        )
        .whenMatchedDelete(
            condition="source._cdc_operation = 'DELETE'"
        )
        .whenNotMatchedInsert(
            condition="source._cdc_operation = 'UPSERT'",
            values=insert_values
        )
        .execute())
        
        merged_count = staging_count
        print(f"   ✅ MERGE complete: processed {merged_count} events\n")
    
    print("=" * 80)
    print("✅ CDC INGESTION COMPLETE (TWO-STAGE MERGE)")
    print("=" * 80)
    print(f"📊 Raw events: {staging_count_raw}")
    print(f"📊 After deduplication: {staging_count} unique events")
    print(f"📊 Staging table: {staging_table_fqn}")
    print(f"📊 Target table:  {target_table_fqn}")
    print()
    print("📋 Target table includes:")
    print("   - All data columns from source")
    print("   - _cdc_operation: UPSERT (for monitoring)")
    print("   - _cdc_timestamp: Last CDC event timestamp")
    print()
    print("💡 TIP: Staging table can be dropped after successful MERGE:")
    print(f"   spark.sql('DROP TABLE IF EXISTS {staging_table_fqn}')")
    
    return {
        "query": query,
        "staging_table": staging_table_fqn,
        "target_table": target_table_fqn,
        "raw_count": staging_count_raw,
        "deduped_count": staging_count,
        "merged": merged_count
    }


def merge_column_family_fragments(df, primary_key_columns, spark):
    """
    Merge column family fragments into complete rows (streaming-compatible).
    
    When split_column_families=true, CockroachDB creates multiple CDC events per row update,
    one for each column family. This function merges these fragments by:
    1. Grouping by (primary_key + _cdc_timestamp + _cdc_operation)
    2. Using first(col, ignorenulls=True) to coalesce NULL values from different fragments
    
    Each fragment has:
    - Primary key columns (always present)
    - Data for ONE column family (other columns are NULL)
    - Same _cdc_timestamp and _cdc_operation
    
    Args:
        df: Spark DataFrame with potential column family fragments
        primary_key_columns: List of primary key column names (e.g., ['ycsb_key'])
        spark: SparkSession
    
    Returns:
        Merged DataFrame with complete rows
    """
    from pyspark.sql import functions as F
    
    # Get all columns
    all_columns = df.columns
    
    # Metadata columns to preserve (not aggregate as data)
    metadata_columns = {'_cdc_operation', '_cdc_timestamp', '_rescued_data'}
    
    # Data columns = all columns except PK and metadata
    data_columns = [
        col for col in all_columns
        if col not in primary_key_columns 
        and col not in metadata_columns
    ]
    
    # Group by: PK + timestamp + operation (preserves all distinct CDC events)
    group_by_cols = primary_key_columns + ['_cdc_timestamp', '_cdc_operation']
    
    # Build aggregation expressions
    # Use first(col, ignorenulls=True) to merge NULL values from fragments
    agg_exprs = [
        F.first(col, ignorenulls=True).alias(col) 
        for col in data_columns
    ]
    
    # Add metadata columns that aren't in the grouping key
    for col in metadata_columns:
        if col in all_columns and col not in group_by_cols:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    
    # Apply merge
    df_merged = df.groupBy(*group_by_cols).agg(*agg_exprs)
    
    return df_merged


def ingest_cdc_append_only_multi_family(
    storage_account_name, container_name,
    source_catalog, source_schema, source_table,
    target_catalog, target_schema, target_table,
    primary_key_columns,
    spark
):
    """
    Ingest CDC events in APPEND-ONLY mode with COLUMN FAMILY support.
    
    **Two-Stage Approach (Serverless Compatible)**:
    - Stage 1: Stream raw CDC events to staging table (no aggregations)
    - Stage 2: Batch merge column family fragments to target table
    
    This function:
    - Reads Parquet CDC files from Azure using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - MERGES column family fragments (split_column_families=true) in batch mode
    - Writes all events (INSERT/UPDATE/DELETE) as rows to Delta table
    - Does NOT apply deletes or deduplicate updates (append_only)
    
    Use this for:
    - Audit logs with column family tables
    - Tables WITH column families (split_column_families=true)
    - Full history tracking with wide tables
    
    Args:
        storage_account_name: Azure storage account name
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_catalog: Databricks catalog
        target_schema: Databricks schema
        target_table: Target table name
        primary_key_columns: List of primary key column names (required for fragment merging)
        spark: SparkSession
    
    Returns:
        StreamingQuery object
    """
    # Build paths
    source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
    checkpoint_path = f"/checkpoints/{target_schema}_{target_table}"
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    
    print("📖 Ingesting CDC events (Append-Only + Column Families)")
    print("=" * 80)
    print(f"Mode: APPEND-ONLY (Multi Column Family)")
    print(f"Source: {source_catalog}.{source_schema}.{source_table} (CockroachDB)")
    print(f"Target: {target_table_fqn} (Databricks Delta)")
    print(f"Primary keys: {primary_key_columns}")
    print(f"Source path: {source_path}/ (all dates, recursively)")
    print(f"File filter: *{source_table}*.parquet")
    print()
    
    # Read with Auto Loader
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("pathGlobFilter", f"*{source_table}*.parquet")
        .option("recursiveFileLookup", "true")
        .load(source_path)
    )
    
    print("✅ Schema inferred from data files")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    transformed_df = raw_df.select(
        "*",
        F.from_unixtime(
            F.col("__crdb__updated").cast("double").cast("bigint") / 1000000000
        ).cast("timestamp").alias("_cdc_timestamp"),
        F.when(F.col("__crdb__event_type") == "d", "DELETE")
         .otherwise("UPSERT")
         .alias("_cdc_operation")
    ).drop("__crdb__event_type", "__crdb__updated")
    
    print("✅ CDC transformations applied (streaming compatible)")
    print("   ℹ️  Column family merge will happen in Stage 2 (batch mode)")
    print()
    
    # ========================================================================
    # STAGE 1: Stream to Staging Table (Serverless Compatible - No Aggregations)
    # ========================================================================
    staging_table_fqn = f"{target_table_fqn}_staging_cf"
    
    print("🔷 STAGE 1: Streaming to staging table (no aggregations)")
    print(f"   Staging: {staging_table_fqn}")
    print()
    
    # Write to staging table (pure Spark, no aggregations)
    query = (transformed_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(staging_table_fqn)
    )
    
    print("⏳ Streaming CDC events to staging table...")
    query.awaitTermination()
    print("✅ Stream completed\n")
    
    # ========================================================================
    # STAGE 2: Merge Column Families in Batch Mode
    # ========================================================================
    print("🔷 STAGE 2: Merging column family fragments (batch mode)")
    print(f"   Reading from staging: {staging_table_fqn}")
    print(f"   Writing to target: {target_table_fqn}")
    print()
    
    # Read staging table in batch mode
    staging_df = spark.table(staging_table_fqn)
    
    # Merge column family fragments (batch mode - no streaming limitations!)
    print("🔧 Merging column family fragments...")
    print(f"   Grouping by: {primary_key_columns} + _cdc_timestamp + _cdc_operation")
    print(f"   Using first(col, ignorenulls=True) to coalesce fragments")
    merged_df = merge_column_family_fragments(staging_df, primary_key_columns, spark)
    print("✅ Column family fragments merged")
    print()
    
    # Write to final target table (batch mode, append_only)
    print(f"💾 Writing merged events to {target_table_fqn}...")
    merged_df.write.format("delta").mode("append").saveAsTable(target_table_fqn)
    print("✅ Append-only write complete")
    print()
    
    # Clean up staging table
    spark.sql(f"DROP TABLE IF EXISTS {staging_table_fqn}")
    print(f"🧹 Staging table dropped: {staging_table_fqn}")
    print()
    
    return query


def ingest_cdc_with_merge_multi_family(
    storage_account_name, container_name,
    source_catalog, source_schema, source_table,
    target_catalog, target_schema, target_table,
    primary_key_columns,
    spark
):
    """
    Ingest CDC events with MERGE logic and COLUMN FAMILY support.
    
    **Two-Stage Approach (Serverless Compatible)**:
    - Stage 1: Stream raw CDC events to staging table (no aggregations)
    - Stage 2: Batch merge column families + deduplicate + MERGE to target
    
    This function:
    - Reads Parquet CDC files from Azure using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - MERGES column family fragments (split_column_families=true) in batch mode
    - Streams to staging table (Serverless-compatible)
    - Deduplicates by primary key in batch mode
    - Applies MERGE logic to target Delta table
    
    Use this for:
    - Current state replication with column families
    - Tables WITH column families (split_column_families=true)
    - Production CDC with UPDATE/DELETE support
    
    Target table will contain:
    - All data columns from source
    - _cdc_operation: "UPSERT" (shows last operation)
    - _cdc_timestamp: Timestamp of last CDC event
    
    Args:
        storage_account_name: Azure storage account name
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_catalog: Databricks catalog
        target_schema: Databricks schema
        target_table: Target table name
        primary_key_columns: List of primary key column names (required for fragments + MERGE)
        spark: SparkSession
    
    Returns:
        Dict with query, staging_table, target_table, raw_count, deduped_count, merged
    """
    from pyspark.sql import functions as F, Window
    from delta.tables import DeltaTable
    
    # Build paths
    source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
    checkpoint_path = f"/checkpoints/{target_schema}_{target_table}_merge_cf"
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    
    print("📖 Ingesting CDC events (MERGE + Column Families)")
    print("=" * 80)
    print(f"Mode: MERGE with Column Families")
    print(f"Source: {source_catalog}.{source_schema}.{source_table} (CockroachDB)")
    print(f"Target: {target_table_fqn} (Databricks Delta)")
    print(f"Primary keys: {primary_key_columns}")
    print(f"Source path: {source_path}/ (all dates, recursively)")
    print()
    
    # Read with Auto Loader
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("pathGlobFilter", f"*{source_table}*.parquet")
        .option("recursiveFileLookup", "true")
        .load(source_path)
    )
    
    print("✅ Schema inferred from data files")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    transformed_df = raw_df.select(
        "*",
        F.from_unixtime(
            F.col("__crdb__updated").cast("double").cast("bigint") / 1000000000
        ).cast("timestamp").alias("_cdc_timestamp"),
        F.when(F.col("__crdb__event_type") == "d", "DELETE")
         .otherwise("UPSERT")
         .alias("_cdc_operation")
    ).drop("__crdb__event_type", "__crdb__updated")
    
    print("✅ CDC transformations applied (streaming compatible)")
    print("   ℹ️  Column family merge will happen in Stage 2 (batch mode)")
    print()
    
    # ========================================================================
    # STAGE 1: Stream to Staging Table (Serverless Compatible - No Aggregations)
    # ========================================================================
    staging_table_fqn = f"{target_table_fqn}_staging_cf"
    
    print("🔷 STAGE 1: Streaming to staging table (no aggregations)")
    print(f"   Staging: {staging_table_fqn}")
    print()
    
    query = (transformed_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(staging_table_fqn)
    )
    
    print("⏳ Streaming CDC events to staging table...")
    query.awaitTermination()
    print("✅ Stream completed\n")
    
    # ========================================================================
    # STAGE 2: Batch MERGE from Staging to Target
    # ========================================================================
    print("🔷 STAGE 2: Applying MERGE logic (batch operation)")
    print(f"   Source: {staging_table_fqn}")
    print(f"   Target: {target_table_fqn}")
    print()
    
    # Read staging table (batch mode)
    staging_df_raw = spark.read.table(staging_table_fqn)
    staging_count_raw = staging_df_raw.count()
    print(f"   📊 Raw staging events: {staging_count_raw}")
    
    if staging_count_raw == 0:
        print("   ℹ️  No new events to process")
        return {"query": query, "staging_table": staging_table_fqn, "merged": 0}
    
    # Merge column family fragments (batch mode - no streaming limitations!)
    print(f"   🔧 Merging column family fragments...")
    print(f"      Grouping by: {primary_key_columns} + _cdc_timestamp + _cdc_operation")
    staging_df_merged = merge_column_family_fragments(staging_df_raw, primary_key_columns, spark)
    print(f"   ✅ Column family fragments merged")
    
    # Deduplicate by primary key (keep latest event)
    print(f"   🔄 Deduplicating by primary keys: {primary_key_columns}...")
    window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
    staging_df = (staging_df_merged
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    
    staging_count = staging_df.count()
    fragments_removed = staging_count_raw - staging_df_merged.count()
    duplicates_removed = staging_df_merged.count() - staging_count
    print(f"   ✅ Column family fragments coalesced: {fragments_removed} fragments merged")
    print(f"   ✅ Deduplicated: {staging_count} unique events ({duplicates_removed} duplicates removed)")
    
    if staging_count == 0:
        print("   ℹ️  All events were duplicates")
        return {"query": query, "staging_table": staging_table_fqn, "merged": 0}
    
    # Create target table if it doesn't exist
    if not spark.catalog.tableExists(target_table_fqn):
        print(f"   📝 Creating new target table: {target_table_fqn}")
        
        # CRITICAL: Proper DELETE handling for initial table creation
        # This matches cockroachdb.py reference implementation
        
        # 1. Get keys that have DELETE events
        delete_keys = staging_df.filter(F.col("_cdc_operation") == "DELETE") \
            .select(*primary_key_columns) \
            .distinct()
        delete_count = delete_keys.count()
        
        # 2. Get all non-DELETE rows
        active_rows = staging_df.filter(F.col("_cdc_operation") != "DELETE")
        active_count = active_rows.count()
        
        # 3. Exclude rows with keys that are deleted (left anti join)
        # This handles case where key has UPSERT at T1, DELETE at T2
        rows_after_delete = active_rows.join(
            delete_keys,
            on=primary_key_columns,
            how="left_anti"
        )
        after_delete_count = rows_after_delete.count()
        
        # Note: staging_df is already deduplicated, so final_rows = rows_after_delete
        final_rows = rows_after_delete
        final_count = after_delete_count
        
        if delete_count > 0:
            print(f"   ℹ️  Found {delete_count} DELETE events")
            print(f"   ℹ️  Active rows before DELETE: {active_count}")
            print(f"   ℹ️  Active rows after DELETE: {after_delete_count}")
            print(f"   ℹ️  Rows removed by DELETE: {active_count - after_delete_count}")
        
        final_rows.write.format("delta").saveAsTable(target_table_fqn)
        merged_count = final_count
        print(f"   ✅ Created table with {merged_count} initial rows")
        print(f"      Schema includes _cdc_operation for observability\n")
    else:
        # Get Delta table and apply MERGE
        delta_table = DeltaTable.forName(spark, target_table_fqn)
        
        # Check if _cdc_operation exists in target
        target_columns = set(spark.read.table(target_table_fqn).columns)
        if "_cdc_operation" not in target_columns:
            print(f"   ⚠️  Target table missing _cdc_operation column")
            print(f"   🔧 Adding _cdc_operation column...")
            spark.sql(f"ALTER TABLE {target_table_fqn} ADD COLUMN _cdc_operation STRING")
            print(f"   ✅ Column added\n")
        
        # Build join condition
        join_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key_columns])
        
        # Get all columns
        data_columns = [col for col in staging_df.columns]
        update_set = {col: f"source.{col}" for col in data_columns}
        insert_values = {col: f"source.{col}" for col in data_columns}
        
        print(f"   🔄 Executing MERGE...")
        print(f"      Join: {join_condition}")
        print(f"      ℹ️  _cdc_operation preserved for monitoring")
        
        # Apply MERGE
        (delta_table.alias("target").merge(
            staging_df.alias("source"),
            join_condition
        )
        .whenMatchedUpdate(
            condition="source._cdc_operation = 'UPSERT' AND source._cdc_timestamp > target._cdc_timestamp",
            set=update_set
        )
        .whenMatchedDelete(
            condition="source._cdc_operation = 'DELETE'"
        )
        .whenNotMatchedInsert(
            condition="source._cdc_operation = 'UPSERT'",
            values=insert_values
        )
        .execute())
        
        merged_count = staging_count
        print(f"   ✅ MERGE complete: processed {merged_count} events\n")
    
    print("=" * 80)
    print("✅ CDC INGESTION COMPLETE (MERGE + COLUMN FAMILIES)")
    print("=" * 80)
    print(f"📊 Raw events: {staging_count_raw}")
    print(f"📊 After deduplication: {staging_count} unique events")
    print(f"📊 Staging table: {staging_table_fqn}")
    print(f"📊 Target table:  {target_table_fqn}")
    print()
    print("💡 TIP: Staging table can be dropped after successful MERGE:")
    print(f"   spark.sql('DROP TABLE IF EXISTS {staging_table_fqn}')")
    
    return {
        "query": query,
        "staging_table": staging_table_fqn,
        "target_table": target_table_fqn,
        "raw_count": staging_count_raw,
        "deduped_count": staging_count,
        "merged": merged_count
    }
