# Stream a Changefeed to Databricks (Azure Edition)

**For submission to:** CockroachDB Documentation  
**Author:** Lakeflow Community Connectors  
**Date:** 2026-01-28

---

## Overview

This guide shows how to stream CockroachDB data to Databricks using changefeeds, Azure Blob Storage, and Delta Lake. The pipeline captures both initial snapshots and ongoing changes (inserts, updates, deletes) from CockroachDB and makes them available in Databricks for analytics and applications.

CockroachDB natively generates changefeed with snapshot and CDC records in Parquet format to Azure Blob Storage. Databricks Auto Loader reads the Parquet files into a streaming DataFrame, Spark transforms the CDC events, and Delta Lake writes them as streaming tables with ACID guarantees.

**What Databricks handles natively:**
- ✅ **SCD Type 1** (latest state with UPDATE/DELETE via MERGE INTO)
- ✅ **SCD Type 2** (full history with append-only ingestion)
- ✅ **Column families** (CockroachDB's `split_column_families` for wide tables with 50+ columns)
- ✅ **Transactional consistency** (atomic commits with resolved timestamps)
- ✅ **Schema evolution** (automatic schema inference and merging)
- ✅ **Multiple changefeed formats** (Parquet, JSON, Avro, CSV)

This guide focuses on **Parquet format** for optimal performance and native Delta Lake integration. For real-time data replication with change data capture (CDC), Databricks provides production-ready streaming pipelines without external dependencies or complex deduplication logic.

---

## Getting Started

### Step 1. Create a table *(CockroachDB)*

```sql
CREATE TABLE usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT
);
```

### Step 2. Create a changefeed *(CockroachDB → Azure)*

Stream changes to Azure Blob Storage as Parquet files:

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://changefeed-events/parquet/defaultdb/public/usertable/usertable_cdc/?AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key}'
WITH 
    format='parquet',
    updated,
    resolved='10s';
```

The changefeed will write Parquet files to Azure in this structure:
```
changefeed-events/parquet/defaultdb/public/usertable/usertable_cdc/
  ├── 2026-01-28/
  │   ├── 202601281910402079962990000000000-abc123.parquet  (data files)
  │   ├── 1738110640000000000.RESOLVED                       (watermark files)
  └── ...
```

### Step 3. Insert data *(CockroachDB)*

```sql
INSERT INTO usertable (ycsb_key, field0, field1, field2)
VALUES 
    (1, 'value_1_0', 'value_1_1', 'value_1_2'),
    (2, 'value_2_0', 'value_2_1', 'value_2_2');
```

The changefeed automatically captures these changes and writes them to Azure Blob Storage.

### Step 4. Stream into Databricks Delta Lake *(Databricks)*

Use Databricks Auto Loader to automatically ingest CDC files:

```python
from pyspark.sql import functions as F

# Read CDC events from Azure (Auto Loader automatically discovers new files)
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/checkpoints/usertable_cdc/schema")
    .option("pathGlobFilter", "*usertable*.parquet")  # Exclude .RESOLVED files
    .option("recursiveFileLookup", "true")
    .load("abfss://changefeed-events@{storage_account}.dfs.core.windows.net/parquet/defaultdb/public/usertable/usertable_cdc/")
)

# Transform CockroachDB CDC format to Delta Lake
df = raw_df.select(
    "*",
    F.from_unixtime(F.col("__crdb__updated").cast("double") / 1e9).cast("timestamp").alias("_cdc_timestamp"),
    F.when(F.col("__crdb__event_type") == "d", "DELETE").otherwise("UPSERT").alias("_cdc_operation")
).drop("__crdb__updated", "__crdb__event_type")

# Write to Delta Lake (append-only mode for full history)
query = (df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/usertable_cdc/data")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("main.default.usertable_cdc")
)

query.awaitTermination()
```

### Step 5. Query your data *(Databricks)*

```sql
SELECT ycsb_key, field0, _cdc_operation, _cdc_timestamp 
FROM main.default.usertable_cdc 
ORDER BY _cdc_timestamp;
```

```
+--------+-----------+--------------+-------------------+
|ycsb_key|field0     |_cdc_operation|_cdc_timestamp     |
+--------+-----------+--------------+-------------------+
|1       |value_1_0  |UPSERT        |2026-01-28 19:10:00|
|2       |value_2_0  |UPSERT        |2026-01-28 19:10:00|
+--------+-----------+--------------+-------------------+
```

**That's it!** You just implemented **append-only ingestion** (SCD Type 2) for a standard table.

### Want more CDC capabilities?

The complete working notebook (`sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`) includes all 4 CDC modes:

| CDC Mode | SCD Type | Column Family | Use Case |
|----------|----------|---------------|----------|
| `append_only` + `single_cf` | Type 2 | Standard | **← You just did this!** Audit logs, full history |
| `append_only` + `multi_cf` | Type 2 | Wide tables | 50+ columns with column families |
| `update_delete` + `single_cf` | Type 1 | Standard | Current state with MERGE INTO |
| `update_delete` + `multi_cf` | Type 1 | Wide tables | Current state + column families |

**Key differences:**
- **`append_only`**: Stores all CDC events as rows (INSERT/UPDATE/DELETE history)
- **`update_delete`**: Applies MERGE operations to maintain current state only
- **`single_cf`**: Standard tables (simpler ingestion)
- **`multi_cf`**: CockroachDB's `split_column_families` for wide tables (requires fragment merging)

---

## Architecture

```
                 ┌─────────────┐
                 │ Changefeed  │
                 └──────┬──────┘
                        ↓
CockroachDB ──→ Azure Blob Storage ──→ Databricks Auto Loader ──→ Delta Lake
                (ADLS Gen2)               ↑
                                          │
                                   ┌──────┴───────┐
                                   │Unity Catalog │
                                   │ (Security &  │
                                   │ Governance)  │
                                   └──────────────┘
```

---

## Beyond the Basics: Implementing UPDATE/DELETE Support

The example above stores all CDC events as rows (SCD Type 2). For production applications that need current state only (SCD Type 1), use Delta Lake's MERGE INTO operation:

```python
from delta.tables import DeltaTable

# Two-stage approach (Serverless-compatible)
# Stage 1: Stream to staging (append-only)
raw_df.writeStream.toTable(f"{target_table}_staging")

# Stage 2: Apply MERGE from staging to final table
staging_df = spark.read.table(f"{target_table}_staging")

# Deduplicate to latest per key
window_spec = Window.partitionBy("ycsb_key").orderBy(F.col("_cdc_timestamp").desc())
deduped_df = staging_df.withColumn("_row_num", F.row_number().over(window_spec)) \
    .filter(F.col("_row_num") == 1).drop("_row_num")

# Apply MERGE
delta_table = DeltaTable.forName(spark, target_table)
delta_table.merge(deduped_df, "target.ycsb_key = source.ycsb_key") \
    .whenMatchedUpdate(set={"*"}) \
    .whenMatchedDelete(condition="source._cdc_operation = 'DELETE'") \
    .whenNotMatchedInsert(values={"*"}) \
    .execute()
```

**Why two stages?** Databricks Serverless doesn't support window functions in streaming mode. This pattern works on all Databricks editions.

---

## Wide Tables with Column Families

For tables with 50+ columns and multiple column families:

**1. Enable `split_column_families` in changefeed:**
```sql
CREATE CHANGEFEED FOR TABLE usertable_wide
INTO 'azure://...'
WITH 
    format='parquet',
    updated,
    split_column_families,  -- ← This is key
    initial_scan='yes';
```

**2. Merge fragments in Databricks:**
```python
def merge_column_family_fragments(df, primary_keys):
    """Merge column family fragments into complete rows."""
    group_by_cols = primary_keys + ['_cdc_timestamp', '_cdc_operation']
    
    # Coalesce NULL values across fragments
    agg_exprs = [F.first(col, ignorenulls=True).alias(col) for col in data_columns]
    
    return df.groupBy(*group_by_cols).agg(*agg_exprs)

# Use in pipeline
df_merged = merge_column_family_fragments(raw_df, ["ycsb_key"])
df_merged.writeStream.toTable(target_table)
```

**Why needed?** With `split_column_families`, CockroachDB generates multiple files per update (one per column family). Merging reconstructs complete rows.

---

## Next Steps

**Complete working notebook**: All 4 CDC modes (append_only, update_delete, single/multi column families) with production-ready code are available in `sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`.

**Learn more:**
- [CockroachDB Changefeed Documentation](https://www.cockroachlabs.com/docs/stable/create-changefeed)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/)
- [Delta Lake MERGE INTO](https://docs.databricks.com/delta/merge.html)

---

**About this guide**: Maintained by Lakeflow Community Connectors | [GitHub](https://github.com/lakeflow/lakeflow-community-connectors) | Apache 2.0 License
