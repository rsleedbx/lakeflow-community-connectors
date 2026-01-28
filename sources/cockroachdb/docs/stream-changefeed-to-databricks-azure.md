# Stream a Changefeed to Databricks (Azure Edition)

**For submission to:** CockroachDB Documentation  
**Author:** Lakeflow Community Connectors  
**Date:** 2026-01-28

---

## Overview

This guide demonstrates how to stream CockroachDB changefeeds to Databricks using Azure Blob Storage, with support for:

- **Append-only ingestion (SCD Type 2)**: Store all CDC events as historical records with full audit trail
- **UPDATE/DELETE support (SCD Type 1)**: Apply MERGE logic to maintain only current state
- **Column family handling**: Automatic merging of fragmented records when changefeed has `split_column_families` enabled

> 📘 **Slowly Changing Dimensions**: Our CDC modes align with standard data warehouse patterns:
> - **`append_only`** = **SCD Type 2**: Preserves all historical versions as separate rows
> - **`update_delete`** = **SCD Type 1**: Overwrites with latest values (no history retention)

> 💡 **Working Notebook Available**: `sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb` contains a complete, tested implementation with 4 CDC ingestion modes. This document focuses on architecture and key challenges.

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

**Pipeline Flow:**
1. **CockroachDB Changefeed**: Streams row-level changes to Azure as Parquet files
2. **Azure Blob Storage (ADLS Gen2)**: Intermediate staging with date-partitioned directories
3. **Unity Catalog External Location**: Secure, governed access to Azure storage
   - Centralized access control and auditing
   - Managed identities (no hardcoded credentials)
   - Data lineage and discovery
4. **Databricks Auto Loader**: Automatically discovers and ingests new files via Unity Catalog
5. **Delta Lake**: Target tables with ACID transactions and time travel

---

## Before You Begin

**Requirements:**
- CockroachDB Cloud account with `CHANGEFEED` privilege
- Azure Storage Account with **hierarchical namespace enabled** (ADLS Gen2)
- Databricks workspace (Standard/Premium tier for Auto Loader)
- Unity Catalog with External Location configured (recommended)

> ⚠️ **Critical**: Azure hierarchical namespace **cannot** be changed after account creation. See [Appendix](#appendix-azure-hierarchical-namespace-requirement) for details.

---

## Four CDC Ingestion Modes

The notebook implements 4 functions by combining 2 independent settings:

| CDC Mode | SCD Type | Column Family | Function | Use Case |
|----------|----------|---------------|----------|----------|
| `append_only` | Type 2 | `single_cf` | `ingest_cdc_append_only_single_family()` | Audit logs, simple tables |
| `append_only` | Type 2 | `multi_cf` | `ingest_cdc_append_only_multi_family()` | Audit logs, wide tables |
| `update_delete` | Type 1 | `single_cf` | `ingest_cdc_with_merge_single_family()` | Current state, simple tables |
| `update_delete` | Type 1 | `multi_cf` | `ingest_cdc_with_merge_multi_family()` | Current state, wide tables |

**Mode Selection (SCD Types):**
- **`append_only` (SCD Type 2)**: All CDC events (INSERT/UPDATE/DELETE) stored as rows
  - Higher storage cost
  - Full historical tracking
  - Perfect for audit logs, time-series analysis, compliance
  
- **`update_delete` (SCD Type 1)**: MERGE operations applied (overwrites)
  - Lower storage cost
  - Current state only (no history)
  - Perfect for production replication, dashboards, analytics

**Column Family Mode:**
- **`single_cf`**: Standard tables (1 column family) - simpler, better performance
- **`multi_cf`**: Multiple column families (50+ columns) - requires fragment merging

---

## Quick Start

### Step 1: Create CockroachDB Table

**Simple table (single column family):**
```sql
CREATE TABLE usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT
);
```

**Wide table with column families (for testing `split_column_families`):**
```sql
CREATE TABLE usertable_multi_cf (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT,
    FAMILY frequently_read (ycsb_key, field0, field1, field2),
    field3 TEXT,
    field4 TEXT,
    FAMILY medium_read (field3, field4),
    field5 TEXT,
    field6 TEXT,
    FAMILY rarely_read (field5, field6)
);
```

---

### Step 2: Create Changefeed

**For single column family tables:**
```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://changefeed-events/parquet/defaultdb/public/usertable/usertable_cdc/?AZURE_ACCOUNT_NAME={name}&AZURE_ACCOUNT_KEY={key}'
WITH 
    format='parquet',
    updated,
    resolved='10s',
    initial_scan='yes';
```

**For multi column family tables:**
```sql
CREATE CHANGEFEED FOR TABLE usertable_multi_cf
INTO 'azure://changefeed-events/parquet/defaultdb/public/usertable_multi_cf/usertable_multi_cf_cdc/?AZURE_ACCOUNT_NAME={name}&AZURE_ACCOUNT_KEY={key}'
WITH 
    format='parquet',
    updated,
    resolved='10s',
    split_column_families,  -- ← Critical for multi-family tables
    initial_scan='yes';
```

**Key Parameters:**
- `format='parquet'`: Columnar format for efficient analytics
- `updated`: Include update timestamps (required for deduplication)
- `resolved='10s'`: Emit watermarks every 10 seconds
- `split_column_families`: Generate separate files per column family
- `initial_scan='yes'`: Capture existing rows before streaming changes

**Path Structure:**
```
parquet/{database}/{schema}/{source_table}/{target_table}/
```
This enables multiple CDC pipelines from the same source table.

---

## Key Implementation Challenges

### Challenge 1: .RESOLVED Files Break Schema Inference

**Problem:**
CockroachDB generates `.RESOLVED` files containing watermark timestamps encoded as `DECIMAL(2147483647, 0)` (where `2147483647 = 2^31 - 1 = INT32_MAX`, the maximum precision allowed by Parquet's DECIMAL encoding). This exceeds Spark's maximum DECIMAL precision limit of 38.

**Solution:**
Use `pathGlobFilter` to exclude `.RESOLVED` files:

```python
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("pathGlobFilter", f"*{source_table}*.parquet")  # ← Exclude .RESOLVED
    .option("recursiveFileLookup", "true")
    .load(source_path)
)
```

Without this filter, Auto Loader fails with:
```
DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION: Decimal precision 2147483647 exceeds max precision 38
```

---

### Challenge 2: Column Family Fragments

**Problem:**
When a changefeed has `split_column_families` enabled, CockroachDB generates **multiple CDC events per row update**, one for each column family. Each fragment has:
- Primary key columns (always present)
- Data for ONE column family (other columns are `NULL`)
- Same `_cdc_timestamp` and `_cdc_operation`

**Example - 3 column families produce 3 files per update:**
```
202601281845...usertable+frequently_read-1.parquet  (PK + field0,1,2)
202601281845...usertable+medium_read-1.parquet      (PK + field3,4)
202601281845...usertable+rarely_read-1.parquet      (PK + field5,6)
```

**Solution:**
Merge fragments by grouping on `(PK + timestamp + operation)` and coalescing `NULL` values:

```python
def merge_column_family_fragments(df, primary_key_columns):
    group_by_cols = primary_key_columns + ['_cdc_timestamp', '_cdc_operation']
    
    # Use first(col, ignorenulls=True) to coalesce NULL values
    agg_exprs = [
        F.first(col, ignorenulls=True).alias(col) 
        for col in data_columns
    ]
    
    return df.groupBy(*group_by_cols).agg(*agg_exprs)
```

---

### Challenge 3: DELETE Operations in Initial Table Creation

**Problem:**
During initial table creation, if CDC events contain DELETE operations, naively filtering `_cdc_operation != "DELETE"` can leave orphaned rows when a key has both UPSERT and DELETE events.

**Example:**
```
Events: SNAPSHOT(key=5), DELETE(key=5)
Naive filter: Only keeps SNAPSHOT(key=5)  ← WRONG! Key was deleted.
```

**Solution:**
Use `left_anti` join to exclude all keys that were deleted (from `cockroachdb.py` reference):

```python
if not table_exists:
    # 1. Identify keys with DELETE events
    delete_keys = staging_df.filter(F.col("_cdc_operation") == "DELETE") \
        .select(*primary_keys) \
        .distinct()
    
    # 2. Get non-DELETE rows
    active_rows = staging_df.filter(F.col("_cdc_operation") != "DELETE")
    
    # 3. Exclude rows with deleted keys (left anti join)
    final_rows = active_rows.join(
        delete_keys,
        on=primary_keys,
        how="left_anti"  # ← Keep only rows NOT in delete_keys
    )
    
    final_rows.write.format("delta").saveAsTable(target_table)
```

This ensures deleted keys never appear in the target table.

---

### Challenge 4: Serverless Compatibility (Two-Stage Approach)

**Problem:**
Databricks Serverless doesn't support:
- Window functions in streaming mode
- Aggregations in `foreachBatch`
- Python UDFs in streaming queries

**Solution:**
Use a two-stage pipeline:

**Stage 1: Stream to Staging (No Aggregations)**
```python
# Stream raw events (pure Spark, Serverless-compatible)
query = (transformed_df.writeStream
    .format("delta")
    .trigger(availableNow=True)
    .toTable(f"{target_table}_staging")  # ← No window functions!
)
query.awaitTermination()
```

**Stage 2: Batch Processing (Full Spark SQL)**
```python
# Read staging in batch mode (window functions allowed!)
staging_df = spark.read.table(f"{target_table}_staging")

# Deduplicate using window functions
window_spec = Window.partitionBy(*primary_keys).orderBy(F.col("_cdc_timestamp").desc())
deduped_df = (staging_df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# Apply MERGE logic
delta_table.merge(deduped_df, join_condition) \
    .whenMatchedUpdate(...) \
    .whenMatchedDelete(...) \
    .whenNotMatchedInsert(...) \
    .execute()
```

This pattern works on **all Databricks editions** (Classic, Pro, Serverless).

---

## Databricks Implementation Patterns

### Pattern 1: Append-Only Ingestion (SCD Type 2)

**Use case**: Audit logs, time-series analysis, full history tracking, compliance

```python
# Configuration
source_path = f"abfss://{container}@{account}.dfs.core.windows.net/parquet/{db}/{schema}/{table}/{target}/"
target_table = f"{catalog}.{schema}.{table}_cdc"

# Read with Auto Loader
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("pathGlobFilter", f"*{source_table}*.parquet")
    .option("recursiveFileLookup", "true")
    .load(source_path)
)

# Transform CDC columns
df = raw_df.select(
    "*",
    F.from_unixtime(F.col("__crdb__updated").cast("double") / 1000000000).alias("_cdc_timestamp"),
    F.when(F.col("__crdb__event_type") == "d", "DELETE").otherwise("UPSERT").alias("_cdc_operation")
).drop("__crdb__updated", "__crdb__event_type")

# Write all events (no deduplication)
query = (df.writeStream
    .format("delta")
    .trigger(availableNow=True)
    .toTable(target_table)
)
```

**Result**: All CDC events stored as rows, including DELETE events marked as `_cdc_operation='DELETE'`.

---

### Pattern 2: UPDATE/DELETE Support (SCD Type 1)

**Use case**: Current state replication, production applications, dashboards

See Challenge 3 and Challenge 4 above for implementation details. The notebook provides full working code.

**Key differences from append-only:**
- Deduplication by primary key (keep latest timestamp)
- MERGE operations (UPDATE/DELETE/INSERT)
- Initial table creation with DELETE handling (`left_anti` join)

---

## SCD Type Comparison

Our CDC implementation follows standard Slowly Changing Dimension (SCD) patterns from data warehouse design:

| Aspect | `append_only` (SCD Type 2) | `update_delete` (SCD Type 1) |
|--------|---------------------------|------------------------------|
| **History** | ✅ Full history preserved | ❌ No history (overwrites) |
| **Storage** | Higher (all versions) | Lower (current state only) |
| **Query complexity** | Requires window functions for latest state | Direct query (one row per key) |
| **DELETE handling** | Stored as `_cdc_operation='DELETE'` row | Row physically removed from table |
| **UPDATE handling** | Creates new row (multiple rows per key) | Updates existing row in-place |
| **Use cases** | Audit logs, compliance, time-series | Production apps, dashboards, analytics |
| **Example** | User changes email 3 times → 3 rows | User changes email 3 times → 1 row (latest) |

### Implementation Verification

**`append_only` (SCD Type 2)** ✅ Confirmed:
```python
# All CDC events stored as rows
df.writeStream.toTable(target_table)  # No MERGE, no filtering

# Result for key=123:
# Row 1: key=123, email='v1@...', _cdc_operation='UPSERT', _cdc_timestamp='10:00'
# Row 2: key=123, email='v2@...', _cdc_operation='UPSERT', _cdc_timestamp='10:05'
# Row 3: key=123, email='v3@...', _cdc_operation='UPSERT', _cdc_timestamp='10:10'
```

**`update_delete` (SCD Type 1)** ✅ Confirmed:
```python
# MERGE logic overwrites
delta_table.merge(source, "target.pk = source.pk")
  .whenMatchedUpdate(...)  # ← Overwrites existing row
  .whenMatchedDelete(...)  # ← Removes row
  .whenNotMatchedInsert(...)
  .execute()

# Result for key=123:
# Row 1: key=123, email='v3@...', _cdc_operation='UPSERT', _cdc_timestamp='10:10'
# (only latest version, history discarded)
```

---

## File Organization Best Practices

**For multiple tables:**
```
changefeed-events/
├── parquet/
│   ├── ecommerce/
│   │   ├── public/
│   │   │   ├── orders/
│   │   │   │   └── orders_cdc/              ← Target table name
│   │   │   │       └── 2026-01-28/
│   │   │   │           └── *.parquet
│   │   │   ├── orders/
│   │   │   │   └── orders_archive/          ← Multiple destinations
│   │   │   │       └── 2026-01-28/
│   │   │   └── customers/
│   │   │       └── customers_cdc/
│   │   └── staging/
│   │       └── temp_orders/
│   │           └── temp_orders_cdc/
│   └── warehouse/
│       └── public/
│           └── inventory/
│               └── inventory_cdc/
```

**Benefits:**
- Prevents table name collisions
- Enables per-database access control
- Supports multiple CDC destinations from same source
- Simplifies Auto Loader path configuration

---

## Monitoring and Verification

### Verify Data Sync

**For `append_only` mode:**
- Max key should match between source and target
- Min key and count will differ (DELETE events are captured but not applied)

**For `update_delete` mode:**
- All statistics should match exactly (min, max, count)
- DELETE operations remove rows from target

**SQL verification:**
```sql
-- CockroachDB
SELECT MIN(ycsb_key), MAX(ycsb_key), COUNT(*) FROM usertable;

-- Databricks
SELECT MIN(ycsb_key), MAX(ycsb_key), COUNT(*) FROM main.default.usertable_cdc;
```

---

### Check Changefeed Status

```sql
-- View all changefeeds
SHOW CHANGEFEED JOBS;

-- View specific changefeed details
SHOW CHANGEFEED JOB {job_id};

-- Pause/Resume/Cancel
PAUSE JOB {job_id};
RESUME JOB {job_id};
CANCEL JOB {job_id};
```

---

## Troubleshooting Guide

### Issue 1: No Files in Azure

**Symptoms**: Auto Loader finds 0 files

**Possible causes:**
1. Changefeed not created or failed
2. Path configuration mismatch
3. Azure credentials incorrect
4. Changefeed batching delay (<1MB of data)

**Solution:**
```python
# Check Azure files manually (from notebook)
result = check_azure_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table
)
print(f"Data files: {len(result['data_files'])}")
```

---

### Issue 2: DECIMAL Precision Error

**Symptoms**:
```
DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION: Decimal precision 2147483647 exceeds max precision 38
```

**Cause**: Auto Loader trying to read `.RESOLVED` files

**Solution**: Add `pathGlobFilter` (see Challenge 1)

---

### Issue 3: DELETE Rows Stored as Data

**Symptoms**: Target table has rows with `_cdc_operation='DELETE'` and `NULL` data columns

**Cause**: Initial table creation didn't exclude deleted keys

**Solution**: Drop and recreate table with `left_anti` join logic (see Challenge 3)

---

### Issue 4: Missing Keys in Target

**Symptoms**: Keys exist in CockroachDB but not in Databricks

**Diagnosis:**
1. Check if keys exist in CockroachDB source
2. Check if CDC files exist in Azure
3. Check if keys exist in staging table (if using two-stage approach)

**Common causes:**
- Auto Loader hasn't picked up new files (re-run ingestion)
- MERGE logic filtering out rows incorrectly
- CDC events not yet generated (< 1MB batch threshold)

---

### Issue 5: Column Family Fragments Not Merged

**Symptoms**: Rows have `NULL` values for columns that should have data

**Cause**: Not merging column family fragments

**Solution**: Use `merge_column_family_fragments()` (see Challenge 2)

---

## Production Deployment Options

### Option A: Lakeflow Spark Declarative Pipelines (SDP)

**Best for:**
- High-volume CDC (>1000 ops/sec)
- Mission-critical pipelines
- TB-scale tables

**Benefits:**
- Auto-scaling compute
- Automatic recovery from failures
- Built-in monitoring and data quality
- Cost optimization (shuts down when idle)

**Note**: SDP (formerly Delta Live Tables) is recommended for production workloads.

---

### Option B: Scheduled Batch Processing

**Best for:**
- Lower change rates
- Acceptable latency (minutes to hours)
- Cost-sensitive deployments

**Implementation**: Use Databricks Jobs to run the notebook on a schedule (hourly/daily).

---

## Known Limitations

### Parquet Format

- **Cannot distinguish INSERT from UPDATE**: Both marked as `__crdb__event_type='c'`
- **Solution**: Use timestamp-based deduplication with MERGE logic

### Column Families

- **Requires `split_column_families`**: Must be explicitly enabled in changefeed
- **Multiple files per event**: Increases storage and processing overhead
- **Solution**: Use two-stage approach with fragment merging

### General

- **Batching delay**: CDC events batched until ~1MB before file write (1-2 min delay for low-volume tables)
- **Schema evolution**: Adding columns is automatic, dropping/renaming requires manual intervention
- **Date-based subdirectories**: CockroachDB creates `YYYY-MM-DD` directories

Refer to [Create and Configure Changefeeds](https://www.cockroachlabs.com/docs/stable/create-changefeed) for complete limitations.

---

## Appendix: Azure Hierarchical Namespace Requirement

### Why Required

For Databricks Unity Catalog External Locations and `abfss://` paths to function correctly, your Azure Storage Account **must have hierarchical namespace enabled** (Azure Data Lake Storage Gen2).

**Critical**: This setting **cannot** be changed after account creation.

### How to Enable

When creating a new Azure Storage Account:

1. Navigate to **Create a resource** → **Storage account**
2. Fill in **Basics** tab (subscription, resource group, storage account name, region)
3. Go to **Advanced** tab
4. Under **Data Lake Storage Gen2**, check: ☑ **Enable hierarchical namespace**
5. Complete remaining configuration and click **Create**

### Verification

**Via Azure Portal:**
1. Navigate to your storage account
2. Go to **Settings** → **Properties**
3. Look for **Hierarchical namespace**: Should show **Enabled**

**Via Azure CLI:**
```bash
az storage account show \
  --name <storage-account-name> \
  --resource-group <resource-group-name> \
  --query 'isHnsEnabled'
```

Expected output: `true`

### Non-Unity Catalog Users

If using `wasbs://` paths with access keys (not Unity Catalog):
- Hierarchical namespace is **not strictly required**
- However, **still recommended** for:
  - Better performance (faster directory operations)
  - Future compatibility (easier migration to Unity Catalog)
  - Consistency with Databricks best practices

---

## Next Steps

1. **Try the working notebook**: `sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`
2. **Learn more**:
   - [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
   - [Delta Lake MERGE](https://docs.databricks.com/delta/merge.html)
   - [CockroachDB Changefeed Best Practices](https://www.cockroachlabs.com/docs/stable/changefeed-best-practices)
   - [Unity Catalog External Locations](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html)

3. **Optimize for production**:
   - Run `OPTIMIZE table_name ZORDER BY (primary_key)`
   - Enable Delta time travel for historical queries
   - Set up monitoring and alerting
   - Configure Lakeflow SDP for auto-scaling

---

## About This Tutorial

**Maintained by:** Lakeflow Community Connectors  
**Source code:** [github.com/lakeflow/lakeflow-community-connectors](https://github.com/lakeflow/lakeflow-community-connectors)  
**License:** Apache 2.0  
**Last updated:** January 28, 2026

For questions or contributions, please visit our GitHub repository or join the CockroachDB Community Slack.
