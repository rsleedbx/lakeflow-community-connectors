# Stream a Changefeed to Databricks (Azure Edition)

> 📘 **DOCUMENT PURPOSE**: This is a comprehensive, Azure-specific guide for production CDC deployments. For a simpler, notebook-based tutorial, see `sources/cockroachdb/learnings/STREAM_CHANGEFEED_TO_DATABRICKS.md` (which is synced with its `.ipynb` version).

**For submission to:** CockroachDB Documentation  
**Author:** Lakeflow Community Connectors  
**Date:** 2026-01-26

---

While CockroachDB is an excellent system of record, it also needs to coexist with other systems. For example, you might want to keep your data mirrored in data lakes, analytics engines, or machine learning pipelines.

This page demonstrates how to use a changefeed to stream row-level changes to Databricks, a unified analytics platform built on Apache Spark and Delta Lake.

> **Note:** This tutorial follows a progressive approach:
> 1. **Append-only ingestion**: All CDC events stored as historical records
> 2. **UPDATE/DELETE support**: Atomic MERGE operations for latest state
> 3. **Column family handling**: Automatic merging of fragmented records
> 
> This progression demonstrates how Databricks handles CDC at different complexity levels, from basic event logging to full transaction support with CockroachDB-specific optimizations.

---

## Before you begin

Before you begin, make sure you have:

* Admin access to a CockroachDB Cloud account
* Write access to an Azure Blob Storage account with **hierarchical namespace enabled** (Azure Data Lake Storage Gen2)  
  → See [Appendix: Azure Hierarchical Namespace Requirement](#appendix-azure-hierarchical-namespace-requirement) for setup details
* Read and write access to a Databricks workspace (Standard or Premium tier recommended; see [Community Edition Compatibility](#databricks-edition-compatibility-matrix))
* The `CHANGEFEED` privilege in order to create and manage changefeed jobs. Refer to [Required privileges](https://www.cockroachlabs.com/docs/stable/create-changefeed#required-privileges) for more details.

---

## Architecture Overview

This tutorial creates a streaming CDC pipeline with:

```
CockroachDB → Azure Blob Storage → Databricks Autoloader → Delta Lake
```

---

## Step 1. Create a CockroachDB cluster

If you have not done so already, [create a cluster](https://www.cockroachlabs.com/docs/cockroachcloud/create-your-cluster).

---

## Step 2. Connect to your cluster

Refer to [Connect to your cluster](https://www.cockroachlabs.com/docs/cockroachcloud/connect-to-your-cluster) for detailed instructions on how to:

1. Download and install CockroachDB and your cluster's CA certificate locally.
2. Generate the `cockroach sql` command that you will use to connect to the cluster from the command line as a SQL user with admin privileges.

---

## Step 3. Configure your cluster

1. In your terminal, enter the `cockroach sql` command and connection string from Step 2 to start the built-in SQL client.

2. Enable rangefeeds:
   ```sql
   SET CLUSTER SETTING kv.rangefeed.enabled = true;
   ```
   
   You should see output similar to:
   ```
   SET CLUSTER SETTING
   ```
   
   To verify it's enabled:
   ```sql
   SHOW CLUSTER SETTING kv.rangefeed.enabled;
   ```
   
   Expected output (note: `t` means `true`):
   ```
    kv.rangefeed.enabled
   ----------------------
    t
   (1 row)
   ```
   
   > **Note:** Rangefeeds are enabled by default on CockroachDB Standard and Basic clusters.

---

## Step 4. Create a database and schema

1. In the built-in SQL shell, create a database called `defaultdb` (or use your preferred name):
   ```sql
   CREATE DATABASE IF NOT EXISTS defaultdb;
   ```

2. Set it as the default:
   ```sql
   SET DATABASE = defaultdb;
   ```

3. Create a schema for organizing tables:
   ```sql
   CREATE SCHEMA IF NOT EXISTS public;
   ```

---

## Step 5. Create a table

Before you can start a changefeed, you need to create at least one table for the changefeed to target. The targeted table's rows are referred to as the "watched rows".

For this tutorial, we'll use a simplified YCSB `usertable` schema **without column families** (column families require additional handling with `split_column_families` option):

```sql
CREATE TABLE public.usertable (
    ycsb_key VARCHAR(255) PRIMARY KEY,
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
);

-- Insert some sample data
INSERT INTO public.usertable (ycsb_key, field0, field1, field2, field3, field4)
VALUES 
    ('user0001', 'value0', 'value1', 'value2', 'value3', 'value4'),
    ('user0002', 'value0', 'value1', 'value2', 'value3', 'value4'),
    ('user0003', 'value0', 'value1', 'value2', 'value3', 'value4');
```

---

## Step 6. Create an Azure Blob Storage container

Every change to a watched row is emitted as a record in a configurable format (JSON or Parquet). To configure Azure Blob Storage as the cloud storage sink:

1. Log in to the [Azure Portal](https://portal.azure.com).

2. Create a storage account or use an existing one.

> ⚠️ **IMPORTANT**: Your Azure Storage Account **must have hierarchical namespace enabled** (Azure Data Lake Storage Gen2). This cannot be changed after account creation. See [Appendix: Azure Hierarchical Namespace Requirement](#appendix-azure-hierarchical-namespace-requirement) for details.

3. Create a container for storing changefeed data (this tutorial uses `changefeed-events`).

4. Navigate to **Access keys** under **Security + networking** and copy:
   - Storage account name
   - One of the access keys (key1 or key2)

You will need the container name (step 3), storage account name, and access key (step 4) when creating your changefeed.

---

## Step 7. Create a changefeed

### Parquet Format (Recommended for Analytics)

Parquet format provides:
- Efficient columnar storage (smaller files, faster queries)
- Native support in Databricks and Spark
- Lower storage costs compared to JSON

Back in the built-in SQL shell, create a changefeed with Parquet format:

```sql
CREATE CHANGEFEED FOR TABLE defaultdb.public.usertable
INTO 'azure://changefeed-events/parquet/defaultdb/public/?AZURE_ACCOUNT_NAME={your-storage-account-name}&AZURE_ACCOUNT_KEY={your-storage-account-key}'
WITH 
    format = 'parquet',
    compression = 'gzip',
    updated,
    resolved = '10s',
    initial_scan = 'yes';
```

**Key parameters:**
- **Path structure:** `parquet/defaultdb/public/` organizes files by format, database, and schema
- **format = 'parquet':** Use columnar Parquet format
- **compression = 'gzip':** Compress files for efficient storage
- **updated:** Include update timestamps for each row (crucial for deduplication)
- **resolved = '10s':** Emit resolved timestamps every 10 seconds
- **initial_scan = 'yes':** Capture existing rows before streaming changes

You will receive the changefeed's job ID:

```
        job_id
+--------------------+
  912345678901234567
(1 row)
```

You can use this job ID to manage the changefeed if needed.

Refer to the [Cloud Storage Authentication](https://www.cockroachlabs.com/docs/stable/cloud-storage-authentication) page for more detail on authenticating to Azure and other cloud providers.

---

## Step 8. Verify data is streaming to Azure

1. In the built-in SQL shell, perform some operations on the `usertable` table:
   ```sql
   -- Insert a new user
   INSERT INTO public.usertable (ycsb_key, field0, field1, field2)
   VALUES ('user0004', 'new_value0', 'new_value1', 'new_value2');
   
   -- Update an existing user
   UPDATE public.usertable 
   SET field0 = 'updated_value0', field1 = 'updated_value1'
   WHERE ycsb_key = 'user0001';
   
   -- Delete a user
   DELETE FROM public.usertable WHERE ycsb_key = 'user0002';
   ```

2. Navigate to your Azure Blob Storage container in the Azure Portal.

3. You should see a directory structure with date-based subdirectories:
   ```
   changefeed-events/
   └── parquet/
       └── defaultdb/
           └── public/
               └── usertable/
                   └── 2026-01-26/          ← Date-based directory (YYYY-MM-DD)
                       ├── 202601261000000000000000000-1234567890-1-72-00000000-usertable-1.parquet
                       ├── 202601261015000000000000000-1234567890-1-73-00000000-usertable-1.parquet
                       └── 202601261030000000000000000-1234567890-1-74-00000000-usertable-1.parquet
   ```

**File Naming Pattern:**

```
{timestamp}-{jobid}-{node}-{topic}-{sequence}-{table}-{file_num}.parquet
└─────┬────┘ └──┬──┘ └─┬─┘ └─┬──┘ └───┬───┘ └──┬──┘ └────┬────┘
  Nanosec   Job ID  Node  Topic  Sequence Table   File seq
  timestamp                                        number
```

> **Note:** Files appear within seconds for initial scan data. CDC events may be batched and appear within 30-120 seconds depending on the volume of changes.

---

## Step 9. Ingest CDC Data (Append-Only)

We'll start with the simplest approach: append-only ingestion. All CDC events are stored as historical records without deduplication.

**What this provides:**
- All CDC events as immutable history
- Simple implementation
- No schema file needed
- Perfect for audit logs, time-series analysis, and learning

> 💡 **Next step:** After completing append-only ingestion, see [Step 11](#step-11-adding-updatedelete-support) to add UPDATE/DELETE support with MERGE logic.

### Create Databricks Notebook

1. Create a new Python notebook in Databricks

2. Configure Azure storage credentials:

```python
# Configure Azure storage access
storage_account_name = "your-storage-account-name"  # ← Replace
storage_account_key = "your-storage-account-key"      # ← Replace

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)
```

3. Add the append-only ingestion code:

```python
from pyspark.sql import functions as F

# ============================================================================
# CONFIGURATION
# ============================================================================
storage_account_name = "your-storage-account-name"  # ← Replace
container_name = "changefeed-events"                 # ← Replace
target_catalog = "main"                              # ← Replace
target_schema = "default"                            # ← Replace

source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/parquet/defaultdb/public/usertable/"
checkpoint_path = "/checkpoints/usertable/append_only"
target_table = f"{target_catalog}.{target_schema}.usertable_cdc_events"

# ============================================================================
# Read and append all CDC events
# ============================================================================
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("recursiveFileLookup", "true")
    .load(source_path)
    .select(
        "*",
        F.when(F.col("__crdb__event_type") == "d", "DELETE")
         .otherwise("UPSERT")
         .alias("_cdc_operation"),
        F.col("__crdb__updated").alias("_cdc_timestamp")
    )
)

# Write all CDC events (no deduplication)
query = (df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .trigger(availableNow=True)
    .toTable(target_table)
)

print("✅ Streaming query started")
print("⏳ Processing data...")

query.awaitTermination()

print("\n" + "="*80)
print("✅ APPEND-ONLY INGESTION COMPLETED")
print("="*80)
print(f"📊 Query your data: SELECT * FROM {target_table}")
```

---

## Step 10. Query CDC Events

### View All CDC Events

```sql
-- All CDC events in chronological order
SELECT * FROM main.default.usertable_cdc_events 
ORDER BY _cdc_timestamp DESC 
LIMIT 100;

-- Count events by operation type
SELECT _cdc_operation, COUNT(*) as event_count
FROM main.default.usertable_cdc_events
GROUP BY _cdc_operation;
```

### Get Latest State (Manual Deduplication)

```sql
-- Latest state per key (deduplication in query)
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY ycsb_key 
      ORDER BY _cdc_timestamp DESC
    ) AS rn
  FROM main.default.usertable_cdc_events
  WHERE _cdc_operation != 'DELETE'
)
WHERE rn = 1;
```

### Audit Trail for Specific Key

```sql
-- See all changes for a specific key
SELECT ycsb_key, _cdc_operation, _cdc_timestamp, field0, field1
FROM main.default.usertable_cdc_events
WHERE ycsb_key = 'user0001'
ORDER BY _cdc_timestamp;
```

**That's it!** You now have a working CDC pipeline.

---

## Step 11. Adding UPDATE/DELETE Support

The append-only approach works great for audit logs and time-series data. For applications that need the latest state with automatic UPDATE/DELETE handling, you'll need:

1. **Schema file with primary keys** (for MERGE join conditions)
2. **MERGE logic** (to apply UPDATE/DELETE operations)
3. **Deduplication** (to handle CockroachDB column family fragmentation)

> 📚 **Reference:** See [Appendix: Full CDC with UPDATE/DELETE](#appendix-full-cdc-with-updatedelete-support) for complete implementation.

**When to use full CDC:**
- Applications requiring latest state (not history)
- Need to handle UPDATE operations as updates (not new rows)
- Need to remove rows on DELETE operations
- Want Databricks to handle deduplication automatically

---

## Step 12. Monitor Changefeed

### Check Changefeed Status

```sql
-- View changefeed status in CockroachDB
SHOW CHANGEFEED JOBS;
```

Look for the status column:
- `running`: Changefeed is active
- `paused`: Changefeed is paused
- `failed`: Changefeed encountered an error

### View Changefeed Details

```sql
-- Get details for your changefeed
SHOW CHANGEFEED JOB {job_id};
```

### Monitor Databricks Streaming Query

In your Databricks notebook, check the query status:

```python
# Get query status
query.status

# View recent progress
query.recentProgress

# Check for errors
query.exception()
```

**Your CDC pipeline is now complete!**

---

## Next Steps

- **Add full CDC support**: See [Appendix: Full CDC with UPDATE/DELETE](#appendix-full-cdc-with-updatedelete-support)
- **Scale to production**: See [Step 13: Production Deployment Options](#step-13-production-deployment-options)
- **Optimize performance**: Run `OPTIMIZE table_name ZORDER BY (primary_key)`
- **Enable time travel**: Query historical data with `SELECT * FROM table_name TIMESTAMP AS OF '2026-01-26'`

---

## Step 13. Production Deployment Options

The notebook approach shown in Step 9 works well for development and testing. For production deployments, consider these options:

### Lakeflow Spark Declarative Pipelines (SDP)

For production workloads, Lakeflow SDP (formerly DLT) provides:
- Auto-scaling compute
- Automatic recovery from failures
- Built-in monitoring and data quality
- Cost optimization (shuts down when idle)

**When to use:**
- High-volume CDC streams (>1000 ops/sec)
- Mission-critical pipelines
- Need for data quality guarantees

> 📚 **Reference:** See [Appendix: Production Patterns](#appendix-production-patterns) for SDP implementation examples.

### Scheduled Batch Processing

For lower-volume workloads, use Databricks Jobs to run the notebook on a schedule:
- Hourly/daily processing
- Lower cost (no always-on compute)
- Simpler operational model

**When to use:**
- Lower change rates
- Acceptable latency (minutes to hours)
- Cost-sensitive deployments

---

##

> **Note**: Lakeflow Spark Declarative Pipelines (SDP) was previously known as Delta Live Tables (DLT). The functionality is identical; only the name has changed as part of Databricks' evolution toward the broader Lakeflow platform.

**Benefits:**
- **Auto-scaling compute**: Automatically scales clusters based on data volume
- **Backpressure handling**: Manages bursts of CDC events without overwhelm
- **Automatic recovery**: Restarts failed tasks and handles transient errors
- **Data quality enforcement**: Built-in expectations for validation
- **Cost optimization**: Shuts down compute when no new data arrives
- **Simplified monitoring**: Unified pipeline observability dashboard

**When to use:**
- Production deployments with TB-scale tables
- High change rates (>1000 ops/sec sustained)
- Multiple tables requiring consistent processing
- Need for data quality guarantees

### Create SDP Pipeline

1. In Databricks, go to **Workflows** → **Delta Live Tables** → **Create Pipeline**

2. Configure the pipeline:
   - **Name**: `cockroachdb_usertable_cdc`
   - **Product Edition**: Core or Pro (based on your needs)
   - **Pipeline Mode**: Triggered or Continuous
   - **Storage Location**: Unity Catalog path (e.g., `main.default.usertable`)

3. Create a notebook with the following SQL:

```sql
-- Read streaming data from Azure Blob Storage
CREATE OR REFRESH STREAMING MATERIALIZED VIEW usertable_raw
AS SELECT 
  *,
  CASE 
    WHEN __crdb__event_type = 'd' THEN 'DELETE'
    ELSE 'UPSERT'
  END AS _cdc_operation,
  __crdb__updated AS _cdc_updated
FROM cloud_files(
  "wasbs://changefeed-events@{your-storage-account}.blob.core.windows.net/parquet/defaultdb/public/usertable/",
  "parquet",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaLocation", "/checkpoints/usertable/schema"
  )
);

-- Deduplicate and apply CDC operations
CREATE OR REFRESH MATERIALIZED VIEW usertable_deduped
AS SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY ycsb_key 
      ORDER BY _cdc_updated DESC
    ) AS row_num
  FROM usertable_raw
)
WHERE row_num = 1 
  AND _cdc_operation != 'DELETE';

-- Final target table with MERGE logic
APPLY CHANGES INTO usertable
FROM usertable_deduped
KEYS (ycsb_key)
SEQUENCE BY _cdc_updated
COLUMNS * EXCEPT (_cdc_operation, _cdc_updated, __crdb__event_type, __crdb__updated, row_num)
STORED AS SCD TYPE 1;
```

4. Attach the notebook to the pipeline and click **Start**

---

## Option B: Structured Streaming (Python Notebook)

**Benefits:**
- **Custom business logic**: Full control over transformation logic
- **Development flexibility**: Easier to prototype and iterate
- **Lower cost**: Can run on smaller clusters or job clusters
- **Existing compute**: Use interactive clusters for development

**When to use:**
- Development and testing environments
- Custom transformation requirements
- Tables with medium change rates (<1000 ops/sec)
- Learning and experimentation

### Create Databricks Notebook

1. Create a new Python notebook in Databricks

2. Configure Azure storage credentials:
   ```python
   # Configure Azure storage access (if not using Unity Catalog)
   storage_account_name = "your-storage-account-name"  # ← Replace
   storage_account_key = "your-storage-account-key"      # ← Replace
   
   spark.conf.set(
       f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
       storage_account_key
   )
   ```

3. Add the complete CDC ingestion pipeline:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================================
# CONFIGURATION
# ============================================================================
# Azure Blob Storage configuration
storage_account_name = "your-storage-account-name"  # ← Azure Blob Storage Account Name
container_name = "changefeed-events"                 # ← Azure Blob Storage Container
virtual_directory_path = "parquet/defaultdb/public/usertable/"  # ← Virtual directory path

# Databricks configuration
target_catalog = "main"                              # ← Replace
target_schema = "default"                            # ← Replace

source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{virtual_directory_path}"
checkpoint_path = "/checkpoints/usertable/parquet"
target_table = f"{target_catalog}.{target_schema}.usertable"

# ============================================================================
# STEP 1: Read streaming data with Autoloader
# ============================================================================
raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("recursiveFileLookup", "true")  # Read date subdirectories
    .load(source_path)
)
print("✅ Autoloader configured")

# ============================================================================
# STEP 2: Process CDC events
# ============================================================================
print("\n🔄 Step 2: Processing CDC events...")
processed_stream = raw_stream.select(
    "*",
    F.when(F.col("__crdb__event_type") == "d", "DELETE")
     .otherwise("UPSERT")
     .alias("_cdc_operation"),
    F.col("__crdb__updated").alias("_cdc_updated")
)

# Deduplicate by primary key (handles split_column_families if enabled)
window = Window.partitionBy("ycsb_key").orderBy(F.col("_cdc_updated").desc())
deduped_stream = processed_stream \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \
    .filter(F.col("_cdc_operation") != "DELETE") \
    .drop("row_num", "__crdb__event_type", "__crdb__updated")
print("✅ CDC processing configured")

# ============================================================================
# STEP 3: Define merge logic
# ============================================================================
print("\n🔄 Step 3: Defining merge logic...")

def merge_to_delta(micro_batch_df, epoch_id):
    """Merge CDC events into Delta table with upsert/delete logic."""
    
    # Create table if it doesn't exist
    if not spark.catalog.tableExists(target_table):
        micro_batch_df.write.format("delta").saveAsTable(target_table)
        print(f"  ✅ Created new table: {target_table}")
        return
    
    # Merge into existing table
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.ycsb_key = source.ycsb_key"
    ).whenMatchedUpdate(
        condition="source._cdc_operation = 'UPSERT' AND source._cdc_updated > target._cdc_updated",
        set={
            "field0": "source.field0",
            "field1": "source.field1",
            "field2": "source.field2",
            "field3": "source.field3",
            "field4": "source.field4",
            "field5": "source.field5",
            "field6": "source.field6",
            "field7": "source.field7",
            "field8": "source.field8",
            "field9": "source.field9",
            "_cdc_updated": "source._cdc_updated"
        }
    ).whenMatchedDelete(
        condition="source._cdc_operation = 'DELETE'"
    ).whenNotMatchedInsert(
        condition="source._cdc_operation = 'UPSERT'",
        values={
            "ycsb_key": "source.ycsb_key",
            "field0": "source.field0",
            "field1": "source.field1",
            "field2": "source.field2",
            "field3": "source.field3",
            "field4": "source.field4",
            "field5": "source.field5",
            "field6": "source.field6",
            "field7": "source.field7",
            "field8": "source.field8",
            "field9": "source.field9",
            "_cdc_updated": "source._cdc_updated"
        }
    ).execute()
    print(f"  ✅ Merged batch {epoch_id}")

print("✅ Merge function defined")

# ============================================================================
# STEP 4: Start streaming
# ============================================================================
print("\n🔄 Step 4: Starting streaming query...")
query = (deduped_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .foreachBatch(merge_to_delta)
    .trigger(availableNow=True)  # Process all available data then stop
    .start()
)

print("✅ Streaming query started")
print("⏳ Processing data...")

# Wait for completion
query.awaitTermination()

print("\n" + "="*80)
print("✅ STREAMING QUERY COMPLETED SUCCESSFULLY")
print("="*80)
print(f"📊 Query your data: SELECT * FROM {target_table}")
```

---

## Recommendation for TB-Scale Tables with High Change Rates

**For TB-sized tables with high change rates (>1000 ops/sec sustained), we strongly recommend Option A (SDP).**

**Why:**

1. **Auto-scaling compute**: SDP automatically provisions and scales clusters based on incoming data volume. During peak hours, it scales up; during quiet periods, it scales down or shuts off entirely.

2. **Backpressure handling**: With high change rates, CDC events can arrive faster than processing. SDP intelligently throttles ingestion to prevent memory pressure and ensures stable processing.

3. **Automatic recovery**: Hardware failures, transient network issues, and cloud provider interruptions are handled automatically. SDP restarts failed tasks from checkpoints without data loss.

4. **Cost optimization**: SDP's intelligent resource management means you only pay for compute when actively processing data. Notebooks require manually stopping clusters.

5. **Data quality**: Built-in expectations ensure data integrity at scale. For TB-scale operations, this prevents downstream issues.

**Real-world example:**
- **Table size**: 5 TB
- **Change rate**: 5,000 ops/sec peak, 500 ops/sec average
- **SDP advantage**: Auto-scales from 2 to 20 nodes during peak, back to 2 during off-hours
- **Cost savings**: ~60% compared to static notebook cluster sized for peak

---

## Databricks Edition Compatibility Matrix

| Feature | Community Edition | Standard/Premium |
|---------|-------------------|------------------|
| Autoloader (`cloudFiles`) | ❌ Not supported | ✅ Supported |
| Unity Catalog | ❌ Not supported | ✅ Supported |
| SDP (formerly DLT) | ❌ Not supported | ✅ Supported |
| Streaming from Azure | ❌ Limited | ✅ Full support |
| DBFS paths | ✅ Supported | ✅ Supported |
| Delta Lake MERGE | ✅ Supported | ✅ Supported |

**Verdict**: For production CDC pipelines, **Standard or Premium tier is required**. Community Edition can be used for learning basic concepts with manual batch loads (see Appendix below).

---

## Organizing multiple databases and schemas

For production deployments with multiple databases, schemas, and tables, use the hierarchical path structure:

```
changefeed-events/
├── parquet/
│   ├── ecommerce/
│   │   ├── public/
│   │   │   ├── orders/
│   │   │   │   └── 2026-01-26/
│   │   │   │       └── *.parquet
│   │   │   ├── customers/
│   │   │   │   └── 2026-01-26/
│   │   │   └── products/
│   │   │       └── 2026-01-26/
│   │   └── staging/
│   │       └── temp_orders/
│   └── warehouse/
│       └── public/
│           └── inventory/
│               └── 2026-01-26/
└── json/
    └── ecommerce/
        └── public/
            └── orders/
                └── 2026-01-26/
```

Create separate changefeeds for each table:

```sql
-- Ecommerce orders (Parquet)
CREATE CHANGEFEED FOR TABLE ecommerce.public.orders
INTO 'azure://changefeed-events/parquet/ecommerce/public/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH format = 'parquet', compression = 'gzip', updated, resolved = '10s', initial_scan = 'yes';

-- Warehouse inventory (Parquet)
CREATE CHANGEFEED FOR TABLE warehouse.public.inventory
INTO 'azure://changefeed-events/parquet/warehouse/public/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH format = 'parquet', compression = 'gzip', updated, resolved = '10s', initial_scan = 'yes';
```

This organization:
- Prevents table name collisions across databases/schemas
- Enables per-database access control
- Simplifies Autoloader configuration (one path per table)
- Supports easy format switching (JSON vs Parquet)
- Organizes files by date for efficient pruning

---

## Monitoring and troubleshooting

### Check changefeed status

```sql
SHOW CHANGEFEED JOBS;
```

Look for the status column:
- `running`: Changefeed is active
- `paused`: Changefeed is paused
- `failed`: Changefeed encountered an error

### View changefeed details

```sql
SHOW CHANGEFEED JOB {job_id};
```

### Monitor Databricks streaming query

In your Databricks notebook, after starting the streaming query:

```python
# Get query status
query.status

# View recent progress
query.recentProgress

# Check for errors
query.exception()
```

### Debug missing data

1. **Verify files in Azure Storage:** Check that Parquet/JSON files are being created in the expected path with date subdirectories.

2. **Check Autoloader schema location:** Ensure the schema location is accessible and not corrupted.

3. **Review checkpoint location:** If reprocessing is needed, delete the checkpoint directory (data will be reprocessed from the beginning).

4. **Validate merge conditions:** Ensure primary key joins in the MERGE statement match your table schema.

5. **Check _metadata/schema.json:** Verify the schema file exists and has the correct primary key definition.

---

## Known limitations

### Parquet format

- **Cannot distinguish INSERT from UPDATE:** Both are marked with `__crdb__event_type = 'c'`. Use MERGE logic with timestamp-based deduplication.
- **Requires split_column_families:** For tables with multiple column families, you must add `split_column_families` to the changefeed options.

### JSON format

- **Larger file sizes:** JSON files are typically 3-5x larger than compressed Parquet.
- **Slower queries:** JSON is less efficient for analytical queries compared to columnar Parquet.

### General considerations

- **Batching delay:** CDC events are batched until approximately 1MB of data is collected before files are written. For low-volume tables, this may cause a 1-2 minute delay.
- **Resolved timestamps:** Use `resolved` option to emit periodic resolved timestamps for exactly-once processing guarantees.
- **Schema evolution:** Adding columns is automatic, but dropping or renaming columns requires manual intervention.
- **Date-based subdirectories:** CockroachDB creates date-based subdirectories (YYYY-MM-DD) for each day's data.

Refer to the [Create and Configure Changefeeds](https://www.cockroachlabs.com/docs/stable/create-changefeed) page for more general changefeed limitations.

---

## Next steps

- Learn more about [Databricks Autoloader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- Explore [Delta Lake MERGE](https://docs.databricks.com/delta/merge.html) capabilities
- Read about [CockroachDB changefeed best practices](https://www.cockroachlabs.com/docs/stable/changefeed-best-practices)
- Set up [Unity Catalog External Locations](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html) for production deployments
- Review [Lakeflow Spark Declarative Pipelines documentation](https://docs.databricks.com/workflows/delta-live-tables/index.html)

---

## Appendix: Full CDC with UPDATE/DELETE Support

This appendix shows how to extend the append-only approach (Steps 9-10) to support UPDATE and DELETE operations with automatic deduplication.

### What You Add

Compared to append-only mode, full CDC requires:

1. **Schema file with primary keys** - Defines join conditions for MERGE
2. **MERGE logic** - Applies UPDATE/DELETE/INSERT operations
3. **Deduplication** - Handles CockroachDB column family fragmentation

### Step 1: Create Schema File

```python
import json
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage credentials
storage_account_name = "your-storage-account-name"
storage_account_key = "your-storage-account-key"
container_name = "changefeed-events"

# Schema definition with primary key
schema_data = {
    "table_schema": {
        "columns": [
            {"name": "ycsb_key", "type": "string"},
            {"name": "field0", "type": "string"},
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "string"},
            {"name": "field3", "type": "string"},
            {"name": "field4", "type": "string"},
            {"name": "field5", "type": "string"},
            {"name": "field6", "type": "string"},
            {"name": "field7", "type": "string"},
            {"name": "field8", "type": "string"},
            {"name": "field9", "type": "string"}
        ],
        "primary_key": ["ycsb_key"]  # ← REQUIRED for MERGE
    }
}

# Upload to Azure
blob_name = "parquet/defaultdb/public/usertable/_metadata/schema.json"
blob_service_client = BlobServiceClient(
    account_url=f"https://{storage_account_name}.blob.core.windows.net",
    credential=storage_account_key
)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_client.upload_blob(json.dumps(schema_data, indent=2), overwrite=True)

print(f"✅ Schema file uploaded to: {blob_name}")
```

### Step 2: Implement MERGE Logic

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Configuration (same as append-only)
storage_account_name = "your-storage-account-name"
container_name = "changefeed-events"
target_catalog = "main"
target_schema = "default"

source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/parquet/defaultdb/public/usertable/"
checkpoint_path = "/checkpoints/usertable/full_cdc"
target_table = f"{target_catalog}.{target_schema}.usertable"

# Read CDC events
raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("recursiveFileLookup", "true")
    .load(source_path)
)

# Process CDC events
processed_stream = raw_stream.select(
    "*",
    F.when(F.col("__crdb__event_type") == "d", "DELETE")
     .otherwise("UPSERT")
     .alias("_cdc_operation"),
    F.col("__crdb__updated").alias("_cdc_updated")
)

# Deduplicate by primary key
window = Window.partitionBy("ycsb_key").orderBy(F.col("_cdc_updated").desc())
deduped_stream = processed_stream \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \
    .filter(F.col("_cdc_operation") != "DELETE") \
    .drop("row_num", "__crdb__event_type", "__crdb__updated")

# MERGE function
def merge_to_delta(micro_batch_df, epoch_id):
    if not spark.catalog.tableExists(target_table):
        micro_batch_df.write.format("delta").saveAsTable(target_table)
        return
    
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.ycsb_key = source.ycsb_key"
    ).whenMatchedUpdate(
        condition="source._cdc_operation = 'UPSERT' AND source._cdc_updated > target._cdc_updated",
        set={
            "field0": "source.field0",
            "field1": "source.field1",
            "field2": "source.field2",
            "field3": "source.field3",
            "field4": "source.field4",
            "field5": "source.field5",
            "field6": "source.field6",
            "field7": "source.field7",
            "field8": "source.field8",
            "field9": "source.field9",
            "_cdc_updated": "source._cdc_updated"
        }
    ).whenMatchedDelete(
        condition="source._cdc_operation = 'DELETE'"
    ).whenNotMatchedInsert(
        condition="source._cdc_operation = 'UPSERT'",
        values={
            "ycsb_key": "source.ycsb_key",
            "field0": "source.field0",
            "field1": "source.field1",
            "field2": "source.field2",
            "field3": "source.field3",
            "field4": "source.field4",
            "field5": "source.field5",
            "field6": "source.field6",
            "field7": "source.field7",
            "field8": "source.field8",
            "field9": "source.field9",
            "_cdc_updated": "source._cdc_updated"
        }
    ).execute()

# Start streaming with MERGE
query = (deduped_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .foreachBatch(merge_to_delta)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
```

### Comparison: Append-Only vs. Full CDC

| Aspect | Append-Only (Steps 9-10) | Full CDC (This Appendix) |
|--------|--------------------------|--------------------------|
| Schema file | Not needed | Required |
| UPDATE handling | Stored as new row | Updates existing row |
| DELETE handling | Stored as event | Removes row |
| Storage | Higher (all events) | Lower (latest only) |
| Query complexity | Higher (window functions) | Lower (current state) |
| Use cases | Audit, time-series, debug | Applications, analytics |

---

## Appendix: CDC Architecture Comparison

This section compares operational steps for CDC ingestion in traditional multi-stage architectures versus Databricks' single-stage approach.

### Multi-Stage Architecture (Traditional)

**Initial Setup:**
1. Create raw staging table (append-only, stores all CDC events)
2. Create target table (deduplicated, final data)
3. Configure ingestion pipeline (e.g., Snowpipe + SQS)
4. Create stream or change tracking on staging table
5. Create scheduled task/job for deduplication
6. Configure task dependencies and error handling

**Per-Batch Operations:**
```
Step 1: Ingest CDC files        → Append to raw_table (contains duplicates)
                                   Duration: ~1-2 minutes

Step 2: Wait for task schedule  → Cron trigger (e.g., every 5 minutes)
                                   Duration: 0-5 minutes

Step 3: Process stream          → Query: SELECT * FROM stream
                                   Duration: ~30 seconds

Step 4: Deduplicate             → Window function: ROW_NUMBER() OVER 
                                      (PARTITION BY id ORDER BY ts DESC)
                                   Duration: ~1-3 minutes

Step 5: Apply CDC logic         → MERGE INTO target_table
                                      USING deduplicated_data
                                      WHEN MATCHED AND op='u' THEN UPDATE
                                      WHEN MATCHED AND op='d' THEN DELETE
                                      WHEN NOT MATCHED THEN INSERT
                                   Duration: ~2-5 minutes

Step 6: Acknowledge stream      → Mark processed records
                                   Duration: ~10 seconds

Total latency: 4-16 minutes
Tables maintained: 2 (raw_table + target_table)
Manual steps: 6
```

### Single-Stage Architecture (Databricks)

**Initial Setup:**
1. Create target table (auto-created on first write)
2. Configure Autoloader path

**Per-Batch Operations:**
```
Step 1: Read CDC files          → Autoloader detects new files
                                   Duration: <1 second

Step 2: Apply CDC + Deduplicate → Delta Lake MERGE (atomic operation)
                                      delta_table.merge(source, "target.id = source.id")
                                        .whenMatchedUpdate(
                                            condition="source.ts > target.ts"
                                        )
                                        .whenMatchedDelete(
                                            condition="source.op = 'DELETE'"
                                        )
                                        .whenNotMatchedInsert()
                                        .execute()
                                   Duration: ~5-30 seconds

Total latency: 5-30 seconds
Tables maintained: 1 (target_table)
Manual steps: 0 (fully automated)
```

### Operational Differences

| Aspect | Multi-Stage | Single-Stage (Databricks) |
|--------|-------------|---------------------------|
| Deduplication | Separate window query | Built into MERGE condition |
| CDC operations | Multi-step MERGE logic | Single atomic MERGE |
| Conflict resolution | Custom logic in task | `source.ts > target.ts` condition |
| Table count | 2 (raw + target) | 1 (target only) |
| Orchestration | Scheduled task/cron | Autoloader triggers |
| Partial data visibility | Yes (in raw table) | No (atomic commits) |
| Latency | Minutes | Seconds |

### Why Single-Stage is More Efficient

1. **Fewer I/O operations**: Reads source files once and writes final data once
2. **No intermediate staging**: Eliminates raw table read/write overhead
3. **Atomic operations**: MERGE combines deduplication + CDC logic in one transaction
4. **Trigger-based**: Processes immediately when files arrive (no cron delay)
5. **Reduced complexity**: Fewer moving parts = fewer failure modes

---

## Appendix: Azure Hierarchical Namespace Requirement

### Why It's Required

For Databricks Unity Catalog External Locations and `abfss://` (Azure Blob Filesystem) paths to function correctly, your Azure Storage Account **must have hierarchical namespace enabled**. This is a feature of Azure Data Lake Storage Gen2.

**Why it's needed:**
- **Unity Catalog External Locations**: Rely on it for managed identities and granular access control
- **`abfss://` paths**: Used by Databricks for ADLS Gen2, require directory-like semantics
- **Performance**: Better directory operations and rename performance
- **Future compatibility**: Required for most modern Databricks features

### How to Enable

**When creating a new Azure Storage Account:**

1. In the Azure Portal, navigate to **Create a resource** → **Storage account**
2. Fill in the **Basics** tab (subscription, resource group, storage account name, region)
3. Go to the **Advanced** tab
4. Under **Data Lake Storage Gen2**, check the box: ☑ **Enable hierarchical namespace**
5. Complete the remaining configuration and click **Create**

**Visual reference:**

```
Advanced Tab:
┌─────────────────────────────────────────────────────┐
│ Security                                             │
│ ☐ Enable infrastructure encryption                  │
│                                                      │
│ Data Lake Storage Gen2                              │
│ ☑ Enable hierarchical namespace  ← Check this box  │
│                                                      │
│ Blob storage                                         │
│ ○ Hot  ● Cool  ○ Archive                           │
└─────────────────────────────────────────────────────┘
```

### Critical Limitations

⚠️ **This setting CANNOT be changed after the storage account is created.**

If your existing storage account does not have hierarchical namespace enabled:
- You **must create a new storage account** with this feature enabled
- You cannot "upgrade" an existing account
- Plan accordingly before migrating production data

### Non-Unity Catalog Users

If you are **only** using `wasbs://` paths with access keys (not Unity Catalog):
- Hierarchical namespace is **not strictly required**
- However, it is **still recommended** for:
  - Better performance (faster directory operations)
  - Future compatibility (if you migrate to Unity Catalog later)
  - Consistency with Databricks best practices

### Verification

To verify if your storage account has hierarchical namespace enabled:

1. Navigate to your storage account in the Azure Portal
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

### References

- [Create an Azure Data Lake Storage Gen2 account](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)
- [Introduction to Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Databricks: Azure Data Lake Storage Gen2](https://docs.databricks.com/external-data/azure-storage.html)

---

## Appendix: Community Edition Workarounds

While Databricks Community Edition has significant limitations (no Autoloader, no Unity Catalog, no SDP), you can still learn CDC concepts using manual batch processing.

### Option 1: Streaming from Azure (No Autoloader)

**Limitations:**
- No automatic schema inference
- No incremental file discovery
- Manual checkpoint management
- Requires hardcoded schema

```python
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

# Define schema manually (no inference)
schema = StructType([
    StructField("ycsb_key", StringType(), False),
    StructField("field0", StringType(), True),
    StructField("field1", StringType(), True),
    StructField("field2", StringType(), True),
    StructField("field3", StringType(), True),
    StructField("field4", StringType(), True),
    StructField("field5", StringType(), True),
    StructField("field6", StringType(), True),
    StructField("field7", StringType(), True),
    StructField("field8", StringType(), True),
    StructField("field9", StringType(), True),
    StructField("__crdb__updated", StringType(), True),
    StructField("__crdb__event_type", StringType(), True)
])

# Azure Blob Storage configuration
storage_account_name = "your-storage-account-name"  # ← Azure Blob Storage Account Name
storage_account_key = "your-storage-account-key"     # ← Azure Blob Storage Account Key
container_name = "changefeed-events"                 # ← Azure Blob Storage Container
virtual_directory_path = "parquet/defaultdb/public/usertable/"  # ← Virtual directory path

# Configure Azure access
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# Read with manual schema
source_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{virtual_directory_path}"
df = spark.read.schema(schema).parquet(source_path)

# Process CDC
df_processed = df.select(
    "*",
    F.when(F.col("__crdb__event_type") == "d", "DELETE").otherwise("UPSERT").alias("_cdc_operation")
).filter(F.col("_cdc_operation") != "DELETE")

# Write to DBFS Delta table
df_processed.write.format("delta").mode("overwrite").save("/dbfs/usertable")
```

### Option 2: Manual Batch Load (DBFS)

**Workflow:**
1. Download Parquet files from Azure to local machine
2. Upload to DBFS using Databricks UI (Data → Upload File)
3. Process manually in notebook

```python
# Read from DBFS
df = spark.read.parquet("dbfs:/FileStore/usertable/*.parquet")

# Process and write
df.write.format("delta").mode("overwrite").save("dbfs:/delta/usertable")

# Query
spark.read.format("delta").load("dbfs:/delta/usertable").show()
```

### Test `cloudFiles` with DBFS in Community Edition

**Test commands:**

```python
# Test 1: Check if cloudFiles is available
try:
    test_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load("dbfs:/test/")
    print("✅ cloudFiles is available")
except Exception as e:
    print(f"❌ cloudFiles error: {e}")

# Test 2: Try reading from DBFS with cloudFiles
try:
    stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", "dbfs:/checkpoints/schema") \
        .load("dbfs:/FileStore/usertable/")
    print("✅ cloudFiles works with DBFS")
except Exception as e:
    print(f"❌ cloudFiles + DBFS error: {e}")
```

**Expected result in Community Edition:**
```
❌ cloudFiles error: [INVALID_FORMAT.FEATURE_NOT_ENABLED] Auto Loader is not enabled in this workspace.
```

**Checkpoint file location:**
- Community Edition does NOT support Unity Catalog, so checkpoints must use DBFS paths:
  ```
  dbfs:/checkpoints/usertable/
  ```
- Standard/Premium can use Unity Catalog Volumes:
  ```
  /Volumes/main/default/checkpoints/usertable/
  ```

---

## About this tutorial

**Maintained by:** Lakeflow Community Connectors  
**Source code:** [github.com/lakeflow/lakeflow-community-connectors](https://github.com/lakeflow/lakeflow-community-connectors)  
**License:** Apache 2.0  
**Last updated:** January 26, 2026

For questions or contributions, please visit our GitHub repository or join the CockroachDB Community Slack.
