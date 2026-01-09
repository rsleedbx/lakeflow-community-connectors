# Stream a Changefeed to Databricks

**For submission to:** CockroachDB Documentation  
**Author:** Lakeflow Community Connectors  
**Date:** 2025-12-23

---

While CockroachDB is an excellent system of record, it also needs to coexist with other systems. For example, you might want to keep your data mirrored in data lakes, analytics engines, or machine learning pipelines.

This page demonstrates how to use a changefeed to stream row-level changes to Databricks, a unified analytics platform built on Apache Spark and Delta Lake.

> **Note:** Databricks Autoloader automatically handles schema inference, evolution, and deduplication, making it significantly simpler than traditional ETL pipelines. This tutorial shows how to stream data to Azure Blob Storage with Databricks Autoloader automatically ingesting changes into Delta Lake tables.

---

## Before you begin

Before you begin, make sure you have:

* Admin access to a CockroachDB Cloud account
* Write access to an Azure Blob Storage account  
  **Note:** This tutorial uses Azure Blob Storage for cloud storage. CockroachDB also supports AWS S3 and Google Cloud Storage. The Databricks Autoloader pattern works identically across all three cloud providers.
* Read and write access to a Databricks workspace (Standard or Premium tier)
* The `CHANGEFEED` privilege in order to create and manage changefeed jobs. Refer to [Required privileges](https://www.cockroachlabs.com/docs/stable/create-changefeed#required-privileges) for more details.

---

## Architecture Overview

This tutorial creates a streaming CDC pipeline with:

```
CockroachDB → Azure Blob Storage → Databricks Autoloader → Delta Lake
```

**Key advantages over other approaches:**

* **No middleware required:** No SQS queues, Snowpipes, or custom ETL
* **Automatic schema inference:** Databricks detects schema changes automatically
* **Built-in deduplication:** Delta Lake MERGE handles updates and duplicates
* **Support for deletes:** Full CDC operations (INSERT, UPDATE, DELETE)
* **Cost-effective:** Only storage costs, no additional compute for ingestion
* **Multi-format support:** Works with both JSON and Parquet

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
   
   > **Note:** Rangefeeds are enabled by default on CockroachDB Standard and Basic clusters.

---

## Step 4. Create a database and schema

1. In the built-in SQL shell, create a database called `ecommerce`:
   ```sql
   CREATE DATABASE ecommerce;
   ```

2. Set it as the default:
   ```sql
   SET DATABASE = ecommerce;
   ```

3. Create a schema for organizing tables (optional but recommended for multi-tenant deployments):
   ```sql
   CREATE SCHEMA IF NOT EXISTS public;
   ```

---

## Step 5. Create a table

Before you can start a changefeed, you need to create at least one table for the changefeed to target. The targeted table's rows are referred to as the "watched rows".

Create a table called `orders`:

```sql
CREATE TABLE public.orders (
    order_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id INT NOT NULL,
    product_id  INT NOT NULL,
    quantity    INT NOT NULL,
    total       DECIMAL(10,2) NOT NULL,
    status      STRING NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Insert some sample data
INSERT INTO public.orders (customer_id, product_id, quantity, total, status)
VALUES 
    (1001, 5001, 2, 49.98, 'pending'),
    (1002, 5002, 1, 29.99, 'pending'),
    (1003, 5003, 3, 89.97, 'processing');
```

---

## Step 6. Create an Azure Blob Storage container

Every change to a watched row is emitted as a record in a configurable format (JSON or Parquet). To configure Azure Blob Storage as the cloud storage sink:

1. Log in to the [Azure Portal](https://portal.azure.com).

2. Create a storage account or use an existing one.

3. Create a container called `changefeed-events` where streaming updates from the watched tables will be collected.

4. Navigate to **Access keys** under **Security + networking** and copy:
   - Storage account name
   - One of the access keys (key1 or key2)

You will need these credentials when creating your changefeed.

---

## Step 7. Create a changefeed

### Option A: Parquet Format (Recommended for Analytics)

Parquet format provides:
- Efficient columnar storage (smaller files, faster queries)
- Native support in Databricks and Spark
- Lower storage costs compared to JSON

Back in the built-in SQL shell, create a changefeed with Parquet format:

```sql
CREATE CHANGEFEED FOR TABLE ecommerce.public.orders
INTO 'azure://changefeed-events/parquet/ecommerce/public/?
    AZURE_ACCOUNT_NAME={your-storage-account-name}&
    AZURE_ACCOUNT_KEY={your-storage-account-key}'
WITH 
    format = 'parquet',
    compression = 'gzip',
    updated,
    resolved = '10s',
    initial_scan = 'yes';
```

**Key parameters:**
- **Path structure:** `parquet/ecommerce/public/` organizes files by format, database, and schema
- **format = 'parquet':** Use columnar Parquet format
- **compression = 'gzip':** Compress files for efficient storage
- **updated:** Include update timestamps for each row
- **resolved = '10s':** Emit resolved timestamps every 10 seconds
- **initial_scan = 'yes':** Capture existing rows before streaming changes

### Option B: JSON Format (For Detailed Change Tracking)

JSON format provides:
- Explicit operation types (INSERT, UPDATE, DELETE)
- Before/after values for updates
- Human-readable format for debugging

```sql
CREATE CHANGEFEED FOR TABLE ecommerce.public.orders
INTO 'azure://changefeed-events/json/ecommerce/public/?
    AZURE_ACCOUNT_NAME={your-storage-account-name}&
    AZURE_ACCOUNT_KEY={your-storage-account-key}'
WITH 
    format = 'json',
    envelope = 'wrapped',
    diff,
    updated,
    resolved = '10s',
    initial_scan = 'yes';
```

**Additional JSON parameters:**
- **envelope = 'wrapped':** Wrap each event with metadata
- **diff:** Include both before and after values for updates

---

Refer to the [Cloud Storage Authentication](https://www.cockroachlabs.com/docs/stable/cloud-storage-authentication) page for more detail on authenticating to Azure and other cloud providers.

You will receive the changefeed's job ID:

```
        job_id
+--------------------+
  912345678901234567
(1 row)
```

You can use this job ID to manage the changefeed if needed.

---

## Step 8. Verify data is streaming to Azure

1. In the built-in SQL shell, perform some operations on the `orders` table:
   ```sql
   -- Insert a new order
   INSERT INTO public.orders (customer_id, product_id, quantity, total, status)
   VALUES (1004, 5004, 1, 19.99, 'pending');
   
   -- Update an existing order
   UPDATE public.orders 
   SET status = 'shipped', updated_at = now() 
   WHERE customer_id = 1001;
   
   -- Delete an order
   DELETE FROM public.orders WHERE customer_id = 1002;
   ```

2. Navigate to your Azure Blob Storage container in the Azure Portal.

3. You should see a new directory structure:
   ```
   parquet/ecommerce/public/orders/2025-12-23/
   └── {timestamp}-{jobid}-...-orders-1.parquet
   ```
   
   Or for JSON:
   ```
   json/ecommerce/public/orders/2025-12-23/
   └── {timestamp}-{jobid}-...-orders-1.ndjson
   ```

> **Note:** Files appear within seconds for initial scan data. CDC events may be batched and appear within 30-120 seconds depending on the volume of changes.

---

## Step 9. Configure Databricks Autoloader

### Create a Databricks notebook

1. Log in to your Databricks workspace.

2. Create a new notebook (Python or SQL).

3. Configure Azure storage credentials (if not using Unity Catalog External Locations):
   ```python
   # Configure Azure storage access
   spark.conf.set(
       "fs.azure.account.key.{your-storage-account-name}.blob.core.windows.net",
       "{your-storage-account-key}"
   )
   ```

### For Parquet Format:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Define source and target paths
source_path = "wasbs://changefeed-events@{your-storage-account-name}.blob.core.windows.net/parquet/ecommerce/public/orders/"
checkpoint_path = "/checkpoints/ecommerce/public/orders/parquet"
target_table = "ecommerce_catalog.public.orders"

# Read streaming data with Autoloader
raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
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

# Deduplicate by primary key (handles split_column_families)
window = Window.partitionBy("order_id").orderBy(F.col("_cdc_updated").desc())
deduped_stream = processed_stream \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num", "__crdb__event_type", "__crdb__updated")

# Define merge logic
def merge_to_delta(micro_batch_df, epoch_id):
    """Merge CDC events into Delta table with upsert/delete logic."""
    
    # Create table if it doesn't exist
    if not spark.catalog.tableExists(target_table):
        micro_batch_df.write.format("delta").saveAsTable(target_table)
        return
    
    # Merge into existing table
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(
        condition="source._cdc_operation = 'UPSERT' AND source._cdc_updated > target._cdc_updated",
        set={
            "customer_id": "source.customer_id",
            "product_id": "source.product_id",
            "quantity": "source.quantity",
            "total": "source.total",
            "status": "source.status",
            "created_at": "source.created_at",
            "updated_at": "source.updated_at",
            "_cdc_updated": "source._cdc_updated"
        }
    ).whenMatchedDelete(
        condition="source._cdc_operation = 'DELETE'"
    ).whenNotMatchedInsert(
        condition="source._cdc_operation = 'UPSERT'",
        values={
            "order_id": "source.order_id",
            "customer_id": "source.customer_id",
            "product_id": "source.product_id",
            "quantity": "source.quantity",
            "total": "source.total",
            "status": "source.status",
            "created_at": "source.created_at",
            "updated_at": "source.updated_at",
            "_cdc_updated": "source._cdc_updated"
        }
    ).execute()

# Start streaming
query = (deduped_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .foreachBatch(merge_to_delta)
    .trigger(availableNow=True)  # Process all available data then stop
    .start()
)

# Wait for completion
query.awaitTermination()
```

### For JSON Format:

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Define source and target paths
source_path = "wasbs://changefeed-events@{your-storage-account-name}.blob.core.windows.net/json/ecommerce/public/orders/"
checkpoint_path = "/checkpoints/ecommerce/public/orders/json"
target_table = "ecommerce_catalog.public.orders"

# Read streaming data with Autoloader
raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)

# Extract data from wrapped envelope
processed_stream = raw_stream.select(
    F.col("after.*"),
    F.col("updated").alias("_cdc_updated"),
    F.when(F.col("after").isNotNull() & F.col("before").isNull(), "INSERT")
     .when(F.col("after").isNotNull() & F.col("before").isNotNull(), "UPDATE")
     .when(F.col("after").isNull() & F.col("before").isNotNull(), "DELETE")
     .alias("_cdc_operation")
)

# Define merge logic
def merge_to_delta(micro_batch_df, epoch_id):
    """Merge CDC events into Delta table with full INSERT/UPDATE/DELETE support."""
    
    # Create table if it doesn't exist
    if not spark.catalog.tableExists(target_table):
        micro_batch_df.filter(F.col("_cdc_operation") != "DELETE") \
            .write.format("delta").saveAsTable(target_table)
        return
    
    # Merge into existing table
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(
        condition="source._cdc_operation = 'UPDATE' AND source._cdc_updated > target._cdc_updated",
        set={"*"}
    ).whenMatchedDelete(
        condition="source._cdc_operation = 'DELETE'"
    ).whenNotMatchedInsert(
        condition="source._cdc_operation IN ('INSERT', 'UPDATE')",
        values={"*"}
    ).execute()

# Start streaming
query = (processed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .foreachBatch(merge_to_delta)
    .trigger(availableNow=True)
    .start()
)

# Wait for completion
query.awaitTermination()
```

---

## Step 10. Query the Delta Lake table

To view the data in Databricks, query the `orders` table:

```sql
SELECT * FROM ecommerce_catalog.public.orders ORDER BY updated_at DESC;
```

The ingested rows will display immediately. Unlike Snowflake, there is no need to refresh pipes or wait for scheduled tasks.

Your changefeed is now streaming to Databricks with automatic deduplication and CDC support!

---

## Benefits over traditional approaches

### Compared to Snowflake

| Feature | Snowflake | Databricks Autoloader |
|---------|-----------|----------------------|
| Setup complexity | 7 steps + SQS configuration | 1 notebook |
| Schema evolution | Manual stage updates | Automatic |
| Deduplication | Requires streams + tasks | Built-in with Delta MERGE |
| Delete handling | Manual materialized views | Native support |
| Cost | Storage + SQS + compute | Storage only |
| Latency | Minutes (Snowpipe batching) | Seconds (streaming) |
| Format support | JSON primarily | JSON + Parquet |

### Key advantages

1. **No middleware required:** Unlike Snowflake (which requires SQS queues and Snowpipes), Databricks Autoloader directly processes files from cloud storage.

2. **Automatic schema inference:** Databricks automatically detects and adapts to schema changes, eliminating manual intervention.

3. **Native CDC support:** Delta Lake MERGE provides native INSERT, UPDATE, and DELETE operations without custom ETL.

4. **Cost-effective:** No additional compute or queue costs—only cloud storage.

5. **Multi-format support:** Works seamlessly with both JSON (for detailed change tracking) and Parquet (for efficient analytics).

6. **Real-time processing:** Autoloader triggers immediately when new files arrive, providing near real-time data availability.

---

## Organizing multiple databases and schemas

For production deployments with multiple databases, schemas, and tables, use the hierarchical path structure:

```
changefeed-events/
├── parquet/
│   ├── ecommerce/
│   │   ├── public/
│   │   │   ├── orders/
│   │   │   ├── customers/
│   │   │   └── products/
│   │   └── staging/
│   │       └── temp_orders/
│   └── warehouse/
│       └── public/
│           └── inventory/
└── json/
    └── ecommerce/
        └── public/
            └── orders/
```

Create separate changefeeds for each table:

```sql
-- Ecommerce orders (Parquet)
CREATE CHANGEFEED FOR TABLE ecommerce.public.orders
INTO 'azure://changefeed-events/parquet/ecommerce/public/?...'
WITH format = 'parquet', ...;

-- Warehouse inventory (Parquet)
CREATE CHANGEFEED FOR TABLE warehouse.public.inventory
INTO 'azure://changefeed-events/parquet/warehouse/public/?...'
WITH format = 'parquet', ...;
```

This organization:
- Prevents table name collisions across databases/schemas
- Enables per-database access control
- Simplifies Autoloader configuration (one path per table)
- Supports easy format switching (JSON vs Parquet)

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

1. **Verify files in Azure Storage:** Check that Parquet/JSON files are being created in the expected path.

2. **Check Autoloader schema location:** Ensure the schema location is accessible and not corrupted.

3. **Review checkpoint location:** If reprocessing is needed, delete the checkpoint directory (data will be reprocessed from the beginning).

4. **Validate merge conditions:** Ensure primary key joins in the MERGE statement match your table schema.

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

Refer to the [Create and Configure Changefeeds](https://www.cockroachlabs.com/docs/stable/create-changefeed) page for more general changefeed limitations.

---

## Next steps

- Learn more about [Databricks Autoloader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- Explore [Delta Lake MERGE](https://docs.databricks.com/delta/merge.html) capabilities
- Read about [CockroachDB changefeed best practices](https://www.cockroachlabs.com/docs/stable/changefeed-best-practices)
- Set up [Unity Catalog External Locations](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html) for production deployments

---

## About this tutorial

**Maintained by:** Lakeflow Community Connectors  
**Source code:** [github.com/lakeflow/lakeflow-community-connectors](https://github.com/lakeflow/lakeflow-community-connectors)  
**License:** Apache 2.0  
**Last updated:** December 23, 2025

For questions or contributions, please visit our GitHub repository or join the CockroachDB Community Slack.




