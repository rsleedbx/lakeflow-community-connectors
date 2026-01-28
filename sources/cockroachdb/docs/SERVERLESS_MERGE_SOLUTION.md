# Databricks Serverless MERGE Solution

## Problem

`foreachBatch` requires Python UDFs to be serialized and executed on Spark workers.  
In Databricks Serverless:
- **Driver**: Uses your notebook's Python version (3.11 in your case)
- **Workers**: Use Databricks Runtime Python version (3.12 in Serverless)
- **Result**: `PYTHON_VERSION_MISMATCH` error

## Why Append-Only Works

```python
# No Python functions sent to workers - pure Spark transformations
query = (df.writeStream
    .toTable(target_table)  # ← Pure JVM code
)
```

## Why Update-Delete Fails

```python
# Python function must run on workers (Python 3.12)
query = (deduped_df.writeStream
    .foreachBatch(merge_to_delta)  # ← Python UDF (requires Python 3.11)
    .start()
)
```

## Solution: Two-Stage Approach

### Stage 1: Stream to Staging Table (No Python UDFs)
```python
# Write to staging table (pure Spark, no Python)
staging_table = f"{target_table}_staging"
query = (deduped_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(staging_table)  # ← No Python UDFs
)
query.awaitTermination()
```

### Stage 2: Batch MERGE (Runs on Driver)
```python
# After stream completes, do MERGE (batch operation on driver)
staging_df = spark.read.table(staging_table)
target_delta = DeltaTable.forName(spark, target_table)

# MERGE runs on driver (Python 3.11) - not on workers
target_delta.alias("target").merge(
    staging_df.alias("source"),
    "target.ycsb_key = source.ycsb_key"
).whenMatchedUpdate(...).execute()
```

## Key Insight

**Streaming transformations** (select, filter, window) compile to JVM bytecode → version-agnostic  
**Python functions in foreachBatch** run on workers → require matching Python versions

The two-stage approach keeps all Python execution on the driver (your notebook), while Spark transformations run as JVM code on workers.

## Implementation

See updated `ingest_cdc_with_merge_single_family()` function in Cell 5.
