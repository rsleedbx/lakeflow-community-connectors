# Import Strategy: Use cockroachdb.py Functions in Notebook

## What's Available in `cockroachdb.py`

### ✅ Ready to Import

1. **`merge_column_family_fragments()`** - The core merge logic (line 5450)
   - Handles both standard and deduplication modes
   - Includes the NULL coalescing fix
   - Auto-detects fragmentation
   - **THIS IS WHAT WE NEED!**

2. **`analyze_azure_changefeed_files()`** - Analysis tool (line 3610)
   - Useful for debugging
   - Can show what's in Azure CDC files

3. **Helper functions:**
   - `load_crdb_config()` - Load JSON config (line 3180)
   - `get_primary_keys()` - Get PK from config (line 3199)
   - `parallel_delete_checkpoint()` - Fast checkpoint cleanup (line 4501)

### ❌ Not Available

The `ingest_cdc_*` functions that Cell 14 references **don't exist** in `cockroachdb.py`. These were likely notebook-specific and were removed.

However, `cockroachdb.py` has `load_and_merge_cdc_to_delta()` which works for **Unity Catalog Volumes**, not Azure Blob Storage.

---

## Recommended Approach

**Import the core building blocks and write simple ingestion code in the notebook.**

### Step 1: Add Import Cell (New Cell 9)

```python
# Import from cockroachdb.py
import sys
import os

# Add parent directory to path to import cockroachdb module
notebook_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
parent_dir = os.path.dirname(os.path.dirname(notebook_dir))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import the functions we need
from cockroachdb import (
    merge_column_family_fragments,  # ← The core merge logic with NULL fix!
    load_crdb_config,               # ← Load JSON config
    get_primary_keys,               # ← Get primary keys from config
    analyze_azure_changefeed_files, # ← Debug tool
    parallel_delete_checkpoint      # ← Fast cleanup
)

print("✅ Imported functions from cockroachdb.py")
print("   • merge_column_family_fragments (with NULL coalescing fix)")
print("   • load_crdb_config")
print("   • get_primary_keys")
print("   • analyze_azure_changefeed_files")
print("   • parallel_delete_checkpoint")
```

### Step 2: Replace Cell 8 with Import Reference

Since we're importing `merge_column_family_fragments` from `cockroachdb.py`, we can **delete Cell 8** (the local definition) and just use the imported one.

**Or** keep Cell 8 as a reference/fallback with a note:

```python
# ============================================================================
# NOTE: merge_column_family_fragments() is imported from cockroachdb.py
# ============================================================================
# This cell is kept for reference only.
# The actual function used is imported in Cell 9.
# 
# Function signature:
#   merge_column_family_fragments(
#       df,
#       primary_key_columns,
#       spark=None,
#       metadata_columns=None,
#       debug=False,
#       is_streaming=None,
#       deduplicate_to_latest_state=False  # ← Use True for NULL coalescing fix
#   )
#
# See cockroachdb.py line 5450 for implementation.
```

### Step 3: Simplify Cell 14 to Use Imported Function

**Option A: Inline CDC Ingestion (Recommended)**

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ============================================================================
# CDC Ingestion with Auto Loader + MERGE
# ============================================================================

print(f"🔷 CDC Configuration:")
print(f"   Processing Mode: {cdc_mode}")
print(f"   Column Family Mode: {column_family_mode}")
print()

# Build paths
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
checkpoint_path = f"/checkpoints/{target_schema}_{target_table}"
target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"

# For multi_cf mode, create staging table for batch processing
if column_family_mode == "multi_cf":
    staging_table_fqn = f"{target_table_fqn}_staging_cf"
    checkpoint_path = f"{checkpoint_path}_merge_cf"
else:
    staging_table_fqn = target_table_fqn  # Direct write for single_cf

print(f"📖 Ingesting CDC events")
print(f"   Source: {source_catalog}.{source_schema}.{source_table}")
print(f"   Target: {target_table_fqn}")
print(f"   Mode: {cdc_mode} + {column_family_mode}")
print()

# ============================================================================
# STAGE 1: Auto Loader → Staging Table
# ============================================================================

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("pathGlobFilter", f"*{source_table}*.parquet")
    .option("recursiveFileLookup", "true")
    .load(source_path)
)

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

# Stream to staging table
query = (transformed_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(staging_table_fqn)
)

query.awaitTermination()
print("✅ Stage 1 complete: Streamed to staging table\n")

# ============================================================================
# STAGE 2: Batch Processing (if needed)
# ============================================================================

if cdc_mode == "append_only" and column_family_mode == "single_cf":
    # Already done - data is in target
    print("✅ CDC INGESTION COMPLETE (append-only, no merge needed)")

elif cdc_mode == "append_only" and column_family_mode == "multi_cf":
    # Merge column families and append to target
    staging_df = spark.table(staging_table_fqn)
    
    # ← USE IMPORTED FUNCTION!
    merged_df = merge_column_family_fragments(
        staging_df,
        primary_key_columns=primary_key_columns,
        spark=spark,
        debug=True
    )
    
    merged_df.write.format("delta").mode("append").saveAsTable(target_table_fqn)
    spark.sql(f"DROP TABLE IF EXISTS {staging_table_fqn}")
    print("✅ CDC INGESTION COMPLETE (append-only + column families merged)")

elif cdc_mode == "update_delete":
    # Merge column families (if needed) + deduplicate + MERGE to target
    staging_df_raw = spark.table(staging_table_fqn)
    
    # ← USE IMPORTED FUNCTION WITH DEDUPLICATION MODE!
    staging_df = merge_column_family_fragments(
        staging_df_raw,
        primary_key_columns=primary_key_columns,
        spark=spark,
        deduplicate_to_latest_state=True,  # ← NULL coalescing fix!
        debug=True
    )
    
    # Create or MERGE to target
    if not spark.catalog.tableExists(target_table_fqn):
        # Initial table creation with DELETE handling
        delete_keys = staging_df.filter(F.col("_cdc_operation") == "DELETE").select(*primary_key_columns).distinct()
        active_rows = staging_df.filter(F.col("_cdc_operation") != "DELETE")
        final_rows = active_rows.join(delete_keys, on=primary_key_columns, how="left_anti")
        final_rows.write.format("delta").saveAsTable(target_table_fqn)
        print(f"   ✅ Created initial table")
    else:
        # MERGE logic
        delta_table = DeltaTable.forName(spark, target_table_fqn)
        join_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key_columns])
        
        (delta_table.alias("target").merge(
            staging_df.alias("source"),
            join_condition
        )
        .whenMatchedUpdate(
            condition="source._cdc_operation = 'UPSERT' AND source._cdc_timestamp > target._cdc_timestamp",
            set={col: f"source.{col}" for col in staging_df.columns}
        )
        .whenMatchedDelete(condition="source._cdc_operation = 'DELETE'")
        .whenNotMatchedInsert(
            condition="source._cdc_operation = 'UPSERT'",
            values={col: f"source.{col}" for col in staging_df.columns}
        )
        .execute())
        print(f"   ✅ MERGE complete")
    
    # Clean up staging
    spark.sql(f"DROP TABLE IF EXISTS {staging_table_fqn}")
    print("✅ CDC INGESTION COMPLETE (update_delete + MERGE applied)")

print(f"\n📊 Query your data: SELECT * FROM {target_table_fqn}")
```

**Option B: Create Helper Functions (keeps Cell 14 structure)**

Add a new cell before Cell 14 that defines the 4 functions using the imported `merge_column_family_fragments`:

```python
def ingest_cdc_append_only_single_family(storage_account_name, container_name, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, spark):
    # ... implementation using merge_column_family_fragments from import
    pass

# ... 3 more functions
```

---

## Benefits of Import Approach

✅ **Single source of truth:** Merge logic lives in `cockroachdb.py`  
✅ **Automatic bug fixes:** Updates to `cockroachdb.py` flow to notebook  
✅ **Consistent behavior:** Same logic in notebook and production code  
✅ **Easier maintenance:** One place to update merge logic  
✅ **Smaller notebook:** Less duplicated code  

---

## Potential Issues & Solutions

### Issue 1: Import Path

**Problem:** Databricks notebooks may not find `cockroachdb.py`

**Solution:** Add parent directory to `sys.path` (shown in Step 1 above)

### Issue 2: Different Environments

**Problem:** Databricks vs. local development

**Solution:** Add error handling to import:

```python
try:
    from cockroachdb import merge_column_family_fragments
    print("✅ Imported from cockroachdb.py")
except ImportError as e:
    print(f"⚠️  Could not import from cockroachdb.py: {e}")
    print("   Using local definition from Cell 8")
    # Cell 8 definition acts as fallback
```

---

## Recommendation

**Use Option A (Inline CDC Ingestion)** because:
1. Simpler and more explicit
2. Self-contained in the notebook
3. Easy to customize per use case
4. Uses imported `merge_column_family_fragments()` for the core logic

---

*Next Step: Shall I implement Option A in the notebook?*
