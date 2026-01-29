# Notebook Sync with cockroachdb.py - Status Report

**Date:** January 29, 2026  
**Task:** Ensure notebook merge logic matches cockroachdb.py exactly

---

## ✅ Successfully Updated

### 1. `merge_column_family_fragments()` - Cell 8

**Status:** ✅ **SYNCED** - Now matches `cockroachdb.py` exactly!

**Changes Made:**
- ✅ Added 4 new parameters: `metadata_columns`, `debug`, `is_streaming`, `deduplicate_to_latest_state`
- ✅ Added deduplication mode with cross-time column coalescing (THE FIX!)
- ✅ Added fragmentation detection for batch mode
- ✅ Enhanced docstring with examples
- ✅ Made `spark` parameter optional (backward compatible)

**New Signature:**
```python
def merge_column_family_fragments(
    df,
    primary_key_columns,
    spark=None,  # Now optional (backward compatible)
    metadata_columns=None,
    debug=False,
    is_streaming=None,
    deduplicate_to_latest_state=False  # ← THE FIX!
):
```

**Usage Examples:**

*Standard Mode (default):*
```python
df_merged = merge_column_family_fragments(
    df_raw,
    primary_key_columns=['ycsb_key'],
    spark,
    debug=True
)
```

*Deduplication Mode (NEW - with NULL coalescing fix):*
```python
df_latest = merge_column_family_fragments(
    df_staging,
    primary_key_columns=['ycsb_key'],
    spark,
    deduplicate_to_latest_state=True,  # ← Preserves old column values!
    debug=True
)
```

---

## ⚠️  Issue Found: Missing Ingest Functions

### Problem

**Cell 14** references 4 CDC ingestion functions that **DO NOT EXIST** in the notebook:

1. `ingest_cdc_append_only_single_family()` ❌
2. `ingest_cdc_append_only_multi_family()` ❌
3. `ingest_cdc_with_merge_single_family()` ❌
4. `ingest_cdc_with_merge_multi_family()` ❌

**Impact:** Cell 14 will fail with `NameError: name 'ingest_cdc_append_only_single_family' is not defined`

**Root Cause:** These functions were likely removed during a previous notebook cleanup, but Cell 14 was not updated.

---

## 📋 Recommendations to Fix Cell 14

### Option 1: Import from `cockroachdb.py` (if available)

**Check first:** Do these functions exist in `cockroachdb.py`?  
**Answer:** ❌ **NO** - They don't exist in `cockroachdb.py` either!

---

### Option 2: Inline the CDC Ingestion Logic (Recommended)

Replace Cell 14 with inline Auto Loader + MERGE logic using the updated `merge_column_family_fragments()` function.

**Example for `update_delete` + `multi_cf` mode:**

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Build paths
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
checkpoint_path = f"/checkpoints/{target_schema}_{target_table}_merge_cf"
target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
staging_table_fqn = f"{target_table_fqn}_staging_cf"

print(f"📖 Ingesting CDC events (MERGE + Column Families)")
print(f"Source: {source_catalog}.{source_schema}.{source_table}")
print(f"Target: {target_table_fqn}")

# Read with Auto Loader
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

# Read staging and apply merge logic with deduplication
staging_df_raw = spark.table(staging_table_fqn)

# ← USE THE UPDATED FUNCTION WITH THE FIX!
staging_df = merge_column_family_fragments(
    staging_df_raw,
    primary_key_columns=primary_key_columns,
    spark=spark,
    deduplicate_to_latest_state=True,  # ← THE FIX for NULL values!
    debug=True
)

# Create or MERGE to target
if not spark.catalog.tableExists(target_table_fqn):
    # Initial creation with DELETE handling
    delete_keys = staging_df.filter(F.col("_cdc_operation") == "DELETE").select(*primary_key_columns).distinct()
    active_rows = staging_df.filter(F.col("_cdc_operation") != "DELETE")
    final_rows = active_rows.join(delete_keys, on=primary_key_columns, how="left_anti")
    final_rows.write.format("delta").saveAsTable(target_table_fqn)
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

# Clean up staging
spark.sql(f"DROP TABLE IF EXISTS {staging_table_fqn}")

print("✅ CDC INGESTION COMPLETE")
```

---

### Option 3: Recreate the 4 Ingest Functions

Create simplified versions of the 4 functions that use the updated `merge_column_family_fragments()`.

**Pros:**
- Cell 14 doesn't need changes
- Clean separation of concerns

**Cons:**
- More code to maintain
- Duplication with `cockroachdb.py` patterns

---

## 🎯 Current State Summary

| Component | Status | Notes |
|-----------|--------|-------|
| `merge_column_family_fragments()` | ✅ **SYNCED** | Cell 8 now matches `cockroachdb.py` exactly |
| Cell 14 Ingest Logic | ❌ **BROKEN** | References 4 functions that don't exist |
| NULL Coalescing Fix | ✅ **IMPLEMENTED** | Available in `merge_column_family_fragments()` via `deduplicate_to_latest_state=True` |
| Fragmentation Detection | ✅ **IMPLEMENTED** | Auto-detects and skips merge if not needed |
| Debug Mode | ✅ **IMPLEMENTED** | Set `debug=True` for detailed output |
| Backward Compatibility | ✅ **MAINTAINED** | Old function calls still work (spark is optional now) |

---

## 🚀 Next Steps

1. **✅ DONE:** Updated `merge_column_family_fragments()` in Cell 8
2. **TODO:** Fix Cell 14 using one of the 3 options above
3. **TODO:** Test the updated function with real CDC data
4. **TODO:** Verify NULL coalescing works correctly

---

## 📚 Related Documentation

- `COCKROACHDB_PY_NULL_FIX.md` - Detailed explanation of the NULL coalescing fix
- `COLUMN_FAMILY_NULL_BEHAVIOR.md` - CockroachDB column family NULL behavior
- `DEDUPLICATION_COLUMN_COALESCE_FIX.md` - Original fix documentation
- `BUG_FIX_SESSION_SUMMARY.md` - Complete debugging session
- `CONNECTOR_EVOLUTION_STRATEGY.md` - Lesson #17 on NULL behavior

---

*Last updated: January 29, 2026*  
*Synced with: `cockroachdb.py` (merge_column_family_fragments function)*
