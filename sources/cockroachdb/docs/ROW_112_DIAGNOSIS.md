# Row 112 Diagnosis Summary

## Issue Overview

Row 112 in the target table has **NULL values for field3-9**, while the source CockroachDB has actual values. The diagnostic analysis reveals the root cause.

## Root Cause

**The changefeed never captured the column family fragment containing field3-9 for row 112.**

### Evidence from Diagnosis

```
📊 Column completeness for mismatched keys:
   ✅ ycsb_key  :   4 with values,   0 NULL
   ✅ field0    :   2 with values,   2 NULL
   ✅ field1    :   2 with values,   2 NULL
   ✅ field2    :   2 with values,   2 NULL
   ⚠️  field3    : Column not in CDC files    ← NOT IN CDC FILES
   ⚠️  field4    : Column not in CDC files    ← NOT IN CDC FILES
   ⚠️  field5    : Column not in CDC files    ← NOT IN CDC FILES
   ⚠️  field6    : Column not in CDC files    ← NOT IN CDC FILES
   ⚠️  field7    : Column not in CDC files    ← NOT IN CDC FILES
   ⚠️  field8    : Column not in CDC files    ← NOT IN CDC FILES
   ⚠️  field9    : Column not in CDC files    ← NOT IN CDC FILES
```

### Azure CDC Files for Key 112

```
+--------+---------------------+--------------------+--------------------+------------------+------------------------------+
|ycsb_key|field0               |field1              |field2              |__crdb__event_type|__crdb__updated               |
+--------+---------------------+--------------------+--------------------+------------------+------------------------------+
|112     |NULL                 |NULL                |NULL                |c                 |1769709683938318073.0000000000|
|112     |inserted_value_112_0 |inserted_value_112_1|inserted_value_112_2|c                 |1769709683938318073.0000000000|
|112     |NULL                 |NULL                |NULL                |c                 |1769709683938318073.0000000000|
|112     |updated_at_1769719820|inserted_value_112_1|inserted_value_112_2|c                 |1769719820020889740.0000000000|
+--------+---------------------+--------------------+--------------------+------------------+------------------------------+
```

**Key observation**: All 4 CDC events for row 112 contain ONLY field0-2 (and ycsb_key). There is **no CDC event** containing field3-9 columns.

## Analysis

### Expected Behavior (with split_column_families)

For a multi-column family table with `split_column_families` enabled, CockroachDB should emit:

1. **Fragment 1**: `ycsb_key` + `field0-2` (column family 1)
2. **Fragment 2**: `ycsb_key` + `field3-9` (column family 2)

### Actual Behavior for Row 112

Only **Fragment 1** was captured by the changefeed. **Fragment 2 was never emitted**.

### Why This Happened

This is one of the known issues with CockroachDB changefeeds and column families:

1. **Initial INSERT**: When row 112 was first inserted, the changefeed captured Fragment 1 (field0-2) but **failed to capture Fragment 2 (field3-9)**.

2. **Subsequent UPDATE**: The UPDATE event at timestamp `1769719820020889740` also only updated field0-2, so no Fragment 2 was emitted then either.

3. **Result**: The Databricks target has row 112 with:
   - ✅ field0-2: Correct values from CDC
   - ❌ field3-9: NULL (never captured)

## Fixes Applied

### Fix 1: Staging Table Suffix (✅ FIXED)

**Issue**: Cell 16 was using wrong staging table name:
- **Old**: `_staging`
- **New**: `_staging_cf` (correct for multi_cf mode)

**Fixed in**: `cockroachdb-cdc-tutorial.ipynb` Cell 16

### Fix 2: Catalog Name (⚠️ CONFIGURATION ISSUE)

**Issue**: The diagnosis shows:
```
Staging: robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf_staging_cf
```

Should be:
```
Staging: main.robert_lee_cockroachdb.usertable_update_delete_multi_cf_staging_cf
```

**Cause**: `target_catalog` in the configuration is set to `robert_lee` instead of `main`.

**Action Required**: Update your configuration file or Cell 3 to set:
```python
target_catalog = "main"  # Not "robert_lee"
```

## Solutions to the Column Family Capture Issue

### Option 1: Re-run Full Ingestion from Scratch (Recommended)

This will force CockroachDB to re-emit all data:

```python
# 1. Drop changefeed in CockroachDB (Cell 8)
CANCEL JOB <job_id>;

# 2. Clear all Azure CDC files
# (manually delete from Azure portal or use dbutils.fs.rm)

# 3. Clear Databricks state (run provided cleanup script)

# 4. Recreate changefeed (Cell 7)

# 5. Re-run ingestion (Cell 12)
```

### Option 2: Manual Backfill (Quick Fix)

If you only have a few affected rows, manually backfill them:

```python
# In Databricks
spark.sql("""
    UPDATE main.robert_lee_cockroachdb.usertable_update_delete_multi_cf
    SET 
        field3 = 'inserted_value_112_3',
        field4 = 'inserted_value_112_4',
        field5 = 'inserted_value_112_5',
        field6 = 'inserted_value_112_6',
        field7 = 'inserted_value_112_7',
        field8 = 'inserted_value_112_8',
        field9 = 'inserted_value_112_9'
    WHERE ycsb_key = 112
""")
```

### Option 3: Investigate Changefeed Configuration

Check if there's a CockroachDB bug or configuration issue preventing the second column family fragment from being emitted:

1. Check CockroachDB logs for errors related to row 112
2. Verify `split_column_families` is enabled in the changefeed
3. Check if there are any CockroachDB version-specific bugs

## Next Steps

1. ✅ **Fixed**: Staging table suffix in Cell 16
2. ⚠️  **Action Required**: Fix `target_catalog` configuration (set to `main`)
3. 🔍 **Investigate**: Why did the changefeed fail to capture field3-9 for row 112?
4. 🔧 **Choose Solution**: Re-run full ingestion (Option 1) or manual backfill (Option 2)

## Related GitHub Issues

This may be related to the CockroachDB issues documented in:
- `sources/cockroachdb/CONNECTOR_EVOLUTION_STRATEGY.md` - Section "GitHub Issues Filed"
- Consider filing a new issue if this is reproducible

## Diagnostic Commands Used

```python
from cockroachdb_debug import run_full_diagnosis

run_full_diagnosis(
    conn=conn,
    spark=spark,
    source_table="defaultdb.public.usertable_update_delete_multi_cf",
    target_df=target_df,
    staging_table="main.robert_lee_cockroachdb.usertable_update_delete_multi_cf_staging_cf",
    azure_path="abfss://...",
    primary_keys=["ycsb_key"],
    mismatched_columns=["field3", "field4", "field5", "field6", "field7", "field8", "field9"]
)
```
