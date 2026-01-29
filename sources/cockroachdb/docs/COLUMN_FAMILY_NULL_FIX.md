# Column Family NULL Coalescing Fix

## Problem Summary

When using `update_delete` mode with `multi_cf` (multiple column families), the target table had NULL values for columns that should have had data:

```
❌ Key (ycsb_key=8):
   field3: snapshot_value_8_3 vs NULL
   field4: snapshot_value_8_4 vs NULL
   field5: snapshot_value_8_5 vs NULL
   field6: snapshot_value_8_6 vs NULL
   field7: snapshot_value_8_7 vs NULL
   field8: snapshot_value_8_8 vs NULL
   field9: snapshot_value_8_9 vs NULL
```

## Root Cause

### The Data Pattern

When CockroachDB splits column families (`split_column_families=true`), a single logical row is split into multiple CDC events (fragments):

```
Azure CDC Events for key=8:
1. field0=value, field1=value, field2=value, field3=NULL, field4=NULL, ... (family 1)
2. field0=NULL, field1=NULL, field2=NULL, field3=value, field4=value, field5=value, ... (family 2)
3. field0=NULL, field1=NULL, ..., field6=value, field7=value, field8=value, field9=value (family 3)
4. field0=UPDATED, field1=value, field2=value, field3=NULL, field4=NULL, ... (UPDATE event)
```

### The Bug

The old code in `ingest_cdc_with_merge_multi_family()` was doing:

1. **Step 1**: Merge fragments within same timestamp (line 1051)
   ```python
   staging_df_merged = merge_column_family_fragments(staging_df_raw, primary_key_columns)
   ```
   - This only merged fragments with the **same timestamp**
   - It used `first(col, ignorenulls=True)` within each event
   - After this step, you'd have multiple rows per key (one per UPDATE event)

2. **Step 2**: Deduplicate to keep latest row (lines 1054-1061)
   ```python
   window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
   staging_df = (staging_df_merged
       .withColumn("_row_num", F.row_number().over(window_spec))
       .filter(F.col("_row_num") == 1)
       .drop("_row_num")
   )
   ```
   - This kept only the **LATEST row** per key
   - ❌ **Problem**: The latest row (event 4) has NULLs for field3-9 because that UPDATE only touched field0
   - Result: Lost the values from earlier events!

## The Fix

### What Changed

Replaced the two-step process with a single call using `deduplicate_to_latest_state=True`:

```python
# OLD CODE (WRONG):
staging_df_merged = merge_column_family_fragments(staging_df_raw, primary_key_columns)
# Then manual deduplication that loses values...

# NEW CODE (CORRECT):
staging_df = merge_column_family_fragments(
    staging_df_raw, 
    primary_key_columns,
    deduplicate_to_latest_state=True,  # ← CRITICAL FIX
    debug=True
)
```

### How `deduplicate_to_latest_state=True` Works

The enhanced `merge_column_family_fragments()` function (from cockroachdb.py lines 5450-5838) does **two operations**:

#### Step 1: Column-Level Coalescing (Across Time)
```python
# Window: partition by PK, order by timestamp, look at ALL rows
window_spec_coalesce = (Window.partitionBy(*primary_key_columns)
    .orderBy(F.col('_cdc_timestamp'))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

# For each column, take LAST non-NULL value across all events
for col in data_columns:
    df = df.withColumn(
        col,
        F.last(F.col(col), ignorenulls=True).over(window_spec_coalesce)
    )
```

**Result after Step 1**: Every row now has ALL column values filled in (no NULLs unless the column was never set):

```
After coalescing (all rows for key=8):
Row 1: field0=value,   field3=value (from event 2), field6=value (from event 3)
Row 2: field0=value,   field3=value (from event 2), field6=value (from event 3)
Row 3: field0=value,   field3=value (from event 2), field6=value (from event 3)
Row 4: field0=UPDATED, field3=value (from event 2), field6=value (from event 3)
```

#### Step 2: Row-Level Deduplication
```python
# Now deduplicate - keep LATEST row (which now has ALL values preserved)
window_spec_dedup = Window.partitionBy(*primary_key_columns).orderBy(F.col('_cdc_timestamp').desc())
df_merged = (df
    .withColumn("_row_num", F.row_number().over(window_spec_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```

**Final result**: One row per key with **all column values preserved**:

```
Final row for key=8:
field0=UPDATED (from latest event 4)
field3=value    (preserved from event 2)
field6=value    (preserved from event 3)
```

## Code Changes

### File: `cockroachdb_autoload.py`

**Lines 1048-1068** in `ingest_cdc_with_merge_multi_family()`:

#### Before
```python
# Merge column family fragments (batch mode - no streaming limitations!)
print(f"   🔧 Merging column family fragments...")
print(f"      Grouping by: {primary_key_columns} + _cdc_timestamp + _cdc_operation")
staging_df_merged = merge_column_family_fragments(staging_df_raw, primary_key_columns)
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
```

#### After
```python
# Merge column family fragments WITH NULL coalescing (batch mode)
# CRITICAL: Use deduplicate_to_latest_state=True to preserve column values
# across fragments and time (fixes the NULL bug for column families)
print(f"   🔧 Merging column family fragments with NULL coalescing...")
print(f"      Mode: deduplicate_to_latest_state=True")
print(f"      This preserves latest non-NULL value per column across all events")

staging_df = merge_column_family_fragments(
    staging_df_raw, 
    primary_key_columns,
    deduplicate_to_latest_state=True,  # ← CRITICAL FIX for column family NULLs
    debug=True  # Show merge statistics
)

staging_count = staging_df.count()
fragments_removed = staging_count_raw - staging_count
print(f"   ✅ Merged and deduplicated: {staging_count} unique keys ({fragments_removed} fragments/duplicates removed)")
```

### Why This Works

1. ✅ **Handles fragments within same event**: The function first groups by PK+timestamp to merge fragments
2. ✅ **Handles multiple UPDATE events**: The coalescing step looks across ALL events for each key
3. ✅ **Preserves old values**: Uses `last(..., ignorenulls=True)` so NULLs from newer events don't overwrite old values
4. ✅ **Single operation**: All logic is in one place, easier to understand and maintain

## Impact on Other Functions

### `ingest_cdc_append_only_multi_family()` (Line 899)
✅ **No change needed** - correctly uses default mode (`deduplicate_to_latest_state=False`):
```python
merged_df = merge_column_family_fragments(staging_df, primary_key_columns)
```
This preserves ALL CDC events as intended for append_only mode.

### `ingest_cdc_with_merge_single_family()` (No multi_cf)
✅ **No change needed** - doesn't use `merge_column_family_fragments()` at all (single family doesn't fragment).

### `ingest_cdc_append_only_single_family()` (No multi_cf)
✅ **No change needed** - doesn't use `merge_column_family_fragments()` at all.

## Testing Instructions

1. **Clean up existing data**:
   ```python
   # Drop target and staging tables
   spark.sql("DROP TABLE IF EXISTS robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf")
   spark.sql("DROP TABLE IF EXISTS robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf_staging_cf")
   
   # Clear checkpoint
   dbutils.fs.rm("/checkpoints/robert_lee_cockroachdb_usertable_update_delete_multi_cf_merge_cf", True)
   ```

2. **Re-run Cell 12** (CDC ingestion)

3. **Run diagnosis Cell 16** - should now show:
   ```
   ✅ Key (ycsb_key=8): All columns match
   ```

4. **Verify the merge statistics** in Cell 12 output - you should see debug output like:
   ```
   🔍 Column Family Merge (Batch Mode)
      Primary key columns: ['ycsb_key']
      ...
   🔄 Applying cross-time coalescing + deduplication...
      (Preserves latest non-NULL value per column across all events)
   ```

## Expected Behavior

### Before Fix
```
Target table for key=8:
field0 = "updated_at_1769725667"  ← From latest UPDATE
field3 = NULL                      ← LOST! (was "snapshot_value_8_3")
field4 = NULL                      ← LOST! (was "snapshot_value_8_4")
...
```

### After Fix
```
Target table for key=8:
field0 = "updated_at_1769725667"  ← From latest UPDATE
field3 = "snapshot_value_8_3"     ← PRESERVED from earlier event
field4 = "snapshot_value_8_4"     ← PRESERVED from earlier event
...
```

## Related Documentation

- **Function Reference**: `merge_column_family_fragments()` in `cockroachdb.py` lines 5450-5838
- **Technical Details**: `COCKROACHDB_PY_NULL_FIX.md`
- **Original Issue**: Discovered during Cell 14 debugging
- **GitHub Issue**: CockroachDB column family NULL behavior (filed separately)

## Summary

The fix ensures that when using `update_delete` mode with multiple column families:
- ✅ Column family fragments are properly merged
- ✅ Column values are preserved across multiple UPDATE events
- ✅ The latest non-NULL value is retained for each column
- ✅ Target table matches source table exactly

**Status**: ✅ Ready for testing - run Cell 12 again!
