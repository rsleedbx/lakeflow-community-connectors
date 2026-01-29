# Deduplication Column Coalesce Fix - Critical for Multi-CF Tables

## Critical Issue

The MERGE logic is losing column family data when UPDATE events don't touch all columns.

### The Data (key=112)

```
Row 1: timestamp=1769709683938318073, field3-5=VALUES, field0-2=NULL, field6-9=NULL  ← Older INSERT
Row 2: timestamp=1769709683938318073, field0-2=VALUES, field3-9=NULL                  ← Older INSERT  
Row 3: timestamp=1769709683938318073, field6-9=VALUES, field0-5=NULL                  ← Older INSERT
Row 4: timestamp=1769719820020889740, field0-2=UPDATE,  field3-9=NULL                  ← Newer UPDATE!
```

### Current Logic Flow

**Step 1**: `merge_column_family_fragments()` groups by `(ycsb_key, _cdc_timestamp, _cdc_operation)`:
- ✅ Group 1 (timestamp=1769709683938318073): Rows 1-3 → Merged into one complete row with ALL field0-9 values
- ✅ Group 2 (timestamp=1769719820020889740): Row 4 → One row with field0-2=UPDATE, field3-9=NULL

Result after this step:
```
Merged Row A: ycsb_key=112, timestamp=1769709683938318073, field0-9=ALL VALUES  ✅
Merged Row B: ycsb_key=112, timestamp=1769719820020889740, field0-2=UPDATE, field3-9=NULL
```

**Step 2**: Deduplication keeps ONLY the LATEST timestamp:
```python
window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
staging_df = (staging_df_merged
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)  ← Keeps ONLY Row B (latest timestamp)
    .drop("_row_num")
)
```

Result:
- ✅ Keeps: Row B (field0-2=UPDATE, field3-9=NULL)
- ❌ **DISCARDS: Row A (field3-9=VALUES)**

**Final Target Table**:
```
ycsb_key=112: field0-2=UPDATE, field3-9=NULL  ❌ WRONG!
```

**Expected Target Table**:
```
ycsb_key=112: field0-2=UPDATE (from newer event), field3-9=VALUES (from older event)  ✅ CORRECT!
```

## Root Cause

The deduplication logic assumes **all columns are updated in every event**, which is FALSE for:
1. **Column family fragmentation** - Different column families can have different timestamps
2. **Partial column updates** - UPDATEs might only touch some columns

**The current logic**: "Keep the entire latest row" ❌  
**The correct logic**: "For each column, keep the latest non-NULL value" ✅

## The Fix

Replace the simple row-level deduplication with **column-level coalescing**:

### Current Code (Incorrect)

```python
# Merge column family fragments (batch mode - no streaming limitations!)
print(f"   🔧 Merging column family fragments...")
print(f"      Grouping by: {primary_key_columns} + _cdc_timestamp + _cdc_operation")
staging_df_merged = merge_column_family_fragments(staging_df_raw, primary_key_columns, spark)
print(f"   ✅ Column family fragments merged")

# Deduplicate by primary key (keep latest event)
print(f"   🔄 Deduplicating by primary keys: {primary_key_columns}...")
window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
staging_df = (staging_df_merged  # ← Use merged DataFrame
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)  ← PROBLEM: Discards older rows with valid column data!
    .drop("_row_num")
)
```

### Fixed Code (Correct)

```python
# Merge column family fragments (batch mode - no streaming limitations!)
print(f"   🔧 Merging column family fragments...")
print(f"      Grouping by: {primary_key_columns} + _cdc_timestamp + _cdc_operation")
staging_df_merged = merge_column_family_fragments(staging_df_raw, primary_key_columns, spark)
print(f"   ✅ Column family fragments merged")

# Coalesce columns across time (keep latest non-NULL value per column)
print(f"   🔄 Coalescing columns by primary keys: {primary_key_columns}...")
print(f"      Using last_value(col, ignorenulls=True) per column")

# Identify data columns (exclude PK and metadata)
metadata_columns = {'_cdc_operation', '_cdc_timestamp', '_rescued_data'}
data_columns = [
    col for col in staging_df_merged.columns
    if col not in primary_key_columns 
    and col not in metadata_columns
]

# For each data column, coalesce to latest non-NULL value
window_spec_coalesce = (Window.partitionBy(*primary_key_columns)
    .orderBy(F.col("_cdc_timestamp"))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

for col in data_columns:
    staging_df_merged = staging_df_merged.withColumn(
        col,
        F.last(F.col(col), ignorenulls=True).over(window_spec_coalesce)
    )

# Also coalesce _cdc_operation to the LATEST value (for DELETE handling)
staging_df_merged = staging_df_merged.withColumn(
    "_cdc_operation",
    F.last(F.col("_cdc_operation"), ignorenulls=True).over(window_spec_coalesce)
)

print(f"   ✅ Columns coalesced (latest non-NULL value per column)")

# Now deduplicate by primary key (keep LATEST row, which now has ALL coalesced columns)
print(f"   🔄 Deduplicating by primary keys: {primary_key_columns}...")
window_spec_dedup = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
staging_df = (staging_df_merged
    .withColumn("_row_num", F.row_number().over(window_spec_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

staging_count = staging_df.count()
fragments_removed = staging_count_raw - staging_df_merged.count()
duplicates_removed = staging_df_merged.count() - staging_count
print(f"   ✅ Column family fragments coalesced: {fragments_removed} fragments merged")
print(f"   ✅ Deduplicated: {staging_count} unique events ({duplicates_removed} duplicates removed)")
```

## Expected Output After Fix

### Before Fix (Incorrect)

Target table for key=112:
```
field0: updated_at_1769719820  ← From UPDATE event ✅
field1: inserted_value_112_1   ← From UPDATE event ✅
field2: inserted_value_112_2   ← From UPDATE event ✅
field3: NULL                   ← LOST! ❌
field4: NULL                   ← LOST! ❌
field5: NULL                   ← LOST! ❌
field6: NULL                   ← LOST! ❌
field7: NULL                   ← LOST! ❌
field8: NULL                   ← LOST! ❌
field9: NULL                   ← LOST! ❌
```

### After Fix (Correct)

Target table for key=112:
```
field0: updated_at_1769719820  ← From UPDATE event (latest) ✅
field1: inserted_value_112_1   ← From UPDATE event (latest) ✅
field2: inserted_value_112_2   ← From UPDATE event (latest) ✅
field3: inserted_value_112_3   ← From INSERT event (older, but only non-NULL value) ✅
field4: inserted_value_112_4   ← From INSERT event (older, but only non-NULL value) ✅
field5: inserted_value_112_5   ← From INSERT event (older, but only non-NULL value) ✅
field6: inserted_value_112_6   ← From INSERT event (older, but only non-NULL value) ✅
field7: inserted_value_112_7   ← From INSERT event (older, but only non-NULL value) ✅
field8: inserted_value_112_8   ← From INSERT event (older, but only non-NULL value) ✅
field9: inserted_value_112_9   ← From INSERT event (older, but only non-NULL value) ✅
```

## Technical Details

### Using `F.last(col, ignorenulls=True)`

```python
F.last(F.col(col), ignorenulls=True).over(
    Window.partitionBy(*primary_key_columns)
          .orderBy(F.col("_cdc_timestamp"))
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
```

**This aggregates for each column**:
- Partition by primary key (all events for same row)
- Order by timestamp (oldest to newest)
- `rowsBetween(unboundedPreceding, unboundedFollowing)` - look at ALL rows in the partition
- `F.last(col, ignorenulls=True)` - return the **LAST non-NULL value** in the ordered window

**Result**: Each row gets the latest non-NULL value for EVERY column!

### Why This Works

For key=112:
- **field0**: Row 4 has "updated_at_1769719820" (latest) → Use this ✅
- **field3**: Only Row 1 has "inserted_value_112_3" (no later non-NULL value) → Use this ✅
- **_cdc_operation**: Row 4 has "UPSERT" (latest) → Use this (correct for MERGE logic) ✅

## Impact

**Before Fix**:
- ❌ Column family fragments from older events are lost when newer events don't touch those columns
- ❌ Partial UPDATEs create data loss (NULLing out untouched columns)
- ❌ Multi-CF tables have data integrity issues

**After Fix**:
- ✅ Each column correctly retains its latest non-NULL value across all events
- ✅ Partial UPDATEs work correctly (untouched columns retain old values)
- ✅ Multi-CF tables have correct data integrity

## Files to Modify

- **`sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`** (Cell 7)
  - Update `ingest_cdc_with_merge_multi_family()` function
  - Add column-level coalescing before deduplication
  - Preserve metadata columns (_cdc_operation, _cdc_timestamp)

## Testing

To test the fix:
1. Run Cell 12 with the updated `ingest_cdc_with_merge_multi_family()` function
2. Run Cell 14 to verify sync
3. The diagnosis should now show: "✅ CDC PIPELINE IS WORKING PERFECTLY!"
4. All field3-9 columns should have VALUES, not NULL

## Related Issues

This is the **ROOT CAUSE** of all the diagnosis contradictions:
1. Data WAS in Azure ✅
2. Data WAS in staging ✅
3. Column family fragments WERE merged correctly ✅
4. BUT deduplication DISCARDED older column values ❌

All diagnostic tools were working correctly - they correctly identified the problem!
