# Bug Fix Session Summary - Column Family Data Integrity

This document summarizes the critical bugs discovered and fixed during the diagnosis of column family data loss in `update_delete + multi_cf` mode.

## Timeline of Discovery

### Initial Report
User reported that field3-9 were NULL in the target table for key=112, even though they had values in the source.

### Diagnosis Journey

1. **First Bug**: Contradictory diagnosis - "All columns match" vs. "field3 has different values"
2. **Second Bug**: Contradictory diagnosis - "Data not in Azure" vs. "Data found in staging (which comes from Azure)"
3. **Root Cause**: Deduplication logic was discarding older column values when newer events didn't touch those columns

---

## Bug #1: NULL Comparison False Positive

### File
`sources/cockroachdb/docs/cockroachdb_debug.py` - `compare_row_by_row()` function (line 288)

### Issue
The function was **skipping** NULL vs. non-NULL comparisons, reporting false positives:

```python
# BAD CODE:
if source_val and target_val:  # ← Skips if target_val is NULL!
    # ... compare values ...

if mismatches:
    print(f"❌ Key ({key_str}):")
else:
    print(f"✅ Key ({key_str}): All columns match")  # ← FALSE POSITIVE!
```

**Result**: When source="inserted_value_112_3" and target=NULL, it reported "✅ All columns match"

### Fix
Added explicit NULL handling BEFORE value comparison:

```python
# Check for NULL mismatches first
source_is_null = source_val is None or source_val == ''
target_is_null = target_val is None or target_val == ''

if source_is_null != target_is_null:
    # One is NULL, the other is not - MISMATCH!
    mismatches.append(f"{col}: {source_val} vs {target_val}")
    continue

# If both are NULL, they match
if source_is_null and target_is_null:
    continue

# Both have values - strip non-numeric for comparison
# ... existing logic ...
```

**Status**: ✅ Fixed

---

## Bug #2: Missing Schema Merge for Column Families

### Files
`sources/cockroachdb/docs/cockroachdb_debug.py` - 3 functions:
1. `analyze_cdc_events_by_column_family()` (line 340)
2. `check_staging_and_azure_for_keys()` (line 607)
3. `inspect_raw_cdc_files()` (line 691)

### Issue
When CockroachDB uses `split_column_families`, it creates **multiple Parquet files per event** with **different schemas**:
- Fragment 1: `ycsb_key`, `field0-2`
- Fragment 2: `ycsb_key`, `field3-9`

The diagnostic code was reading Azure files **without** `mergeSchema=true`, so Spark only saw columns from the **first file**:

```python
# BAD CODE:
df_raw = (spark.read
    .format("parquet")
    .option("pathGlobFilter", f"*{table_name}*.parquet")
    .option("recursiveFileLookup", "true")
    # ❌ MISSING: .option("mergeSchema", "true")
    .load(azure_path)
)
```

**Result**: Diagnostic code reported "field3-9 not in CDC files" even though they WERE in Azure (in different Parquet files)

### Fix
Added `.option("mergeSchema", "true")` to all 3 functions:

```python
df_raw = (spark.read
    .format("parquet")
    .option("pathGlobFilter", f"*{table_name}*.parquet")
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")  # ← Merge schemas from all column families
    .load(azure_path)
)

print(f"   📋 Columns detected: {len(df_raw.columns)} columns")
```

**Status**: ✅ Fixed

---

## Bug #3: Deduplication Discarding Older Column Values (ROOT CAUSE)

### File
`sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb` - Cell 7, `ingest_cdc_with_merge_multi_family()` function

### Issue
The deduplication logic was keeping the **entire latest row** instead of the **latest non-NULL value PER COLUMN**.

#### The Data (key=112)
```
Row 1: timestamp=1769709683938318073, field3-5=VALUES, field0-2=NULL, field6-9=NULL  ← Older INSERT
Row 2: timestamp=1769709683938318073, field0-2=VALUES, field3-9=NULL                  ← Older INSERT
Row 3: timestamp=1769709683938318073, field6-9=VALUES, field0-5=NULL                  ← Older INSERT
Row 4: timestamp=1769719820020889740, field0-2=UPDATE,  field3-9=NULL                  ← Newer UPDATE!
```

#### Current (Broken) Logic Flow

**Step 1**: `merge_column_family_fragments()` groups by `(ycsb_key, _cdc_timestamp, _cdc_operation)`:
- ✅ Group 1 (timestamp=...073): Rows 1-3 → ONE complete row with ALL field0-9 values
- ✅ Group 2 (timestamp=...740): Row 4 → ONE row with field0-2=UPDATE, field3-9=NULL

Result: 2 rows (one complete, one partial)

**Step 2**: Deduplication keeps ONLY the LATEST timestamp:
```python
# BAD CODE:
window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
staging_df = (staging_df_merged
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)  ← Keeps ONLY Row 4, DISCARDS complete row!
    .drop("_row_num")
)
```

**Result**:
- ✅ Keeps: Row 4 (field0-2=UPDATE, field3-9=NULL)
- ❌ **DISCARDS: Complete row (field3-9=VALUES)**

**Final Target**:
```
ycsb_key=112: field0-2=UPDATE, field3-9=NULL  ❌ DATA LOSS!
```

### Fix
Added **column-level coalescing** BEFORE deduplication:

```python
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
# This ensures older column family fragments don't get lost when newer events don't touch those columns
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
```

**Result After Fix**:
```
ycsb_key=112: field0-2=UPDATE (from newer event), field3-9=VALUES (from older event)  ✅ CORRECT!
```

**Status**: ✅ Fixed

---

## Impact Summary

### Before Fixes

- ❌ **Bug #1**: Diagnostic tools reported false positives, hiding real data integrity issues
- ❌ **Bug #2**: Diagnostic tools couldn't see columns from all column family fragments, blaming the wrong system (CockroachDB changefeed instead of MERGE logic)
- ❌ **Bug #3**: Production ingestion was LOSING DATA when:
  - Tables used multiple column families (`split_column_families=true`)
  - UPDATE events didn't touch all column families
  - Result: Older column values were discarded, replaced with NULL

### After Fixes

- ✅ **Bug #1 Fixed**: Diagnostic tools correctly detect NULL vs. non-NULL mismatches
- ✅ **Bug #2 Fixed**: Diagnostic tools can see ALL columns from ALL column family fragments
- ✅ **Bug #3 Fixed**: Production ingestion correctly retains the latest non-NULL value for EACH column, preventing data loss

---

## Testing Results

### Before Bug #3 Fix
```
📊 Checking 7 columns for value mismatches...

   ❌ field3: 1 rows with different values
   ❌ field4: 1 rows with different values
   ❌ field5: 1 rows with different values
   ❌ field6: 1 rows with different values
   ❌ field7: 1 rows with different values
   ❌ field8: 1 rows with different values
   ❌ field9: 1 rows with different values

+--------+--------------------+----------+
|ycsb_key|source_val          |target_val|
+--------+--------------------+----------+
|112     |inserted_value_112_3|NULL      |
+--------+--------------------+----------+
```

### After Bug #3 Fix (Expected)
```
📊 Checking 7 columns for value mismatches...

   ✅ field3: All values match
   ✅ field4: All values match
   ✅ field5: All values match
   ✅ field6: All values match
   ✅ field7: All values match
   ✅ field8: All values match
   ✅ field9: All values match

✅ CDC PIPELINE IS WORKING PERFECTLY!
```

---

## Files Modified

1. **`sources/cockroachdb/docs/cockroachdb_debug.py`**
   - Fixed `compare_row_by_row()` - NULL comparison bug
   - Fixed `analyze_cdc_events_by_column_family()` - Added `mergeSchema=true`
   - Fixed `check_staging_and_azure_for_keys()` - Added `mergeSchema=true`
   - Fixed `inspect_raw_cdc_files()` - Added `mergeSchema=true`

2. **`sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`** (Cell 7)
   - Fixed `ingest_cdc_with_merge_multi_family()` - Added column-level coalescing

## Documentation Created

1. **`COMPARE_ROW_BY_ROW_NULL_BUG_FIX.md`** - Bug #1 details
2. **`MERGE_SCHEMA_BUG_FIX.md`** - Bug #2 details
3. **`DEDUPLICATION_COLUMN_COALESCE_FIX.md`** - Bug #3 details (ROOT CAUSE)
4. **`BUG_FIX_SESSION_SUMMARY.md`** - This document

---

## Key Lessons Learned

1. **NULL comparisons must be explicit** - Boolean expressions like `if source_val and target_val` will skip NULL mismatches
2. **Spark doesn't merge schemas by default** - When reading multiple Parquet files with different schemas (column families), you MUST use `mergeSchema=true`
3. **Row-level deduplication is insufficient for column families** - You need column-level coalescing to preserve data from older events when newer events don't touch all columns
4. **Diagnostic tools must match production behavior** - The diagnostic code initially had Bug #2, but the production ingestion (Auto Loader with Structured Streaming) handled schema merging correctly by default

---

## Next Steps

1. **Run Cell 12** with the fixed `ingest_cdc_with_merge_multi_family()` function
2. **Run Cell 14** to verify sync
3. **Run Example 4 (Cell 30)** to verify diagnostic tools show consistent results
4. **Expected Result**: "✅ CDC PIPELINE IS WORKING PERFECTLY!" with all columns matching

---

## Production Impact

**Critical**: Bug #3 was causing **data loss** in production for ANY table using:
- `split_column_families=true` (multiple column families)
- `update_delete` mode (MERGE logic)
- Partial column UPDATEs (not all columns updated in every event)

**Severity**: High - Data integrity issue  
**Scope**: Only affected `update_delete + multi_cf` mode  
**Fix Status**: ✅ Complete - All bugs fixed

---

## Validation Checklist

- [x] Bug #1 fixed and tested (NULL comparison)
- [x] Bug #2 fixed and tested (`mergeSchema=true`)
- [x] Bug #3 fixed (column-level coalescing) - Awaiting user testing
- [ ] User confirms Cell 12 ingestion works correctly
- [ ] User confirms Cell 14 shows "✅ CDC PIPELINE IS WORKING PERFECTLY!"
- [ ] User confirms Example 4 (Cell 30) shows consistent diagnosis
