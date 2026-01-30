# Fix: Consistent Deduplication for APPEND_ONLY Mode Diagnosis

## Problem

There was a mismatch between:
- **Top** (Column Family Assignments): Showed field3-field9 as ❌ mismatched
- **Bottom** (Detailed Analysis): Showed all fields as ✅ matching

This happened because:
1. **Cell 14** (verification): Used **raw target data** without deduplication → reported mismatches
2. **Diagnosis functions**: Used **deduplicated target data** → showed matches

## Root Cause

In `append_only` mode, the target table contains multiple versions of the same key (full CDC event log). When comparing:
- **Source**: Has only current state (1 row per key)
- **Target (raw)**: Has all events (multiple rows per key) → Sums are inflated
- **Target (deduplicated)**: Has latest state (1 row per key) → Sums match source

## Solution

### 1. Fixed Cell 14 (Verification Cell)

**Before:**
```python
target_df = spark.read.table(target_table_fqn)
target_sum = get_column_sum_spark(target_df, 'ycsb_key')  # Uses all rows!
```

**After:**
```python
target_df = spark.read.table(target_table_fqn)

# For append_only mode, deduplicate to latest state before comparison
if cdc_mode == "append_only":
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F
    
    print("\n📝 Mode: APPEND_ONLY - Deduplicating to latest state per key...")
    
    timestamp_col = "_cdc_timestamp" if "_cdc_timestamp" in target_df.columns else "__crdb__updated"
    window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col(timestamp_col).desc())
    
    target_df_deduplicated = target_df.withColumn("_row_num", F.row_number().over(window_spec)) \
                                     .filter(F.col("_row_num") == 1) \
                                     .drop("_row_num")
    
    print(f"   ✅ Deduplicated: {target_df.count()} rows → {target_df_deduplicated.count()} rows")
    target_df = target_df_deduplicated

# Now all comparisons use deduplicated data
target_sum = get_column_sum_spark(target_df, 'ycsb_key')
```

### 2. Fixed `compare_row_by_row()` Function

Added timestamp-based ordering for append_only mode:

```python
# Get target row (latest for append_only mode)
target_filtered = target_df.filter(target_filter)

# For append_only mode, get the LATEST row by timestamp
if cdc_mode == "append_only":
    if "_cdc_timestamp" in target_df.columns:
        target_filtered = target_filtered.orderBy(F.col("_cdc_timestamp").desc())
    elif "__crdb__updated" in target_df.columns:
        target_filtered = target_filtered.orderBy(F.col("__crdb__updated").desc())

target_row = target_filtered.select(*columns_to_check).first()
```

### 3. Updated Output Messages

Added clear indicators of what's being compared:

**Cell 14 Output:**
```
📝 Mode: APPEND_ONLY - Deduplicating to latest state per key...
   ✅ Deduplicated: 48 rows → 14 rows (latest per key)

📊 Column Sums Comparison (All Fields):
--------------------------------------------------------------------------------
   Comparing Source vs Target (deduplicated to latest state per key)

✅ ycsb_key: Source=162 | Target=162
✅ field0: Source=1,540 | Target=1,540
...
```

**Diagnosis Output:**
```
ROW-BY-ROW COMPARISON (Source → Target)
================================================================================

📝 Mode: APPEND_ONLY
   Comparing source against LATEST target row (by timestamp)

✅ Key (ycsb_key=16): All columns match
✅ Key (ycsb_key=17): All columns match
```

### 4. Fixed Comparison Direction

Changed from TARGET→SOURCE to SOURCE→TARGET:
- ✅ Now checks all source rows exist in target (correct)
- ❌ Before checked target rows exist in source (backwards)

## Result

Now both Cell 14 and the diagnosis functions use **identical deduplication logic**:
- Both deduplicate by timestamp (keeping latest row per key)
- Both compare source current state vs target latest state
- Both will show consistent results

## Testing

To verify the fix works:
1. Run Cell 14 → Should show ✅ all columns match (after deduplication message)
2. Run Cell 30 Example 4 (diagnosis) → Should show ✅ all columns match
3. Both should report identical results

## Files Changed

1. `/sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb` - Cell 14
2. `/sources/cockroachdb/docs/cockroachdb_debug.py` - `compare_row_by_row()` and `diagnose_column_family_sync()`
3. `/sources/cockroachdb/docs/COCKROACHDB_DEBUG_README.md` - Documentation
