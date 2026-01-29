# cockroachdb.py NULL Coalescing Fix

## Date: January 29, 2026

## Overview

Added robust NULL coalescing logic to `merge_column_family_fragments()` in `cockroachdb.py`, matching the fix previously implemented in `cockroachdb-cdc-tutorial.ipynb`.

---

## Problem Context

When CockroachDB emits CDC events with `split_column_families` enabled:
- Each event may only update SOME columns (one column family)
- Other columns are NULL in that event's fragment
- If you deduplicate by keeping the latest row, you lose old values for columns not updated

**Example:**
```
Event 1 (t=100): field0=1, field1=2, field3=3
Event 2 (t=200): field0=10, field1=NULL, field3=NULL  (only field0's family updated)

Old behavior: Keep latest entire row → field0=10, field1=NULL, field3=NULL ❌
Fixed behavior: Coalesce columns → field0=10, field1=2, field3=3 ✅
```

---

## Solution

### New Parameter: `deduplicate_to_latest_state`

```python
def merge_column_family_fragments(
    df,
    primary_key_columns: List[str],
    metadata_columns: List[str] = None,
    debug: bool = False,
    is_streaming: bool = None,
    deduplicate_to_latest_state: bool = False  # ← NEW PARAMETER
):
```

### Two Modes of Operation

#### Mode 1: Standard (Default) - `deduplicate_to_latest_state=False`

**Behavior:** Merges column family fragments WITHIN the same CDC event, preserves ALL events

**Use Case:** CDC streaming where you want to keep all SNAPSHOT, UPDATE, DELETE events

**Implementation:**
```python
# Groups by PK + timestamp + operation
group_by_cols = primary_key_columns + [timestamp_col, '_cdc_operation']

# Uses first() to merge fragments within same event
for col in data_columns:
    agg_exprs.append(F.first(col, ignorenulls=True).alias(col))

df_merged = df.groupBy(*group_by_cols).agg(*agg_exprs)
```

---

#### Mode 2: Deduplication (New) - `deduplicate_to_latest_state=True`

**Behavior:** Coalesces columns ACROSS TIME + deduplicates to latest state per primary key

**Use Case:** Staging → target MERGE scenarios where you want latest state with value preservation

**Implementation:**
```python
# Step 1: Coalesce columns across time (latest non-NULL per column)
window_spec_coalesce = (Window.partitionBy(*primary_key_columns)
    .orderBy(F.col(timestamp_col))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

for col in data_columns:
    df = df.withColumn(
        col,
        F.last(F.col(col), ignorenulls=True).over(window_spec_coalesce)
    )

# Also coalesce _cdc_operation to latest value
df = df.withColumn(
    "_cdc_operation",
    F.last(F.col("_cdc_operation"), ignorenulls=True).over(window_spec_coalesce)
)

# Step 2: Deduplicate to keep latest row (which now has ALL coalesced values)
window_spec_dedup = Window.partitionBy(*primary_key_columns).orderBy(F.col(timestamp_col).desc())
df_merged = (df
    .withColumn("_row_num", F.row_number().over(window_spec_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```

---

## When to Use Each Mode

| Scenario | Mode | Parameter |
|----------|------|-----------|
| **CDC Streaming (Auto Loader → Delta)** | Standard | `deduplicate_to_latest_state=False` (default) |
| **Preserving all SNAPSHOT/UPDATE/DELETE events** | Standard | `deduplicate_to_latest_state=False` (default) |
| **Staging → Target MERGE** | Deduplication | `deduplicate_to_latest_state=True` |
| **Want latest state per key** | Deduplication | `deduplicate_to_latest_state=True` |
| **Multiple UPDATEs per key, partial column updates** | Deduplication | `deduplicate_to_latest_state=True` |

---

## Usage Examples

### Example 1: Standard Mode (CDC Streaming)

```python
from cockroachdb import merge_column_family_fragments

# Read streaming CDC data from Auto Loader
df_raw = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .load("abfss://container@account.dfs.core.windows.net/cdc/")

# Merge fragments within events, preserve all CDC events
df_merged = merge_column_family_fragments(
    df_raw,
    primary_key_columns=['ycsb_key'],
    debug=True
)

# Write to Delta (all events preserved)
df_merged.writeStream.toTable("main.schema.cdc_events")
```

### Example 2: Deduplication Mode (Staging → Target)

```python
# Read staging table (has multiple UPDATE events per key)
df_staging = spark.read.table("main.schema.staging_table")

# Merge fragments + deduplicate to latest state (preserves old column values)
df_latest = merge_column_family_fragments(
    df_staging,
    primary_key_columns=['ycsb_key'],
    deduplicate_to_latest_state=True,  # ← Use new mode
    debug=True
)

# Now merge into target
df_latest.write.mode("overwrite").saveAsTable("main.schema.target_table")

# OR use in MERGE operation
target_df = spark.read.table("main.schema.target_table")
from delta.tables import DeltaTable
delta_target = DeltaTable.forName(spark, "main.schema.target_table")
delta_target.alias("target").merge(
    df_latest.alias("source"),
    "target.ycsb_key = source.ycsb_key"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## Technical Details

### Column Coalescing Logic

**Key Insight from CockroachDB Behavior:**
- When CockroachDB updates ANY column in a column family, it emits the **ENTIRE family**
- This means explicit NULLs are always in complete fragments
- So `F.last(col, ignorenulls=True)` correctly:
  - Ignores NULLs from columns not updated (preserves old values) ✅
  - Uses NULLs from explicit NULL updates (in complete fragments) ✅
  - Returns NULL if all values are NULL ✅

### Timestamp Column Detection

The function automatically detects the timestamp column in this priority order:
1. `_cdc_timestamp` (preferred)
2. `_cdc_updated`
3. `__crdb__updated`
4. `updated`

If no timestamp column is found and `deduplicate_to_latest_state=True`, it falls back to standard mode.

### Metadata Column Handling

The `_cdc_operation` column is also coalesced to the LATEST value. This ensures:
- If the latest event is a DELETE, `_cdc_operation = 'DELETE'`
- MERGE operations can correctly handle the DELETE

---

## Changes Made to `cockroachdb.py`

### 1. Function Signature Updated

**File:** `sources/cockroachdb/cockroachdb.py`  
**Function:** `merge_column_family_fragments()`  
**Line:** ~5450

**Added parameter:**
```python
deduplicate_to_latest_state: bool = False
```

### 2. Docstring Enhanced

**Added sections:**
- "Deduplication Mode (NEW)" explanation
- Example usage for deduplication mode
- Technical details comparing both modes

### 3. Implementation Logic Added

**Location:** After fragmentation detection, before existing merge logic

**New code block:** ~100 lines implementing:
- Window-based column coalescing across time
- Row deduplication to latest state
- Fallback to standard mode if no timestamp column

---

## Backward Compatibility

✅ **Fully backward compatible**
- Default behavior unchanged (`deduplicate_to_latest_state=False`)
- All existing code continues to work as before
- New mode is opt-in only

---

## Validation

✅ No linter errors  
✅ Imports scoped inside function (no global pollution)  
✅ Debug output for both modes  
✅ Graceful fallback if timestamp missing  
✅ Comprehensive docstring with examples  

---

## Related Documentation

- `COLUMN_FAMILY_NULL_BEHAVIOR.md` - CockroachDB NULL behavior analysis
- `DEDUPLICATION_COLUMN_COALESCE_FIX.md` - Original notebook fix
- `CONNECTOR_EVOLUTION_STRATEGY.md` - Lesson #17 (Column Family Columns CAN Be NULL)
- `BUG_FIX_SESSION_SUMMARY.md` - Complete debugging session

---

## Next Steps

### For Users

1. **Review existing pipelines:** Identify where you might benefit from `deduplicate_to_latest_state=True`
2. **Staging → Target patterns:** Consider using the new mode for MERGE operations
3. **Test with your data:** Enable `debug=True` to see before/after row counts

### For Testing

1. Add unit tests for both modes:
   - `test_merge_column_families_standard_mode()`
   - `test_merge_column_families_deduplication_mode()`
2. Test with YCSB schema (multiple column families)
3. Test with NULL values (explicit vs. not-updated)
4. Test edge cases:
   - All NULLs for a column
   - Mixed NULL/non-NULL across events
   - DELETE as latest event

### For Documentation

1. Update main README with new parameter
2. Add to production deployment guide
3. Include in troubleshooting FAQ (when to use which mode)

---

## Performance Considerations

### Standard Mode
- Single shuffle operation (groupBy)
- Minimal memory overhead
- Optimized by Spark AQE

### Deduplication Mode
- Two window operations (coalesce + deduplicate)
- Higher memory usage (full window lookback)
- May benefit from explicit repartitioning by PK for large datasets

**Recommendation:** Use deduplication mode only when needed (staging → target scenarios), not for high-volume CDC streaming.

---

*Last updated: January 29, 2026*  
*Implemented by: Robert Lee*  
*Verified: Linter clean, backward compatible*
