# NULL Handling Clarification

## Core Principle

**NULLs in CDC events must always be respected as potential explicit values.**

We **cannot distinguish** between:
1. **Fragment NULL**: Column is in a different fragment file (column family split)
2. **Explicit NULL**: Application UPDATE explicitly set the column to NULL

Therefore, the CDC ingestion logic must:
- ✅ Merge fragments **within the same CDC event** (same timestamp)
- ✅ Keep the **latest state as-is** (including NULLs) across different timestamps

---

## Why `deduplicate_to_latest_state=True` Is Wrong

### What It Does
```python
F.last(col, ignorenulls=True).over(Window.partitionBy(PK).orderBy(timestamp))
```

This **ignores NULLs** and finds the last non-NULL value across time.

### The Problem

**Scenario 1: Explicit NULL Update**
```
T0 (snapshot): field3='value'
T1 (UPDATE):   field3=NULL  ← Application explicitly set to NULL

With ignorenulls=True:
  Result: field3='value' (from T0)
  ❌ WRONG! Lost the explicit NULL update

With ignorenulls=False:
  Result: field3=NULL (from T1)
  ✅ CORRECT! Respects the explicit NULL
```

**Scenario 2: Column Family Fragment**
```
T1 Event (same timestamp, split into 2 fragments):
  Fragment 1: field0='updated', field3=NULL (field3 in different fragment)
  Fragment 2: field0=NULL, field3='value' (field0 in different fragment)

Within same timestamp, use first(col, ignorenulls=True):
  Result: field0='updated', field3='value'
  ✅ CORRECT! Merged fragments properly
```

**The Key**: 
- Use `ignorenulls=True` **only within the same timestamp** (merge fragments)
- Use `ignorenulls=False` **across different timestamps** (respect explicit NULLs)

---

## Correct Implementation

### For SCD Type 1 (update_delete mode)

```python
def ingest_cdc_with_merge_multi_family(...):
    # Step 1: Merge fragments WITHIN same CDC event
    staging_df_merged = merge_column_family_fragments(
        staging_df_raw,
        primary_key_columns,
        deduplicate_to_latest_state=False  # ← Only merges within same timestamp
    )
    
    # Step 2: Keep LATEST row (including NULLs)
    window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
    staging_df = staging_df_merged \
        .withColumn("_row_num", F.row_number().over(window_spec)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")
    
    # Result: Current state with explicit NULLs ✅
```

**Behavior**:
- Merges column family fragments
- Keeps only the latest row per key
- Respects explicit NULLs in updates
- Target matches source exactly

---

### For SCD Type 2 (append_only mode)

```python
def ingest_cdc_append_only_multi_family(...):
    # Step 1: Merge fragments WITHIN same CDC event
    merged_df = merge_column_family_fragments(
        staging_df,
        primary_key_columns,
        deduplicate_to_latest_state=False  # ← Only merges within same timestamp
    )
    
    # Step 2: NO deduplication - keep ALL events
    merged_df.write.format("delta").mode("append").saveAsTable(target_table)
    
    # Result: ALL historical events with explicit NULLs ✅
```

**Behavior**:
- Merges column family fragments
- Keeps **ALL** rows (full history)
- Respects explicit NULLs in all events
- Target has complete audit trail

---

## Examples

### Example 1: Explicit NULL Update

**Source Events**:
```
T0 (INSERT): ycsb_key=1, field3='initial_value'
T1 (UPDATE): ycsb_key=1, field3=NULL  ← Explicit NULL
```

**SCD Type 1** (update_delete):
```
Target: ycsb_key=1, field3=NULL  ← Latest state (T1)
✅ CORRECT
```

**SCD Type 2** (append_only):
```
Target Row 1: ycsb_key=1, field3='initial_value', _cdc_timestamp=T0
Target Row 2: ycsb_key=1, field3=NULL, _cdc_timestamp=T1
✅ CORRECT - Full history preserved
```

---

### Example 2: Column Family Fragments

**Source Event** (split_column_families=true):
```
T1 UPDATE (same timestamp):
  Fragment 1: ycsb_key=1, field0='updated', field3=NULL (field3 in fragment 2)
  Fragment 2: ycsb_key=1, field0=NULL, field3='value' (field0 in fragment 1)
```

**After Merging Fragments**:
```
Merged Row: ycsb_key=1, field0='updated', field3='value', _cdc_timestamp=T1
✅ CORRECT - Fragments merged within same timestamp
```

**SCD Type 1**:
```
Target: ycsb_key=1, field0='updated', field3='value'
✅ CORRECT
```

**SCD Type 2**:
```
Target Row: ycsb_key=1, field0='updated', field3='value', _cdc_timestamp=T1
✅ CORRECT
```

---

### Example 3: Mixed Scenario (Fragment + Explicit NULL)

**Source Events**:
```
T0 INSERT (snapshot):
  field0='initial', field3='initial_value'

T1 UPDATE (split into fragments):
  Fragment 1: field0='updated', field3=NULL (field3 in fragment 2)
  Fragment 2: field0=NULL, field3='new_value' (field0 in fragment 1)
  Merged: field0='updated', field3='new_value'

T2 UPDATE (explicitly sets field3 to NULL):
  field0='final', field3=NULL  ← Explicit NULL
```

**SCD Type 1** (keep latest):
```
Target: field0='final', field3=NULL
✅ CORRECT - Latest state from T2
```

**SCD Type 2** (keep all):
```
Row 1: field0='initial', field3='initial_value', _cdc_timestamp=T0
Row 2: field0='updated', field3='new_value', _cdc_timestamp=T1
Row 3: field0='final', field3=NULL, _cdc_timestamp=T2
✅ CORRECT - Full history including explicit NULL at T2
```

---

## What `deduplicate_to_latest_state=True` Was Trying to Solve (WRONG)

### The Incorrect Assumption

Someone thought: "Column family fragments create NULLs, so let's fill them from history!"

```python
# WRONG LOGIC:
F.last(field3, ignorenulls=True)  # "Fill NULLs from history"
```

### Why It's Wrong

This assumes **ALL NULLs are fragment artifacts**, but:
- ❌ Some NULLs are **explicit values** from UPDATEs
- ❌ We **cannot distinguish** fragment NULLs from explicit NULLs
- ❌ The timestamp is **the same** for all fragments in an event

### The Correct Approach

**Merge fragments by timestamp**, not by ignoring NULLs across time:

```python
# CORRECT LOGIC:
# Within same timestamp: Merge fragments
merge_column_family_fragments(df, pk, deduplicate_to_latest_state=False)
  → Groups by (PK + timestamp + operation)
  → Uses first(col, ignorenulls=True) within group
  → Result: Fragments merged, explicit NULLs preserved

# Across different timestamps: Keep latest
Window.partitionBy(PK).orderBy(timestamp.desc())
  → row_number() = 1
  → Result: Latest state (including explicit NULLs)
```

---

## Summary Table

| Scenario | Within Same Timestamp | Across Different Timestamps |
|----------|----------------------|----------------------------|
| **Fragment NULL** (in different file) | Merge with `ignorenulls=True` ✅ | N/A (fragments have same timestamp) |
| **Explicit NULL** (UPDATE set to NULL) | Keep as-is ✅ | Keep latest (including NULL) ✅ |
| **Non-NULL value** | Merge ✅ | Keep latest ✅ |

**Key Insight**: Fragment NULLs and explicit NULLs have **different timestamps**, so we can handle them correctly:
- **Fragment NULLs**: Same timestamp → Merge
- **Explicit NULLs**: Different timestamp → Keep latest

---

## Status

✅ **Current implementation is CORRECT for both SCD Type 1 and Type 2**

The fix in `cockroachdb_autoload.py` properly:
1. Merges fragments within the same CDC event (same timestamp)
2. Keeps the latest state as-is (including explicit NULLs)
3. Works correctly for both update_delete and append_only modes

**No further changes needed!** 🎉
