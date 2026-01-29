# Comprehensive NULL Testing Configuration ✅

## Overview

The notebook is now configured for **maximum NULL edge case testing** with ALL fields (field0-9) randomized.

---

## Configuration

### Cell 8: Snapshot Insert
```python
insert_ycsb_snapshot_with_random_nulls(
    conn=conn,
    table_name=source_table,
    snapshot_count=snapshot_count,
    null_probability=0.3,  # 30% chance of NULL
    columns_to_randomize=['field0', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],  # ALL fields
    seed=42,  # Reproducible
    force_all_null_row=True  # Row 0 = ALL NULLs
)
```

### Cell 10: UPDATE Workload
```python
run_ycsb_workload_with_random_nulls(
    conn=conn,
    table_name=source_table,
    insert_count=insert_count,
    update_count=update_count,
    delete_count=delete_count,
    null_probability=0.5,  # 50% chance of NULL
    columns_to_randomize=['field0', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],  # ALL fields
    seed=42,  # Reproducible
    force_all_null_update=True  # First UPDATE = ALL NULLs
)
```

---

## What This Tests

### 1. Extreme Edge Case: Row with ALL NULLs

**Snapshot (Row 0):**
```
ycsb_key=0, field0=NULL, field1=NULL, field2=NULL, field3=NULL, field4=NULL, 
field5=NULL, field6=NULL, field7=NULL, field8=NULL, field9=NULL
```

**Tests:**
- ✅ Changefeed captures rows with ALL data columns NULL
- ✅ CDC events are emitted for completely empty rows
- ✅ Parquet files can handle NULL-only rows
- ✅ Auto Loader doesn't drop NULL-only rows
- ✅ Delta Lake stores NULL-only rows correctly

---

### 2. Extreme Edge Case: UPDATE Sets ALL Columns to NULL

**Before UPDATE:**
```
Row 2: field0='snapshot_value_2_0', field1='snapshot_value_2_1', ..., field9='snapshot_value_2_9'
```

**After UPDATE (first updated row):**
```
Row 2: field0=NULL, field1=NULL, field2=NULL, ..., field9=NULL
```

**Tests:**
- ✅ NULL coalescing preserves ALL original values across ALL column families
- ✅ `deduplicate_to_latest_state=True` handles extreme NULL scenarios
- ✅ `F.last(col, ignorenulls=True)` works for ALL columns, not just some

---

### 3. Primary Key Column Family NULL Handling

**Why This Matters Now:**

Previously, we didn't test field0-2 because they're in the primary key column family and "never fragment." But now we're testing:

✅ **What if the primary key family columns ARE NULL?**
- This can happen if a row is inserted with NULLs
- Or if an UPDATE explicitly sets them to NULL
- Tests if CDC ingestion handles NULL in primary key family correctly

✅ **Cross-family NULL coalescing:**
- If field0 (family 1) is NULL in UPDATE event
- But field0 had a value in snapshot
- Does coalescing work for primary key family too?

---

### 4. All Three Column Families with Random NULLs

**Column Family Distribution:**
- **Family 1**: field0, field1, field2 (primary key family)
- **Family 2**: field3, field4, field5
- **Family 3**: field6, field7, field8, field9

**With split_column_families=true:**
```
Fragment 1 (Family 1): field0=NULL, field1='value', field2=NULL, field3=NULL, field4=NULL, ...
Fragment 2 (Family 2): field0=NULL, field1=NULL, field2=NULL, field3='value', field4=NULL, field5='value', ...
Fragment 3 (Family 3): field0=NULL, ..., field6='value', field7=NULL, field8='value', field9=NULL
```

**Tests:**
- ✅ NULL coalescing works across ALL 3 families
- ✅ No family is "special" - all get equal treatment
- ✅ Mixed NULL/non-NULL patterns in all families

---

## Expected Results

### Snapshot Phase (Cell 8)

**Expected Output:**
```
📊 Inserting 10 initial rows with random NULLs...
   NULL probability: 30.0%
   Columns to randomize: field0, field1, field2, field3, field4, field5, field6, field7, field8, field9
   Random seed: 42 (reproducible)
   ⚠️  Row 0 will have ALL randomized columns as NULL (edge case testing)

✅ Sample data inserted with random NULLs
   Rows inserted: 10 (keys 0 to 9)

   NULL count by column:
      field0:   4 NULLs ( 40.0%) [randomized]  ← NOW RANDOMIZED!
      field1:   3 NULLs ( 30.0%) [randomized]  ← NOW RANDOMIZED!
      field2:   4 NULLs ( 40.0%) [randomized]  ← NOW RANDOMIZED!
      field3:   4 NULLs ( 40.0%) [randomized]
      field4:   4 NULLs ( 40.0%) [randomized]
      field5:   4 NULLs ( 40.0%) [randomized]
      field6:   3 NULLs ( 30.0%) [randomized]
      field7:   3 NULLs ( 30.0%) [randomized]
      field8:   4 NULLs ( 40.0%) [randomized]
      field9:   3 NULLs ( 30.0%) [randomized]
```

**CockroachDB Verification:**
```sql
SELECT * FROM usertable_update_delete_multi_cf WHERE ycsb_key = 0;

Expected:
  ycsb_key | field0 | field1 | field2 | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------
         0 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL
```

---

### UPDATE Phase (Cell 10)

**Expected Output:**
```
📊 Current table state:
   Min key: 0, Max key: 9, Total rows: 10

➕ Running 5 INSERTs (keys 10 to 14)...
📝 Running 3 UPDATEs with random NULLs...
   NULL probability: 50.0%
   Columns to randomize: field0, field1, field2, field3, field4, field5, field6, field7, field8, field9
   ⚠️  First updated row (key 0) will have ALL randomized columns as NULL
🗑️  Running 2 DELETEs (keys 0 to 1)...

✅ Workload complete
   Inserts: 5
   Updates: 3 (with random NULLs)
   Deletes: 2
   Before: 10 rows (keys 0-9)
   After:  13 rows (keys 2-14)
   Net change: +3 rows
```

**CockroachDB Verification (after UPDATE):**
```sql
SELECT * FROM usertable_update_delete_multi_cf WHERE ycsb_key = 2;

Expected (first UPDATE sets ALL to NULL):
  ycsb_key | field0 | field1 | field2 | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------
         2 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL

⚠️  ALL columns are now NULL in source! (Snapshot had values)
```

---

### Ingestion Phase (Cell 12)

**Expected: NULL Coalescing Should Preserve ALL Original Values**

After CDC ingestion with `deduplicate_to_latest_state=True`:

```python
target_df = spark.read.table("robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf")
display(target_df.filter("ycsb_key = 2"))
```

**Expected Result (SUCCESS):**
```
  ycsb_key | field0              | field1              | field2              | field3              | field4              | field5              | field6              | field7              | field8              | field9
  ---------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------
         2 | snapshot_value_2_0  | snapshot_value_2_1  | snapshot_value_2_2  | snapshot_value_2_3  | snapshot_value_2_4  | snapshot_value_2_5  | snapshot_value_2_6  | snapshot_value_2_7  | snapshot_value_2_8  | snapshot_value_2_9

✅ SUCCESS: ALL columns preserved from snapshot, even though UPDATE set them ALL to NULL!
✅ NULL coalescing works for ALL 10 fields across ALL 3 column families!
```

**Bug Scenario (if NULL coalescing fails):**
```
  ycsb_key | field0 | field1 | field2 | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------
         2 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL

❌ FAIL: Lost ALL original values! NULL coalescing is completely broken.
```

---

## Additional Test Scenarios

### Scenario 1: Row 0 (ALL NULLs in Snapshot, Then Deleted)

**Snapshot**: Row 0 has ALL NULLs
**UPDATE**: Doesn't matter (deleted before UPDATE)
**DELETE**: Row 0 deleted

**Tests:**
- ✅ NULL-only rows can be captured and deleted correctly
- ✅ DELETE operations work on NULL-only rows

---

### Scenario 2: Row with Partial NULLs

**Snapshot**: Row 3 might have `field0='value', field1=NULL, field2='value', field3=NULL, ...`
**UPDATE**: Row 3 might be updated with `field0=NULL, field1='value', field2=NULL, field3='value', ...`

**Expected in Target:**
```
Row 3: field0='value' (from snapshot), field1='value' (from UPDATE), 
       field2='value' (from snapshot), field3='value' (from UPDATE), ...
```

**Tests:**
- ✅ Column-by-column coalescing (not just row-level)
- ✅ Each column independently preserves latest non-NULL value
- ✅ Mixed NULL/non-NULL patterns handled correctly

---

### Scenario 3: Multiple UPDATEs with Different NULL Patterns

**Snapshot**: Row 4 has some values, some NULLs
**UPDATE 1**: Sets different columns to NULL
**UPDATE 2**: Sets different columns to NULL
**UPDATE 3**: Sets different columns to NULL

**Expected in Target:**
- ✅ Latest non-NULL value for each column across all events
- ✅ Coalescing works across multiple UPDATE events, not just snapshot + one UPDATE

---

## Why This Configuration Is Better

### Before (field3-9 only):
- ✅ Tests column family fragmentation issue
- ⚠️  Doesn't test NULL in primary key family
- ⚠️  Assumes field0-2 always have values

### After (field0-9 ALL):
- ✅ Tests column family fragmentation issue
- ✅ Tests NULL in primary key family
- ✅ Tests extreme edge case (ALL NULLs)
- ✅ Tests cross-family NULL coalescing comprehensively
- ✅ No assumptions about which fields have values

---

## Potential Issues to Watch For

### 1. Field0 as Timestamp Marker

**Problem**: If field0 is NULL, we can't tell if a row was updated.

**Solution**: The UPDATE workload will still show updated rows because:
- The UPDATE operation itself creates a CDC event
- We can check `_cdc_timestamp` metadata
- Other columns will show the UPDATE happened

### 2. All-NULL Rows Might Be Filtered

**Problem**: Some systems drop rows with all NULLs.

**Test**: This configuration specifically tests if:
- ✅ CockroachDB emits CDC events for NULL-only rows
- ✅ Auto Loader ingests NULL-only rows
- ✅ Delta Lake stores NULL-only rows
- ✅ MERGE logic handles NULL-only rows

### 3. Performance Impact

**Problem**: More NULLs might mean more fragments or different Parquet encoding.

**Test**: This configuration helps measure:
- ✅ CDC event size with many NULLs
- ✅ Parquet compression with many NULLs
- ✅ Merge performance with many NULLs

---

## Summary

✅ **Comprehensive NULL Testing Enabled**

**What's Tested:**
- ✅ ALL 10 fields (field0-9) randomized
- ✅ ALL 3 column families tested
- ✅ Extreme edge case: Row with ALL NULLs (row 0)
- ✅ Extreme edge case: UPDATE sets ALL to NULL (first updated row)
- ✅ NULL coalescing across all families
- ✅ Mixed NULL/non-NULL patterns throughout dataset

**Expected Outcome:**
- ✅ NULL coalescing preserves ALL values across ALL columns
- ✅ No data loss despite extreme NULL scenarios
- ✅ CDC ingestion handles NULL-only rows correctly

**Status**: ✅ Ready for maximum NULL stress testing! 🚀
