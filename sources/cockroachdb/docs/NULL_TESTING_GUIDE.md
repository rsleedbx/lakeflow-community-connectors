# NULL Testing Guide for Column Family CDC

## Overview

This guide explains how to use the new NULL testing functions in `cockroachdb_ycsb.py` to test the NULL coalescing logic in CDC ingestion, particularly for column family scenarios.

---

## The Problem: NULL Edge Cases

When testing `update_delete` mode with `multi_cf` (multiple column families), we need to ensure the CDC ingestion correctly handles these critical edge cases:

### Edge Case 1: Row with ALL NULLs in Snapshot
```
Snapshot Insert:
  Row 0: ycsb_key=0, field0='value', field1='value', field2='value',
         field3=NULL, field4=NULL, field5=NULL, ← All family 2 columns are NULL
         field6=NULL, field7=NULL, field8=NULL, field9=NULL ← All family 3 columns are NULL
```

**Why This Matters:**
- Tests if changefeed captures rows when entire column families are NULL
- Tests if CDC events are emitted for families with no data
- Verifies Delta Lake doesn't drop NULL-only columns

---

### Edge Case 2: UPDATE Sets ALL Columns to NULL
```
Snapshot Insert:
  Row 0: field3='snapshot_value', field4='snapshot_value', field5='snapshot_value'

UPDATE Workload:
  Row 0: field3=NULL, field4=NULL, field5=NULL ← UPDATE sets all to NULL

Expected in Target (after NULL coalescing):
  Row 0: field3='snapshot_value', field4='snapshot_value', field5='snapshot_value'
  ✅ Preserved! Earlier non-NULL values should be retained

Bug (without proper NULL coalescing):
  Row 0: field3=NULL, field4=NULL, field5=NULL
  ❌ Lost! VALUES replaced with NULLs from UPDATE
```

**Why This Matters:**
- Tests if NULL coalescing (`deduplicate_to_latest_state=True`) correctly preserves earlier values
- Simulates real-world scenarios where UPDATEs only touch some columns
- Verifies the `F.last(col, ignorenulls=True)` logic works correctly

---

## New Functions

### 1. `insert_ycsb_snapshot_with_random_nulls()` - Enhanced!

**New Parameter**: `force_all_null_row: bool = True`

```python
def insert_ycsb_snapshot_with_random_nulls(
    conn,
    table_name: str,
    snapshot_count: int,
    null_probability: float = 0.3,
    columns_to_randomize: Optional[List[str]] = None,
    seed: Optional[int] = None,
    force_all_null_row: bool = True  # ← NEW!
) -> bool:
```

**What It Does:**
- If `force_all_null_row=True` (default), **row 0 will have ALL randomized columns as NULL**
- Other rows will have random NULLs based on `null_probability`
- Guarantees at least one edge case row for testing

**Example:**
```python
from cockroachdb_ycsb import insert_ycsb_snapshot_with_random_nulls

conn = get_cockroachdb_connection()
try:
    insert_ycsb_snapshot_with_random_nulls(
        conn=conn,
        table_name='usertable_update_delete_multi_cf',
        snapshot_count=10,
        null_probability=0.3,
        columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
        seed=42,
        force_all_null_row=True  # ← Row 0 will have ALL these columns as NULL
    )
finally:
    conn.close()
```

**Expected Output:**
```
📊 Inserting 10 initial rows with random NULLs...
   NULL probability: 30.0%
   Columns to randomize: field3, field4, field5, field6, field7, field8, field9
   Random seed: 42 (reproducible)
   ⚠️  Row 0 will have ALL randomized columns as NULL (edge case testing)

✅ Sample data inserted with random NULLs
   Rows inserted: 10 (keys 0 to 9)

   NULL count by column:
      field0:   0 NULLs (  0.0%) [not randomized]
      field1:   0 NULLs (  0.0%) [not randomized]
      field2:   0 NULLs (  0.0%) [not randomized]
      field3:   4 NULLs ( 40.0%) [randomized]  ← Includes forced NULL from row 0
      field4:   4 NULLs ( 40.0%) [randomized]  ← Includes forced NULL from row 0
      field5:   4 NULLs ( 40.0%) [randomized]  ← Includes forced NULL from row 0
      field6:   4 NULLs ( 40.0%) [randomized]  ← Includes forced NULL from row 0
      field7:   3 NULLs ( 30.0%) [randomized]
      field8:   3 NULLs ( 30.0%) [randomized]
      field9:   4 NULLs ( 40.0%) [randomized]  ← Includes forced NULL from row 0
```

---

### 2. `run_ycsb_workload_with_random_nulls()` - NEW!

**Purpose**: Run INSERT/UPDATE/DELETE workload where UPDATEs randomly set columns to NULL.

```python
def run_ycsb_workload_with_random_nulls(
    conn,
    table_name: str,
    insert_count: int,
    update_count: int,
    delete_count: int,
    null_probability: float = 0.5,
    columns_to_randomize: Optional[List[str]] = None,
    seed: Optional[int] = None,
    force_all_null_update: bool = True  # ← Guarantees 1 UPDATE with all NULLs
) -> Dict[str, Any]:
```

**What It Does:**
- INSERTs new rows (standard, no NULLs)
- UPDATEs existing rows with **random NULLs** in specified columns
- If `force_all_null_update=True` (default), **the first UPDATE will set ALL randomized columns to NULL**
- DELETEs oldest rows (standard)

**Example:**
```python
from cockroachdb_ycsb import run_ycsb_workload_with_random_nulls

conn = get_cockroachdb_connection()
try:
    run_ycsb_workload_with_random_nulls(
        conn=conn,
        table_name='usertable_update_delete_multi_cf',
        insert_count=5,
        update_count=3,
        delete_count=2,
        null_probability=0.5,
        columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
        seed=42,
        force_all_null_update=True  # ← First UPDATE will set all these to NULL
    )
finally:
    conn.close()
```

**Expected Output:**
```
📊 Current table state:
   Min key: 0, Max key: 9, Total rows: 10

➕ Running 5 INSERTs (keys 10 to 14)...
📝 Running 3 UPDATEs with random NULLs...
   NULL probability: 50.0%
   Columns to randomize: field3, field4, field5, field6, field7, field8, field9
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

---

## Complete Testing Workflow

### Step 1: Create Table with Column Families
```python
from cockroachdb_ycsb import create_ycsb_table

conn = get_cockroachdb_connection()
try:
    create_ycsb_table(
        conn=conn,
        table_name='usertable_update_delete_multi_cf',
        column_family_mode='multi_cf'
    )
finally:
    conn.close()
```

---

### Step 2: Insert Snapshot with Forced NULL Row
```python
from cockroachdb_ycsb import insert_ycsb_snapshot_with_random_nulls

conn = get_cockroachdb_connection()
try:
    insert_ycsb_snapshot_with_random_nulls(
        conn=conn,
        table_name='usertable_update_delete_multi_cf',
        snapshot_count=10,
        null_probability=0.3,
        columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
        seed=42,
        force_all_null_row=True  # ← CRITICAL: Tests edge case
    )
finally:
    conn.close()
```

---

### Step 3: Verify Snapshot in CockroachDB
```sql
-- Check row 0 (should have all NULLs in field3-9)
SELECT * FROM usertable_update_delete_multi_cf WHERE ycsb_key = 0;

Expected:
  ycsb_key | field0          | field1          | field2          | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+-----------------+-----------------+-----------------+--------+--------+--------+--------+--------+--------+--------
         0 | snapshot_value_0_0 | snapshot_value_0_1 | snapshot_value_0_2 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL
```

---

### Step 4: Create Changefeed (with `split_column_families`)
```sql
CREATE CHANGEFEED FOR TABLE usertable_update_delete_multi_cf
INTO 'azure://...'
WITH 
    format='parquet',
    updated,
    resolved='10s',
    split_column_families  -- ← CRITICAL: Fragments column families
```

---

### Step 5: Run Workload with NULL Updates
```python
from cockroachdb_ycsb import run_ycsb_workload_with_random_nulls

conn = get_cockroachdb_connection()
try:
    run_ycsb_workload_with_random_nulls(
        conn=conn,
        table_name='usertable_update_delete_multi_cf',
        insert_count=5,
        update_count=3,
        delete_count=2,
        null_probability=0.5,
        columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
        seed=42,
        force_all_null_update=True  # ← CRITICAL: First UPDATE sets all to NULL
    )
finally:
    conn.close()
```

---

### Step 6: Verify UPDATE Set Columns to NULL
```sql
-- Check row 0 after UPDATE (should have NULLs if it wasn't deleted)
SELECT * FROM usertable_update_delete_multi_cf WHERE ycsb_key = 0;

-- Or check row 2 (if row 0 was deleted)
SELECT * FROM usertable_update_delete_multi_cf WHERE ycsb_key = 2;

Expected (if row 2 was the first update):
  ycsb_key | field0          | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+-----------------+--------+--------+--------+--------+--------+--------+--------
         2 | updated_at_1234 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL
  
  ⚠️  field3-9 are now NULL in source!
```

---

### Step 7: Ingest to Databricks with NULL Coalescing
```python
from cockroachdb_autoload import ingest_cdc_with_merge_multi_family

result = ingest_cdc_with_merge_multi_family(
    spark=spark,
    config=config,
    # ... other parameters ...
)

# The merge_column_family_fragments() function should be called with:
# deduplicate_to_latest_state=True  ← CRITICAL: Preserves earlier non-NULL values
```

---

### Step 8: Verify NULL Coalescing Worked
```python
# Read target table
target_df = spark.read.table("robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf")

# Check row 2 (or whatever row was updated with all NULLs)
display(target_df.filter("ycsb_key = 2"))

Expected in Target:
  ycsb_key | field0          | field3              | field4              | field5              | field6              | field7              | field8              | field9
  ---------+-----------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------
         2 | updated_at_1234 | snapshot_value_2_3  | snapshot_value_2_4  | snapshot_value_2_5  | snapshot_value_2_6  | snapshot_value_2_7  | snapshot_value_2_8  | snapshot_value_2_9

✅ SUCCESS: field3-9 preserved from snapshot, even though UPDATE set them to NULL!

Bug (without NULL coalescing fix):
  ycsb_key | field0          | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+-----------------+--------+--------+--------+--------+--------+--------+--------
         2 | updated_at_1234 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL
  
  ❌ FAIL: Lost original values!
```

---

## Why These Edge Cases Matter

### Real-World Scenarios

1. **Sparse Column Families**
   - Not all column families are updated in every transaction
   - Some families may remain NULL for long periods
   - Application may only populate certain families based on business logic

2. **Partial Updates**
   - Applications often UPDATE only a subset of columns
   - Other columns appear as NULL in the CDC event (because they weren't touched)
   - Without NULL coalescing, these NULLs would overwrite actual values

3. **Schema Evolution**
   - New columns added to existing tables start as NULL
   - Historical data has NULL for new columns
   - Need to ensure NULLs don't propagate incorrectly

---

## Testing Matrix

| Test Case | Snapshot | UPDATE | Expected Target | Tests |
|-----------|----------|--------|-----------------|-------|
| 1. All NULL snapshot | field3-9 = NULL | No UPDATE | field3-9 = NULL | CDC captures NULL-only rows |
| 2. All NULL UPDATE | field3-9 = 'value' | field3-9 = NULL | field3-9 = 'value' | NULL coalescing preserves values |
| 3. Mixed NULLs | field3 = 'value', field4 = NULL | field3 = NULL, field4 = 'value' | field3 = 'value', field4 = 'value' | Cross-event coalescing |
| 4. No NULLs | field3-9 = 'value' | field3-9 = 'new_value' | field3-9 = 'new_value' | Standard path still works |

---

## Summary

✅ **New Features:**
1. `force_all_null_row` parameter in `insert_ycsb_snapshot_with_random_nulls()`
2. `run_ycsb_workload_with_random_nulls()` function
3. `force_all_null_update` parameter
4. Guarantees at least one edge case in both INSERT and UPDATE phases

✅ **Testing Coverage:**
- ✅ NULL-only rows in snapshot
- ✅ UPDATE that sets all columns to NULL
- ✅ Random NULLs throughout dataset
- ✅ Mixed NULL/non-NULL scenarios

✅ **Verifies:**
- ✅ Changefeed captures NULL-only column families
- ✅ NULL coalescing logic preserves earlier values
- ✅ `deduplicate_to_latest_state=True` works correctly
- ✅ Delta Lake merge handles NULL edge cases

**Status**: ✅ Ready for comprehensive NULL testing!
