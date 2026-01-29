# Notebook Updated: NULL Testing Enabled ✅

## Summary

The `cockroachdb-cdc-tutorial.ipynb` notebook has been updated to use the new NULL testing functions, ensuring comprehensive testing of NULL edge cases in column family CDC ingestion.

---

## Changes Made

### Cell 7 (Previously Cell 8): Insert Snapshot with NULLs

**Before**: Used `insert_ycsb_snapshot()` - standard snapshot without NULLs
**After**: Uses `insert_ycsb_snapshot_with_random_nulls()` - snapshot with NULL edge cases

```python
# Insert snapshot data with NULL testing using cockroachdb_ycsb.py
from cockroachdb_ycsb import insert_ycsb_snapshot_with_random_nulls

conn = get_cockroachdb_connection()
try:
    insert_ycsb_snapshot_with_random_nulls(
        conn=conn,
        table_name=source_table,
        snapshot_count=snapshot_count,
        null_probability=0.3,  # 30% chance of NULL in snapshot
        columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
        seed=42,  # Reproducible random NULLs
        force_all_null_row=True  # Row 0 will have all randomized columns as NULL (edge case)
    )
finally:
    conn.close()
```

**What This Tests:**
- ✅ Row 0 will have ALL NULLs in field3-9 (column families 2 & 3)
- ✅ Other rows will have random NULLs (30% probability)
- ✅ Verifies changefeed captures rows with NULL-only column families

---

### Cell 10: Run Workload with NULL Updates

**Before**: Used `run_ycsb_workload()` - standard workload
**After**: Uses `run_ycsb_workload_with_random_nulls()` - workload with NULL updates

```python
# Run workload with NULL testing using cockroachdb_ycsb.py
from cockroachdb_ycsb import run_ycsb_workload_with_random_nulls

conn = get_cockroachdb_connection()
try:
    run_ycsb_workload_with_random_nulls(
        conn=conn,
        table_name=source_table,
        insert_count=insert_count,
        update_count=update_count,
        delete_count=delete_count,
        null_probability=0.5,  # 50% chance of NULL in UPDATEs
        columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
        seed=42,  # Reproducible random NULLs
        force_all_null_update=True  # First UPDATE will have all NULLs (edge case)
    )
finally:
    conn.close()
```

**What This Tests:**
- ✅ First UPDATE will set ALL field3-9 to NULL (edge case)
- ✅ Other UPDATEs will have random NULLs (50% probability)
- ✅ Verifies NULL coalescing preserves earlier non-NULL values

---

## Testing Coverage

### Edge Case 1: Row with ALL NULLs in Snapshot ✅

```
Cell 7 Output (expected):
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
      field3:   4 NULLs ( 40.0%) [randomized]  ← Includes row 0
      field4:   4 NULLs ( 40.0%) [randomized]  ← Includes row 0
      field5:   4 NULLs ( 40.0%) [randomized]  ← Includes row 0
      field6:   3 NULLs ( 30.0%) [randomized]  ← Includes row 0
      field7:   3 NULLs ( 30.0%) [randomized]  ← Includes row 0
      field8:   4 NULLs ( 40.0%) [randomized]  ← Includes row 0
      field9:   3 NULLs ( 30.0%) [randomized]  ← Includes row 0
```

**Verification in CockroachDB:**
```sql
SELECT ycsb_key, field0, field3, field4, field5, field6, field7, field8, field9
FROM usertable_update_delete_multi_cf
WHERE ycsb_key = 0;

Expected:
  ycsb_key | field0          | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+-----------------+--------+--------+--------+--------+--------+--------+--------
         0 | snapshot_value_0_0 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL
```

---

### Edge Case 2: UPDATE Sets ALL Columns to NULL ✅

```
Cell 10 Output (expected):
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

**Note**: Since row 0 and row 1 are deleted, the first UPDATE actually affects row 2 (the first surviving row after DELETEs).

**Verification in CockroachDB (after UPDATE):**
```sql
SELECT ycsb_key, field0, field3, field4, field5, field6, field7, field8, field9
FROM usertable_update_delete_multi_cf
WHERE ycsb_key = 2;

Expected (after UPDATE with all NULLs):
  ycsb_key | field0          | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+-----------------+--------+--------+--------+--------+--------+--------+--------
         2 | updated_at_1234 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL

⚠️  Source now has NULLs, but snapshot had values!
```

---

### Edge Case 3: Databricks Target Should Preserve Values ✅

After running Cell 12 (CDC ingestion with `deduplicate_to_latest_state=True`):

```python
# Check target table in Databricks
target_df = spark.read.table("robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf")
display(target_df.filter("ycsb_key = 2"))
```

**Expected Result (NULL coalescing working correctly):**
```
  ycsb_key | field0          | field3              | field4              | field5              | field6              | field7              | field8              | field9
  ---------+-----------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------
         2 | updated_at_1234 | snapshot_value_2_3  | snapshot_value_2_4  | snapshot_value_2_5  | snapshot_value_2_6  | snapshot_value_2_7  | snapshot_value_2_8  | snapshot_value_2_9

✅ SUCCESS: Values preserved from snapshot, even though UPDATE set them to NULL!
```

**Bug (if NULL coalescing is NOT working):**
```
  ycsb_key | field0          | field3 | field4 | field5 | field6 | field7 | field8 | field9
  ---------+-----------------+--------+--------+--------+--------+--------+--------+--------
         2 | updated_at_1234 | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL

❌ FAIL: Lost original values! NULL coalescing is broken.
```

---

## Configuration Details

### Snapshot Insert (Cell 7)
- **NULL Probability**: 30% for field3-9
- **Guaranteed NULL Row**: Row 0 (ycsb_key=0)
- **Seed**: 42 (reproducible)
- **Columns Affected**: field3, field4, field5, field6, field7, field8, field9 (column families 2 & 3)
- **Columns NOT Affected**: field0, field1, field2 (column family 1)

### Workload Update (Cell 10)
- **NULL Probability**: 50% for field3-9 in UPDATEs
- **Guaranteed NULL UPDATE**: First updated row (after DELETEs)
- **Seed**: 42 (reproducible)
- **Columns Affected**: field3, field4, field5, field6, field7, field8, field9 (column families 2 & 3)
- **Columns NOT Affected**: field0, field1, field2 (column family 1)

---

## Why This Configuration?

### 30% NULL in Snapshot
- Provides good mix of NULL and non-NULL values
- Ensures most rows have some data for comparison
- Tests both "has value" and "is NULL" scenarios

### 50% NULL in UPDATE
- More aggressive NULL rate to stress-test coalescing
- Increases probability of hitting edge cases
- Better simulates real-world partial updates

### field3-9 Only
- These are in column families 2 and 3 (when `multi_cf` mode is used)
- field0-2 are in column family 1 (always has values)
- Tests the specific columns that are affected by column family fragmentation

### Seed 42
- Makes tests reproducible
- Same NULL pattern every run
- Easier to debug issues

### Forced Edge Cases
- **Row 0 with all NULLs**: Tests changefeed captures NULL-only rows
- **First UPDATE with all NULLs**: Tests NULL coalescing preserves earlier values

---

## Expected Behavior

### 1. Snapshot Phase (Cell 7)
✅ CockroachDB should contain row 0 with all NULLs in field3-9
✅ Other rows should have mixed NULLs and values
✅ Changefeed should capture all rows (including NULL-only ones)

### 2. UPDATE Phase (Cell 10)
✅ Some rows will be updated with random NULLs
✅ First surviving row (e.g., row 2) will have ALL field3-9 set to NULL
✅ Changefeed should emit UPDATE events with NULLs

### 3. Ingestion Phase (Cell 12)
✅ Auto Loader should read all Parquet fragments (including NULL-only ones)
✅ `merge_column_family_fragments()` with `deduplicate_to_latest_state=True` should coalesce values
✅ Target table should preserve earlier non-NULL values despite later NULL updates

### 4. Verification Phase (Cell 14)
✅ Row-by-row comparison should show all columns match between source and target
✅ Specifically, rows that were updated with NULLs should still have original values in target

---

## Rollback (if needed)

If you want to revert to standard testing without NULLs:

### Cell 7:
```python
# Insert snapshot data using cockroachdb_ycsb.py
from cockroachdb_ycsb import insert_ycsb_snapshot

conn = get_cockroachdb_connection()
try:
    insert_ycsb_snapshot(
        conn=conn,
        table_name=source_table,
        snapshot_count=snapshot_count
    )
finally:
    conn.close()
```

### Cell 10:
```python
# Run workload using cockroachdb_ycsb.py
from cockroachdb_ycsb import run_ycsb_workload

conn = get_cockroachdb_connection()
try:
    run_ycsb_workload(
        conn=conn,
        table_name=source_table,
        insert_count=insert_count,
        update_count=update_count,
        delete_count=delete_count
    )
finally:
    conn.close()
```

---

## Next Steps

1. ✅ **Run Cell 7** - Insert snapshot with NULL edge cases
2. ✅ **Verify in CockroachDB** - Check that row 0 has all NULLs in field3-9
3. ✅ **Run Cell 10** - Run workload with NULL updates
4. ✅ **Verify in CockroachDB** - Check that some rows have NULL updates
5. ✅ **Run Cell 12** - Ingest CDC data with NULL coalescing
6. ✅ **Run Cell 14** - Verify all columns match (NULL coalescing worked!)

---

## Documentation

For more details on NULL testing, see:
- `NULL_TESTING_GUIDE.md` - Comprehensive guide to NULL testing
- `cockroachdb_ycsb.py` - Source code with docstrings
- `YCSB_MODULE_SUMMARY.md` - Overview of all YCSB functions

---

## Status

✅ **Notebook Updated with NULL Testing**

Both insert and update operations now include NULL edge case testing, ensuring the NULL coalescing fix in `cockroachdb_autoload.py` is properly validated.

**Ready to test!** 🚀
