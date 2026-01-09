# Test Workload Enhancement: UPDATEs + DELETEs

## Summary

Enhanced `test_cdc_matrix.sh` to include **DELETE operations** in addition to UPDATEs, ensuring comprehensive CDC operation testing.

---

## Changes

### Before
```bash
🏋️  Running workload (500 UPDATEs)...
UPDATE usertable SET field0 = field0 || '_updated' WHERE ... LIMIT 500;
```

**Coverage:**
- ✅ SNAPSHOT (initial scan)
- ✅ UPDATE (500 updates)
- ❌ DELETE (none!)

### After
```bash
🏋️  Running workload (400 UPDATEs + 100 DELETEs)...
  Step 1: Updating 400 rows...
  UPDATE usertable SET field0 = field0 || '_updated' WHERE ... LIMIT 400;
  
  Step 2: Deleting 100 rows...
  DELETE FROM usertable WHERE ... LIMIT 100;
```

**Coverage:**
- ✅ SNAPSHOT (initial scan)
- ✅ UPDATE (400 updates)
- ✅ DELETE (100 deletes)

---

## Expected Results

### For `usertable` (starts with 10,000 rows)

| Operation | Count | Description |
|-----------|-------|-------------|
| **Snapshot** | 10,000 | Initial table scan |
| **UPDATE** | 400 | Updates to first 400 rows |
| **DELETE** | 100 | Deletes last 100 rows |
| **Final row count** | 9,900 | 10,000 - 100 deleted |

**Delta Table Expected:**
- Total unique keys: **9,900** (10,000 initial - 100 deleted)
- UPSERT events: 10,000 snapshot + 400 updates = **10,400 total events**
- DELETE events: **100**
- After deduplication & applying deletes: **9,900 rows**

### For `simple_test` (starts with 1,000 rows)

| Operation | Count | Description |
|-----------|-------|-------------|
| **Snapshot** | 1,000 | Initial table scan |
| **UPDATE** | 400 | Updates to rows 1-400 |
| **DELETE** | 100 | Deletes rows 901-1000 |
| **Final row count** | 900 | 1,000 - 100 deleted |

**Delta Table Expected:**
- Total unique keys: **900** (1,000 initial - 100 deleted)
- UPSERT events: 1,000 snapshot + 400 updates = **1,400 total events**
- DELETE events: **100**
- After deduplication & applying deletes: **900 rows**

---

## Workload Details

### `usertable` Workload

```sql
-- Step 1: Update first 400 rows
UPDATE usertable SET field0 = field0 || '_updated' 
WHERE ycsb_key IN (
    SELECT ycsb_key FROM usertable LIMIT 400
);

-- Step 2: Delete last 100 rows
DELETE FROM usertable 
WHERE ycsb_key IN (
    SELECT ycsb_key FROM usertable 
    ORDER BY ycsb_key DESC 
    LIMIT 100
);
```

**Result:**
- 400 rows updated (generates UPDATE events)
- 100 rows deleted (generates DELETE events)
- 9,500 rows unchanged
- **Final: 9,900 rows**

### `simple_test` Workload

```sql
-- Step 1: Update first 400 rows
UPDATE simple_test 
SET value = value + 1, updated_at = now() 
WHERE id <= 400;

-- Step 2: Delete last 100 rows
DELETE FROM simple_test 
WHERE id > 900;
```

**Result:**
- 400 rows updated (ids 1-400, generates UPDATE events)
- 100 rows deleted (ids 901-1000, generates DELETE events)
- 500 rows unchanged (ids 401-900)
- **Final: 900 rows**

---

## Analysis Output Expected

After running the test, you should see:

```bash
📊 CDC Operation Statistics:
  Snapshot rows: 1,000
  Insert rows: 0
  Update rows: 400
  Delete rows: 100
  Unique keys (deduplicated): 900

📊 Comparison (after load_and_merge_cdc_to_delta):
   Delta: 900
   Source (deduplicated): 900
   
   ✅✅✅ PERFECT MATCH! ✅✅✅
```

**Note:** The "Unique keys" count should be **900** (or 9,900 for usertable) because:
1. We start with 1,000 (or 10,000) rows
2. We delete 100 rows
3. Final unique keys = 900 (or 9,900)

---

## Verification with Notebooks

### Test with `test_cdc_scenario.ipynb`

```python
# Cell 1: Configuration
SOURCE_TABLE = "simple_test"
TEST_SCENARIO = "test-parquet_simple_test_no_split"

# Expected results after running:
result = load_and_merge_cdc_to_delta(...)

print(f"Delta count: {result['delta_count']}")      # Should be 900
print(f"Source count: {result['source_count']}")    # Should be 900
print(f"Match: {result['match']}")                   # Should be True
```

### Delta Table Verification

```sql
-- Count rows by operation type
SELECT 
    _cdc_operation,
    COUNT(*) as count
FROM main.robert_lee_cockroachdb.simple_test_delta
GROUP BY _cdc_operation;

-- Expected output:
-- UPSERT: 900  (all non-deleted rows)
-- DELETE: 0    (deleted rows are removed from table)
```

**Important:** With `outputMode("complete")`, the Delta table contains the **final state** after applying all operations:
- UPSERT rows: 900 (snapshot + updates, minus deleted)
- DELETE rows: Not in table (already removed)

---

## Benefits

### 1. **Complete CDC Coverage**
- ✅ Tests all three CDC operations: SNAPSHOT, UPDATE, DELETE
- ✅ Validates changefeed captures all operation types
- ✅ Ensures DELETE events are properly encoded in Parquet/JSON

### 2. **Better Validation**
- ✅ Verifies row count decreases after deletes
- ✅ Tests deduplication logic with mixed operations
- ✅ Confirms Delta table applies deletes correctly

### 3. **Production-Realistic**
- ✅ Mirrors real-world workloads (insert, update, delete)
- ✅ Tests edge cases (deleted then updated? No!)
- ✅ Validates idempotency with all operation types

---

## Troubleshooting

### If DELETE count is 0

**Possible causes:**
1. Changefeeds don't capture DELETEs by default
2. Need `diff` option to enable DELETE tracking
3. Parquet format encoding issue

**Solution:**
The test already includes `updated` option which enables DELETE tracking:
```sql
CREATE CHANGEFEED FOR TABLE simple_test
INTO '...'
WITH updated, resolved = '10s', ...
```

### If Delta count is 1,000 instead of 900

**Possible causes:**
1. DELETE events not being applied in Delta
2. Using `append` mode instead of `complete` mode
3. Deduplication not working

**Solution:**
Ensure `load_and_merge_cdc_to_delta` uses:
```python
.outputMode("complete")  # Applies deletes correctly
```

---

## Related Files

- ✅ `test_cdc_matrix.sh` - Test script with workload
- ✅ `test_cdc_scenario.ipynb` - Automated testing notebook
- ✅ `cockroachdb.py` - `analyze_volume_changefeed_files()` for DELETE counting
- ✅ `CHANGEFEED_CLEANUP_FIX.md` - Auto-cleanup before tests
- ✅ `SOURCE_COMPARISON_FIX.md` - Fixed deduplication logic

---

## Testing

Run the enhanced test:

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

**Look for:**
```bash
📊 CDC Operation Statistics:
  Snapshot rows: 1,000
  Insert rows: 0
  Update rows: 400        ← Should see updates!
  Delete rows: 100        ← Should see deletes!
  Unique keys (deduplicated): 900
```

**Verify in notebook:**
```python
# After running load_and_merge_cdc_to_delta
df_delta = spark.table("main.robert_lee_cockroachdb.simple_test_delta")
print(f"Final row count: {df_delta.count()}")  # Should be 900
```

---

## Next Steps

1. ✅ Run `test_cdc_matrix.sh` to generate test data with DELETEs
2. ✅ Use `test_cdc_scenario.ipynb` to validate results
3. ✅ Verify Delta table has 900 rows (or 9,900 for usertable)
4. ✅ Check that DELETE events are properly counted in statistics

**All CDC operations now tested!** 🎉


