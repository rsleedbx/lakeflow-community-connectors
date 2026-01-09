# Critical Bug Fix: Unique Keys Count Including Deleted Keys

## The Bug

**Symptom:**
```bash
Pre-workload: 1000 rows
DELETE 100 rows
Post-workload: 900 rows ✅ Correct in database

But CDC Analysis shows:
  Delete rows: 100 ✅ Deletes detected
  Unique keys: 1000 ❌ WRONG! Should be 900!
```

**Root Cause:**
```python
# OLD CODE (WRONG):
total_stats['unique_keys'] = len(coalesced_events)  # Includes DELETEs!
```

The analysis was counting **ALL** coalesced events, including DELETE operations, in the `unique_keys` count. This is incorrect because deleted keys should NOT be counted in the final unique key count.

---

## Why This Is Wrong

### Logical Error
**Unique keys** should represent the count of **active/existing** keys after all operations are applied (after merging CDC events).

- Initial: 1000 keys
- DELETE 100: removes 100 keys
- **Expected unique keys: 900** ✅

But the old code counted:
- All events in `coalesced_events`: 1000 (including 100 DELETE events)
- **Wrong unique keys: 1000** ❌

### Impact
This bug made it impossible to validate that CDC merge logic was working correctly:
- Can't compare `unique_keys` with `post_workload_count` (should match!)
- Can't detect if deletes are being properly applied during merge
- False impression that records are not being deleted

---

## The Fix

### Changed Code

**Location:** `cockroachdb.py` lines 2750-2770, 2870-2890, 3045-3066

**Before (WRONG):**
```python
total_stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
for event in coalesced_events:
    operation = event.get('_cdc_operation', 'UNKNOWN')
    if operation == 'SNAPSHOT':
        total_stats['snapshot'] += 1
    elif operation == 'INSERT':
        total_stats['insert'] += 1
    elif operation == 'UPDATE':
        total_stats['update'] += 1
    elif operation == 'DELETE':
        total_stats['delete'] += 1  # Counted in delete stats...

total_stats['unique_keys'] = len(coalesced_events)  # ❌ But ALSO counted here!
```

**After (CORRECT):**
```python
total_stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
active_keys = 0  # Track non-deleted keys separately

for event in coalesced_events:
    operation = event.get('_cdc_operation', 'UNKNOWN')
    if operation == 'SNAPSHOT':
        total_stats['snapshot'] += 1
        active_keys += 1  # Count as active
    elif operation == 'INSERT':
        total_stats['insert'] += 1
        active_keys += 1  # Count as active
    elif operation == 'UPDATE':
        total_stats['update'] += 1
        active_keys += 1  # Count as active
    elif operation == 'DELETE':
        total_stats['delete'] += 1
        # Don't count deleted keys in active_keys ✅

total_stats['unique_keys'] = active_keys  # ✅ Only non-deleted keys
```

---

## Expected Results After Fix

### Test Case: 1000 initial, UPDATE 400, DELETE 100

**Before Fix (WRONG):**
```bash
Pre-workload: 1000 rows
UPDATE 400
DELETE 100
Post-workload: 900 rows

CDC Analysis:
  Snapshot rows: 900
  Update rows: 0 (Parquet limitation)
  Delete rows: 100 ✅
  Unique keys: 1000 ❌ WRONG!
  ⚠️  Note: Unique keys (1000) ≠ Expected (900 from table)
```

**After Fix (CORRECT):**
```bash
Pre-workload: 1000 rows
UPDATE 400
DELETE 100
Post-workload: 900 rows

CDC Analysis:
  Snapshot rows: 900
  Update rows: 0 (Parquet limitation)
  Delete rows: 100 ✅
  Unique keys: 900 ✅ CORRECT!
  ✅ Unique keys match post-workload count (900)
```

---

## Validation

Now we can properly validate CDC merge logic:

```bash
if [ "$post_workload_count" -ne "$unique_keys" ]; then
    echo "  ⚠️  Unique keys ($unique_keys) ≠ Expected ($post_workload_count)"
    echo "     CDC merge is not working correctly!"
else
    echo "  ✅ Unique keys match post-workload count ($post_workload_count)"
    echo "     CDC merge is working correctly!"
fi
```

**Before fix:** Always mismatched (false alarm)  
**After fix:** Matches correctly (validates merge logic works)

---

## Files Changed

1. **`cockroachdb.py:2750-2770`** - Parquet analysis (`analyze_azure_changefeed_files`)
2. **`cockroachdb.py:2870-2890`** - JSON analysis (`analyze_azure_changefeed_files`)
3. **`cockroachdb.py:3045-3066`** - Volume analysis (`analyze_volume_changefeed_files`)

All three analysis functions had the same bug - now all fixed!

---

## Testing

To verify the fix:

```bash
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

**Expected output for tests with DELETEs:**
```
Post-workload count: 900 rows (expected: 900) ✓
...
Unique keys (deduplicated): 900
✅ Unique keys match post-workload count (900)
```

---

## Impact

### What This Fixes
- ✅ Unique keys now correctly exclude deleted records
- ✅ Can validate CDC merge logic correctness
- ✅ Matches expected database state after operations
- ✅ Deterministic test validation works

### What This Doesn't Fix
- ⚠️  Parquet still can't distinguish snapshots from updates (known limitation)
- ⚠️  Counts are still approximations for split column families

---

## Related Issues

- **User Report:** "but we have deletes so the count can't be 1000"
- **Previous Doc:** `PARQUET_ANALYSIS_LIMITATION.md` (explained Parquet update issue)
- **Previous Doc:** `COUNT_MISMATCH_FIX.md` (fixed expected value calculation)

This completes the trilogy of count fixes! 🎉

---

## Summary

**Bug:** `unique_keys` included deleted keys  
**Fix:** Count only active keys (exclude DELETE operations)  
**Result:** `unique_keys` now correctly matches `post_workload_count`

**Critical for:** Validating that CDC merge logic properly applies deletes and produces correct final row counts.


