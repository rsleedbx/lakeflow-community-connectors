# Run Diagnostic for Column Family Issues

**Purpose:** Determine root cause of DELETE and UPDATE doubling issues

---

## 🚀 Quick Start

### Step 1: Run Test with New Debug Output

```bash
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

---

## 🔍 What to Look For

### For DELETE Doubling Issue

Look for output like this in tests 1, 2, 5, and 6 (usertable tests):

```
📍 Before coalescing: 101,220 events
   Operations before: snap=X, ins=Y, upd=Z, del=??? 
   Sample DELETE _cdc_keys: [[('ycsb_key', 'user1')], [('ycsb_key', 'user1')], [('ycsb_key', 'user2')]]
📍 After coalescing: 19,644 events
   Operations after: snap=X, ins=Y, upd=Z, del=???
```

**Key Questions:**

1. **What is `del=` before coalescing?**
   - If `del=200` before → CockroachDB is emitting 2× DELETE events (confirmed)
   - If `del=100` before → Issue is elsewhere

2. **What is `del=` after coalescing?**
   - If `del=100` after → Coalescing IS working! ✅
   - If `del=200` after → Coalescing is NOT merging DELETE events ❌

3. **Are Sample DELETE _cdc_keys identical?**
   - If `[('ycsb_key', 'user1')]` appears twice → Same key, should be merged
   - If keys are different → That's why they're not merging

---

### For UPDATE Doubling Issue

Look for output like this in test 2 (json_usertable_no_split):

```
📍 Before coalescing: 103,220 events
   Operations before: snap=???, ins=50, upd=???, del=200
📍 After coalescing: 19,644 events
   Operations after: snap=???, ins=50, upd=800, del=200
```

**Key Questions:**

1. **What is `upd=` before coalescing?**
   - If `upd=800` before → Events are classified wrong from the start
   - If `upd=400` before and `upd=800` after → Coalescing is creating duplicates!

2. **What is `snap=` before vs after?**
   - Compare with Test 1 (with_split)
   - If snapshot count changes → Coalescing might be misclassifying operations

---

## 📊 Expected Diagnostic Output Patterns

### Pattern A: DELETE events not being coalesced

```
Before: del=200  (2× DELETE events from column families)
After:  del=200  (NOT merged)
Sample keys: [('ycsb_key', 'user1')], [('ycsb_key', 'user1')]  (SAME key!)
```

**Diagnosis:** Coalescing logic is broken - events with same key not being merged

**Fix:** Check coalescing grouping logic at lines 832-837

---

### Pattern B: DELETE events have different keys

```
Before: del=200
After:  del=200
Sample keys: [('ycsb_key', 'user1')], [('ycsb_key', 'user2')]  (DIFFERENT keys)
```

**Diagnosis:** _cdc_key construction is inconsistent

**Fix:** Check _cdc_key building at lines 3149-3153 (Parquet) and 3345-3349 (JSON)

---

### Pattern C: DELETE events being coalesced correctly

```
Before: del=200
After:  del=100  (Merged successfully!)
```

**Diagnosis:** Coalescing works, but counting is wrong

**Fix:** Check counting logic at lines 3189-3190

---

### Pattern D: UPDATE misclassification (json_usertable_no_split)

```
Test 1 (with_split):
  Before: snap=19000, upd=400
  After:  snap=18994, upd=400

Test 2 (no_split):
  Before: snap=19000, upd=400
  After:  snap=18594, upd=800
```

**Diagnosis:** Coalescing is changing operation types incorrectly

**Fix:** Check merge logic at lines 850-855 - `_cdc_operation` field being overwritten wrong

---

## 🔧 Based on Results, Apply Fix

### If Pattern A (coalescing not working):

```python
# Fix at line 836-837
pk_values = tuple(val for col, val in sorted(cdc_key) if col in pk_columns)
# Add debug:
if debug and cdc_operation == 'DELETE':
    print(f"DEBUG: Grouping DELETE by pk_values={pk_values}")
```

### If Pattern B (keys different):

```python
# Fix at lines 3149-3153
cdc_key_pairs = []
for pk_col in sorted(primary_key_columns):
    if pk_col in record:
        cdc_key_pairs.append((pk_col, record[pk_col]))
    else:
        # ADD: Log missing PK
        if debug:
            print(f"WARNING: PK column {pk_col} missing from {cdc_operation} event")
```

### If Pattern C (counting wrong):

```python
# Fix at lines 3189-3190
elif operation == 'DELETE':
    total_stats['delete'] += 1
    # ADD: Verify we're counting coalesced events
    if debug and total_stats['delete'] <= 3:
        print(f"DEBUG: Counting DELETE #{total_stats['delete']}, key={event.get('_cdc_key')}")
```

### If Pattern D (operation overwrite):

```python
# Fix at lines 850-855 in _coalesce_events_by_key()
# Take last non-null value for each field
operation_by_timestamp = {}
for event in key_events:
    timestamp = event.get('_cdc_updated')
    operation = event.get('_cdc_operation')
    if timestamp and operation:
        operation_by_timestamp[timestamp] = operation
    
    for field, value in event.items():
        if value is not None:
            merged[field] = value

# Use chronologically last operation
if operation_by_timestamp:
    last_timestamp = max(operation_by_timestamp.keys())
    merged['_cdc_operation'] = operation_by_timestamp[last_timestamp]
```

---

## ✅ Success Criteria

After applying fix and re-running test:

```
Test 1/8: json_usertable_with_split
  Delete rows: 100 ✅ (was 200)

Test 2/8: json_usertable_no_split  
  Update rows: 400 ✅ (was 800)
  Delete rows: 100 ✅ (was 200)

Test 5/8: parquet_usertable_with_split
  Delete rows: 100 ✅ (was 200)

Test 6/8: parquet_usertable_no_split
  Delete rows: 100 ✅ (was 200)

Tests 3, 4, 7, 8: Still correct (unchanged)
```

---

## 📝 Document Results

After test run, document in `COLUMN_FAMILY_FIXES.md`:
1. Which pattern was observed
2. What fix was applied
3. Test results after fix
4. Any remaining issues

---

**Next Action:** Run `./test_cdc_matrix.sh` and examine the debug output! 🚀

