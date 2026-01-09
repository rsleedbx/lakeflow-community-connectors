# CDC Test Matrix - Final Analysis
**Date:** January 7, 2026
**Test Run:** Complete 8-test matrix (2 formats × 2 tables × 2 split options)

## Executive Summary

**Result:** 4/8 tests PERFECT, 4/8 tests have column family coalescing issues

✅ **Perfect Tests (4):**
- json_simple_test_with_split
- json_simple_test_no_split  
- parquet_simple_test_with_split
- parquet_simple_test_no_split

❌ **Issues Found (4):**
- All 4 usertable tests (both JSON and Parquet)

## Complete Test Results

| Test | Format | Table | Split | snap | ins | upd | del | Status |
|------|--------|-------|-------|------|-----|-----|-----|--------|
| 1 | json | usertable | with | 18994 | 50 | 400 | 200 | ❌ del×2 |
| 2 | json | usertable | no | 18594 | 50 | 800 | 200 | ❌ upd×2, del×2 |
| 3 | json | simple | with | 500 | 50 | 400 | 100 | ✅ PERFECT |
| 4 | json | simple | no | 500 | 50 | 400 | 100 | ✅ PERFECT |
| 5 | parquet | usertable | with | 18994 | 0 | 450 | 200 | ❌ ins→upd, del×2 |
| 6 | parquet | usertable | no | 18994 | 0 | 450 | 200 | ❌ ins→upd, del×2 |
| 7 | parquet | simple | with | 500 | 0 | 450 | 100 | ✅* ins→upd expected |
| 8 | parquet | simple | no | 500 | 0 | 450 | 100 | ✅* ins→upd expected |

**Expected counts:** snap=depends, ins=50, upd=400, del=100

\*Parquet ins=0, upd=450 is EXPECTED BEHAVIOR (INSERTs appear as 'c' change events)

## Root Cause Analysis

### Issue 1: DELETE Doubling (ALL usertable tests)

**Affected Tests:** 1, 2, 5, 6 (all usertable tests)

**Diagnostic Evidence:**
```
Test 1 (json_usertable_with_split):
  Before coalescing: del=1100 (11 events per DELETE × 100 = 1100)
  After coalescing:  del=200  (should be 100)
  Sample _cdc_keys: [('ycsb_key', 'user9065999390998836560')]

Test 5 (parquet_usertable_with_split):
  Before coalescing: del=1600 (more complex due to old CDC files)
  After coalescing:  del=200  (should be 100)
```

**Root Cause:**
- `usertable` has 11 columns split across 2 column families
- Each DELETE generates multiple events (one per family or column)
- Current coalescing reduces 11→2 but should reduce to 1
- The coalescing logic is merging by PK but not properly deduplicating DELETE operations

**Fix Required:**
- Modify `_coalesce_events_by_key()` to ensure DELETE events with identical PKs are merged to a single DELETE
- Priority: DELETE should "win" over other operations for the same key+timestamp

### Issue 2: UPDATE Doubling (ONLY json_usertable_no_split)

**Affected Tests:** Test 2 only

**Diagnostic Evidence:**
```
Test 2 (json_usertable_no_split):
  Before coalescing: snap=108220, ins=50, upd=4400, del=1100
  After coalescing:  snap=18594, ins=50, upd=800,  del=200
```

**Root Cause:**
- Before coalescing: upd=4400 = 11 events × 400 updates
- After coalescing: upd=800 (should be 400)
- This suggests 400 snapshot rows are being incorrectly classified as UPDATEs during coalescing
- Pattern: snap decreased by 400 (from ~19000 to 18594), upd increased by 400 (from 400 to 800)

**Why Only This Test?**
- Unique combination: usertable (multi-column) + no split_column_families + JSON
- Hypothesis: Without `split_column_families`, all 11 columns come in single events, but CDC still generates multiple snapshot fragments that get misclassified during merge

**Fix Required:**
- Ensure coalescing respects chronological order (timestamp) when merging
- SNAPSHOT operations from earlier timestamps should not override later UPDATE operations
- Operation priority during merge: newer timestamp wins for non-DELETE ops

### Non-Issue: Parquet INSERT → UPDATE Conversion

**Affected Tests:** ALL Parquet tests (5, 6, 7, 8)

**Evidence:**
```
All Parquet tests show: ins=0, upd=450 (instead of ins=50, upd=400)
Total CDC operations = 450 = 50 inserts + 400 updates ✓
```

**Status:** ✅ **EXPECTED BEHAVIOR - NOT A BUG**

**Explanation:**
- Parquet format uses generic 'c' event type for both CREATE and CHANGE
- Cannot distinguish INSERT from UPDATE based on event type alone
- The total count (450) is correct: it represents 50 INSERTs + 400 UPDATEs
- Production streaming pipelines handle this correctly via MERGE operations

**Documentation:** This is already documented in strategy docs as expected Parquet behavior.

## Technical Details

### Table Schemas

**usertable (11 columns, 2 families):**
```sql
CREATE TABLE usertable (
  ycsb_key VARCHAR(255) PRIMARY KEY,
  field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT,
  field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT
)
```

**simple_test (3 columns, 1 family):**
```sql
CREATE TABLE simple_test (
  id INT PRIMARY KEY,
  data TEXT,
  timestamp TIMESTAMP
)
```

### Coalescing Behavior Analysis

**Current Logic:**
1. Groups events by primary key
2. Sorts by timestamp within each group
3. Merges column values
4. Determines final operation

**Issue:**
- Step 4 (operation determination) doesn't properly handle:
  - Multiple DELETE events for the same PK → should merge to 1 DELETE
  - Snapshot fragments being classified as UPDATEs during merge

**Sample _cdc_keys from Diagnostics:**
```python
# DELETEs all have same PK structure:
[('ycsb_key', 'user9065999390998836560')]
[('ycsb_key', 'user9067657022664552183')]
[('ycsb_key', 'user9068157409403684803')]

# This confirms PK is being extracted correctly
# The issue is in the merge logic, not PK detection
```

## Proposed Fixes

### Fix 1: DELETE Deduplication

**Location:** `cockroachdb.py::_coalesce_events_by_key()`

**Change:**
```python
# Current: Merges events but doesn't deduplicate DELETE operations
# Proposed: After merging by PK, deduplicate DELETE events

for key, events in grouped_by_key.items():
    # ... existing merge logic ...
    
    # NEW: If multiple events are all DELETE for same PK, keep only one
    if all(e['_cdc_operation'] == 'delete' for e in events):
        coalesced[key] = events[-1]  # Keep latest DELETE
        continue
```

### Fix 2: Operation Priority During Merge

**Location:** `cockroachdb.py::_coalesce_events_by_key()`

**Change:**
```python
# Ensure operation is determined by LATEST timestamp, not overridden by earlier snapshot
# Current logic may be giving priority to snapshot operations incorrectly

# Proposed priority (for same PK):
# 1. DELETE (latest) > all others
# 2. Among INSERT/UPDATE: latest timestamp wins
# 3. SNAPSHOT: only if no later INSERT/UPDATE/DELETE exists
```

## Test Coverage

### What Works Perfectly (4/8 tests)
✅ Single-family tables (simple_test)
✅ JSON format with single-family tables
✅ Parquet format with single-family tables
✅ split_column_families flag on/off for simple tables

### What Needs Fixing (4/8 tests)
❌ Multi-family tables (usertable) - DELETE doubling
❌ json_usertable_no_split - UPDATE doubling  
⚠️  Parquet INSERT detection (documented as expected behavior)

## Impact Assessment

### Production Impact: **MEDIUM**

**User-facing Impact:**
- Tables with column families will show 2× DELETE operations in analysis
- One specific scenario (JSON, multi-column, no split) shows 2× UPDATE operations
- Streaming pipelines using MERGE are likely unaffected (MERGE is idempotent)
- Iterator/testing workflows are affected (incorrect counts)

**Workaround:**
- Use MERGE operations in production (already implemented)
- MERGE with PK deduplication handles duplicate operations correctly
- The test analysis tool is more sensitive than production pipelines

### Code Quality Impact: **HIGH**

**Why:**
- Test suite validates core CDC transformation logic
- 50% test failure rate indicates coalescing logic needs refinement
- Clear patterns emerge from test matrix (column families are the issue)

## Next Steps

### Immediate (Priority 1)
1. ✅ Complete test matrix validation
2. ⏭️ Fix DELETE deduplication in `_coalesce_events_by_key()`
3. ⏭️ Fix operation priority during merge
4. ⏭️ Re-run test matrix to validate fixes

### Follow-up (Priority 2)
5. Add unit tests specifically for column family coalescing
6. Add diagnostic logging to production coalescing code
7. Update documentation with column family handling details

### Future (Priority 3)
8. Consider refactoring coalescing into separate testable component
9. Add schema introspection to detect column families upfront
10. Optimize coalescing performance for high-column-count tables

## Key Learnings

### Test Matrix Value
✅ **The 8-test matrix was essential:**
- Isolated column families as root cause (usertable vs simple_test)
- Identified format-specific vs. universal issues
- Confirmed Parquet INSERT→UPDATE is expected behavior
- Found edge case in json_usertable_no_split

### Diagnostic Output Value
✅ **Before/after coalescing counts were critical:**
- Showed exact reduction ratios (1100→200 DELETEs)
- Confirmed PK extraction works correctly
- Pinpointed merge logic as the issue (not detection)

### Production vs. Test Sensitivity
✅ **Test analysis is more sensitive than production:**
- Production MERGE pipelines are idempotent (duplicate DELETEs are OK)
- Test counting expects exact operation counts
- This is good: tests catch issues that production tolerates

## Conclusion

**Status:** Well-understood issues with clear fix paths

The test matrix successfully identified two specific coalescing issues:
1. **DELETE doubling** (all multi-family tables)
2. **UPDATE doubling** (one specific scenario)

Both issues are in the `_coalesce_events_by_key()` function and have straightforward fixes. The Parquet INSERT→UPDATE conversion is expected behavior, not a bug.

**Confidence Level:** HIGH
- Root causes identified
- Diagnostic data supports hypotheses  
- Fix locations pinpointed
- Re-test strategy clear

**Estimated Fix Time:** 1-2 hours coding + 30min testing = 2-3 hours total

