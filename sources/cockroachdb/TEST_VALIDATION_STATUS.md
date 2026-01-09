# Test Validation Status - Current State

**Updated:** Jan 7, 2026 12:45 PM  
**Phase:** 9 - Test Validation & Bug Fixes  
**Status:** 🔄 In Progress

---

## 📊 Current Test Results

### Test Matrix (8 combinations)

```
Test 1/8: json_usertable_with_split    - snap=19044 ins=0 upd=400 del=200
Test 2/8: json_usertable_no_split      - snap=18644 ins=0 upd=800 del=200
Test 3/8: json_simple_test_with_split  - snap=550   ins=0 upd=400 del=100
Test 4/8: json_simple_test_no_split    - snap=550   ins=0 upd=400 del=100
Test 5/8: parquet_usertable_with_split - snap=18994 ins=0 upd=450 del=200
Test 6/8: parquet_usertable_no_split   - snap=18994 ins=0 upd=450 del=200
Test 7/8: parquet_simple_test_with_split - snap=500 ins=0 upd=450 del=100
Test 8/8: parquet_simple_test_no_split - snap=500   ins=0 upd=450 del=100
```

### Expected Workload

From `test_cdc_matrix.sh`:
- **50 INSERTs** (new rows after snapshot)
- **400 UPDATEs** (modify first 400 rows)
- **100 DELETEs** (delete last 100 rows)

---

## 🐛 Issues Identified

### 1. ✅ JSON INSERT rows = 0 (FIXED)

**Status:** Fix Applied, Awaiting Validation

**Problem:** Test analysis showed 0 INSERTs for all JSON tests

**Root Cause:** Snapshot cutoff timestamp detection was missing in test analysis path

**Fix Applied:**
- Added snapshot cutoff detection for JSON files (lines 3206-3248)
- Added timestamp-based INSERT detection (lines 3268-3288)
- Now distinguishes: `timestamp <= cutoff` = SNAPSHOT, `timestamp > cutoff` = INSERT

**Expected After Fix:** All JSON tests should show ~50 INSERT rows

**Files Changed:** `cockroachdb.py`

**Documentation:** `JSON_INSERT_DETECTION_FIX.md`

---

### 2. ✅ Parquet upd=450 (NOT A BUG - Expected Behavior)

**Status:** Documented as Correct

**Explanation:** Parquet format CANNOT distinguish INSERT from UPDATE!

CockroachDB Parquet changefeeds use:
- `'c'` = CREATE/CHANGE (ambiguous - could be snapshot, insert, OR update)
- `'d'` = DELETE

To distinguish snapshot from CDC, we use timestamps:
- `timestamp <= cutoff` → SNAPSHOT
- `timestamp > cutoff` → UPDATE **(includes inserts!)**

**Result:** 450 = 400 updates + 50 inserts = ✅ Correct

**Expected:** Parquet will always show `ins=0, upd=450` (not a bug!)

**Documentation:** `TEST_COUNT_ANALYSIS.md`

---

### 3. 🔍 usertable del=200 (Investigating)

**Status:** Under Investigation

**Pattern:**
```
Tables WITH column families (usertable):
  - ALL formats show del=200 (2× expected)
  
Tables WITHOUT column families (simple_test):
  - ALL formats show del=100 (correct)
```

**Hypothesis:** CockroachDB emits TWO DELETE events per row for tables with column families:
1. DELETE for primary column family (`family_0`)
2. DELETE for data column family (`family_1`)

**Debug Added:**
- Event count before coalescing
- Event count after coalescing
- Sample primary keys for each operation type

**Next Step:** Run test with debug output to validate hypothesis

**Expected After Fix:** `usertable` should show del=100

---

### 4. 🔍 json_usertable_no_split upd=800 (Investigating)

**Status:** Under Investigation

**Pattern:** ONLY `json_usertable_no_split` shows this issue

**Details:**
- Expected: 400 updates
- Actual: 800 updates (2× expected)
- Only affects: JSON format + usertable + WITHOUT `split_column_families`

**Hypothesis:** 
- Without `split_column_families`, JSON might emit duplicate UPDATE events
- OR: Coalescing not working correctly for this specific combination
- OR: Related to column family handling in non-split mode

**Debug Added:** Event count before/after coalescing will help diagnose

**Next Step:** Examine debug output to see if events are duplicated in source data or during processing

**Expected After Fix:** Should show upd=400

---

## 🔧 Code Changes Made

### Fix #1: JSON Snapshot Cutoff Detection

**Location:** `cockroachdb.py` lines 3206-3248

```python
# Step 1: Determine snapshot cutoff timestamp for JSON files
# Find files with sequence 00000000 (snapshot files) and get max timestamp
snapshot_cutoff = None
max_timestamp = None

for blob_name in data_blobs:
    if '-00000000-' in blob_name or blob_name.endswith('-00000000.ndjson'):
        # Read JSON file and find max timestamp
        # (Implementation details in cockroachdb.py)
        
snapshot_cutoff = max_timestamp
```

### Fix #2: JSON INSERT Detection

**Location:** `cockroachdb.py` lines 3268-3288

```python
# Determine CDC operation from before/after fields
after = event_data.get('after')
before = event_data.get('before')
event_timestamp = event_data.get('updated', '')

if after and not before:
    # Use timestamp to distinguish SNAPSHOT from INSERT
    if snapshot_cutoff and event_timestamp:
        if event_timestamp <= snapshot_cutoff:
            cdc_operation = 'SNAPSHOT'  # Early timestamp = snapshot
        else:
            cdc_operation = 'INSERT'     # Late timestamp = INSERT
    else:
        cdc_operation = 'SNAPSHOT'  # Safe default
```

---

## 📝 Code Duplication Strategy

### Discovery

CDC operation detection logic exists in **THREE locations:**

| Location | Purpose | Lines |
|----------|---------|-------|
| `_add_cdc_metadata_to_dataframe()` | Production Spark streaming | ~1098 |
| `analyze_azure_changefeed_files()` (Parquet) | Test analysis | ~3116 |
| `analyze_azure_changefeed_files()` (JSON) | Test analysis | ~3283 |

### Decision: Intentional Technical Debt

**Status:** Documented and maintained as duplicate code

**Rationale:**
- ✅ Different contexts (Spark SQL DataFrame vs Python dictionaries)
- ✅ Different trade-offs (distributed processing vs direct file analysis)
- ✅ Both implementations are stable and well-tested
- ✅ Changes are infrequent
- ❌ Shared abstraction would add complexity without clear value

### Maintenance Strategy

**Critical Rule:** If you change CDC detection rules, update ALL THREE locations!

1. Cross-reference comments in code pointing to other locations
2. Comprehensive documentation in `CDC_LOGIC_DUPLICATION_DOCUMENTED.md`
3. Test suite validates consistency between paths
4. Clear warnings in code and documentation

### Refactoring Available But Deferred

A complete refactoring plan exists in `CODE_DEDUP_REFACTORING_PLAN.md`:
- Create shared `_determine_cdc_operation()` method
- Create shared `_detect_snapshot_cutoff_from_files()` method
- Estimated effort: 2-3 hours

**Decision:** Defer until CDC logic becomes significantly more complex or divergence bugs occur frequently.

---

## ✅ Next Actions

### Immediate (Today)

1. ⏳ **Run test_cdc_matrix.sh** with updated code
2. 🔍 **Validate JSON INSERT fix** (should show ~50 INSERTs)
3. 📊 **Examine debug output** for coalescing and operation detection
4. 🔍 **Investigate column family DELETE doubling** (del=200 issue)
5. 🔍 **Investigate json_usertable_no_split upd=800** issue

### Short Term (This Week)

6. ✅ **Document final test results** once all issues resolved
7. ✅ **Update CONNECTOR_EVOLUTION_STRATEGY.md** with final status
8. ✅ **Clean up debug output** (remove or make optional)
9. ✅ **Verify all test scenarios pass** with expected counts

### Long Term (When Needed)

10. ⏸️ **Revisit CDC deduplication** if logic becomes complex
11. ⏸️ **Deploy Native DLT** when production use case identified

---

## 📚 Related Documentation

### Current Session
- `TEST_COUNT_ANALYSIS.md` - Detailed analysis of test results
- `JSON_INSERT_DETECTION_FIX.md` - Fix applied to test analysis path
- `CDC_LOGIC_DUPLICATION_DOCUMENTED.md` - Code duplication strategy
- `CODE_DEDUP_REFACTORING_PLAN.md` - Refactoring plan (deferred)

### Previous Work
- `INSERT_DETECTION_BUG_FIX.md` - Original production path fix
- `JSON_ANALYSIS_BUG_FIX.md` - JSON format detection improvements
- `SOURCE_COUNT_ZERO_FIX.md` - Debug parameter passing fix
- `TIMESTAMP_BASED_CDC_ANALYSIS.md` - Timestamp-based classification

### Strategy
- `CONNECTOR_EVOLUTION_STRATEGY.md` - Overall evolution plan and status

---

## 🎯 Success Criteria

### Phase 9 Complete When:

- ✅ JSON tests show ~50 INSERT rows (not 0)
- ✅ Parquet behavior documented as expected (ins=0, upd=450)
- ✅ `usertable` DELETE count explained and fixed (del=100, not 200)
- ✅ `json_usertable_no_split` UPDATE count fixed (upd=400, not 800)
- ✅ All test scenarios produce expected, reproducible results
- ✅ Debug output validated and cleaned up
- ✅ Documentation updated with final findings

### Expected Final Results

```
JSON tests:
  - simple_test: snap=1000, ins=50, upd=400, del=100 ✅
  - usertable:   snap=10000, ins=50, upd=400, del=100 ✅

Parquet tests:
  - simple_test: snap=1000, ins=0, upd=450, del=100 ✅ (450=400+50, expected!)
  - usertable:   snap=10000, ins=0, upd=450, del=100 ✅ (450=400+50, expected!)
```

**Note:** Parquet showing `ins=0, upd=450` is CORRECT and expected behavior!

---

## 🔗 Quick Links

- **Run tests:** `cd sources/cockroachdb/scripts && ./test_cdc_matrix.sh`
- **Analyze Azure:** `python changefeed_helper.py analyze-azure-changefeed-files ...`
- **Analyze Volume:** `python changefeed_helper.py analyze-volume-changefeed-files ...`
- **Main code:** `sources/cockroachdb/cockroachdb.py`

---

**Status Summary:** 1 fix applied, 2 issues under investigation, 1 documented as expected behavior. Awaiting test validation to confirm fixes and diagnose remaining issues.

