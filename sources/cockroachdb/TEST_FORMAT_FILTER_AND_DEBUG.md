# Test Script Enhancements - Format Filter & Debug Mode

## 🎯 Changes Made

### 1. Format Filter Option

**Added command-line argument to test only specific formats:**

```bash
# Usage options:
./test_cdc_matrix.sh              # Test all formats (json + parquet) - 8 tests
./test_cdc_matrix.sh parquet      # Test only parquet - 4 tests  
./test_cdc_matrix.sh json         # Test only json - 4 tests
```

**Benefits:**
- ⚡ **Faster iteration** - Test only the format you're debugging (4 tests vs 8)
- 🎯 **Focused testing** - Verify specific fixes without waiting for all combinations
- 🧹 **Cleaner output** - Less noise when debugging a specific format

### 2. Debug Mode for Analysis

**Enabled `--debug` flag in changefeed analysis:**

```bash
# Old (no debug):
python3 changefeed_helper.py analyze-files ...

# New (with debug):
python3 changefeed_helper.py analyze-files ... --debug
```

**Debug output now shows:**
- 📁 Number of snapshot files (sequence 00000000) found
- 📁 Number of CDC files (sequence 00000001+) found  
- 📄 Sample snapshot filename
- 📄 Sample CDC filename
- ⏰ Max timestamp from each snapshot file
- ✅ Final snapshot cutoff timestamp used for classification

### 3. Removed Obsolete Warning

**Removed this misleading warning:**
```bash
# OLD (WRONG):
⚠️  NOTE: Parquet uses 'c' events for BOTH snapshots AND updates (indistinguishable)
```

**Why removed:**
- This was true BEFORE the timestamp-based fix
- Now we CAN distinguish them using timestamp analysis
- The warning was confusing and no longer accurate

---

## 🐛 Current Issue Being Debugged

### Problem

**usertable** tests show incorrect results:
```
Test 5/8: parquet_usertable_with_split
  snap=9494 ins=0 upd=0 del=100  ← WRONG (should be snap=9094, upd=400)

Test 6/8: parquet_usertable_no_split  
  snap=9494 ins=0 upd=0 del=100  ← WRONG (should be snap=9094, upd=400)
```

**simple_test** works correctly:
```
Test 7/8: parquet_simple_test_with_split
  snap=500 ins=0 upd=400 del=100  ← CORRECT!

Test 8/8: parquet_simple_test_no_split
  snap=500 ins=0 upd=400 del=100  ← CORRECT!
```

### Hypothesis

The timestamp-based logic works for **simple_test** but not **usertable**. Possible reasons:

1. **Filename parsing issue** - Maybe usertable files have a different naming pattern?
2. **Timestamp comparison issue** - Maybe usertable timestamps aren't being compared correctly?
3. **Snapshot cutoff issue** - Maybe the cutoff isn't being captured for usertable?

### Debug Strategy

With the new debug output, we'll see:

```
📊 Analyzing changefeed data...
   Found 7 snapshot files (sequence 00000000)
   Found 1 CDC files (sequence 00000001+)
   Sample snapshot file: 202601070255040508466230000000000-9bf8f51fc303997f-1-31556-00000000-test_parquet_usertable_with_split-1.parquet
   Sample CDC file: 202601070256412977932510000000001-9bf8f51fc303997f-1-31556-00000001-test_parquet_usertable_with_split-1.parquet
   File 202601070255040508466230000000000-9bf8f51fc303997f... has max timestamp: 1736242504508466230.0000000000
   File 202601070255040717891730000000000-9bf8f51fc303997f... has max timestamp: 1736242504717891730.0000000000
   ...
   ✅ Snapshot cutoff timestamp: 1736242504717891730.0000000000
```

This will help us diagnose:
- ✅ Are snapshot files being identified correctly? (sequence 00000000)
- ✅ Is the cutoff timestamp being extracted?
- ✅ Are update timestamps being compared to the cutoff?

---

## 🧪 Testing Instructions

### Run Parquet Tests Only (Faster)

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh parquet
```

**Expected:**
- 4 tests total (usertable + simple_test, with/without split)
- ~8-10 minutes (vs ~15-20 for all formats)
- Debug output for each analysis

### Watch for Debug Output

Look for this section in each test:

```
📊 Analyzing changefeed data...
   Found X snapshot files (sequence 00000000)
   Found Y CDC files (sequence 00000001+)
   Sample snapshot file: ...
   ✅ Snapshot cutoff timestamp: ...
   
📊 CDC Operation Statistics:
  Snapshot rows: XXXX
  Insert rows: 0
  Update rows: YYYY  ← Should be 400 for all tests!
  Delete rows: 100
```

### Expected Results After Fix

**All Parquet tests should show:**

| Test | Snapshot | Insert | Update | Delete |
|------|----------|--------|--------|--------|
| usertable_with_split | 9094 | 0 | **400** ✅ | 100 |
| usertable_no_split | 9094 | 0 | **400** ✅ | 100 |
| simple_test_with_split | 500 | 0 | **400** ✅ | 100 |
| simple_test_no_split | 500 | 0 | **400** ✅ | 100 |

**Current Status:**
- ✅ simple_test: Working correctly (upd=400)
- ❌ usertable: Still broken (upd=0, snap includes updates)

---

## 📝 Files Modified

### `test_cdc_matrix.sh`

1. **Added format filter** (lines 10-33)
   - Parse command-line argument
   - Validate format (json|parquet)
   - Set FORMATS array based on filter

2. **Updated test matrix** (lines 75-85)
   - Conditionally set FORMATS based on filter
   - Show filter in test header

3. **Filtered cleanup** (lines 732-750)
   - Only clean up tables for filtered format
   - Prevents unnecessary table drops

4. **Enabled debug mode** (line 569)
   - Added `--debug` flag to analyze-files command

5. **Show debug output** (line 575)
   - Display debug lines (indented with "   ")

6. **Removed obsolete warning** (lines 611-613)
   - Removed "indistinguishable" warning for Parquet

### `cockroachdb.py`

1. **Enhanced debug output** (lines 2866-2905)
   - Show snapshot vs CDC file counts
   - Show sample filenames
   - Show timestamp from each file
   - Show final cutoff timestamp

---

## 🔄 Next Steps

1. ⏳ **Let current test complete** - Running with debug output
2. 🔍 **Analyze debug output** - See why usertable differs from simple_test
3. 🔧 **Fix root cause** - Based on debug findings
4. ✅ **Verify fix** - Re-run with `./test_cdc_matrix.sh parquet`

---

## 📊 Current Test Status

**Running:** `./test_cdc_matrix.sh parquet`

**Expected duration:** ~8-10 minutes

**Watch terminal for:**
- Debug output during analysis
- CDC Operation Statistics for each test
- Comparison between usertable (broken) vs simple_test (working)

---

**Date:** 2026-01-07  
**Status:** 🧪 **DEBUGGING IN PROGRESS**  
**Goal:** Fix usertable update counting (should show upd=400, currently shows upd=0)


