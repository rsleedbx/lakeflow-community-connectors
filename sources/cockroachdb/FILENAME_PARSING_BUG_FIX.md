# Filename Parsing Bug Fix

## 🐛 Bug Discovered

During test verification, discovered that the timestamp-based CDC analysis fix wasn't working because **the filename parsing was incorrect**.

---

## 🔍 Problem

### Incorrect Assumption

**Original code assumed:**
```
TIMESTAMP-JOBID-SHARD-SEQUENCE-TABLE+FAMILY-VERSION.parquet
```

**Actual format includes NODE field:**
```
TIMESTAMP-JOBID-SHARD-NODE-SEQUENCE-TABLE+FAMILY-VERSION.parquet
```

### Real Example from Test

```
202601070255040508466230000000000-9bf8f51fc303997f-1-31556-00000000-test_parquet_usertable_with_split-1.parquet
```

**Parts breakdown:**
- `parts[0]`: `202601070255040508466230000000000` ← TIMESTAMP
- `parts[1]`: `9bf8f51fc303997f` ← JOB ID
- `parts[2]`: `1` ← SHARD
- `parts[3]`: `31556` ← NODE (missed this!)
- `parts[4]`: `00000000` ← SEQUENCE ✅
- `parts[5]`: `test_parquet_usertable_with_split` ← TABLE
- `parts[6]`: `1.parquet` ← VERSION

### Bug Impact

```python
# WRONG CODE (before fix):
sequence = parts[3]  # Got "31556" (NODE) instead of "00000000" (SEQUENCE)
if sequence == "00000000":  # This NEVER matched!
    snapshot_files.append(blob_name)
```

**Result:** ALL files were classified as CDC files, so snapshot cutoff was never calculated, and timestamp-based logic never executed!

---

## ✅ Fix Applied

### Code Change (Line 2857 in `cockroachdb.py`)

**Before:**
```python
if len(parts) >= 4:
    sequence = parts[3]  # WRONG - gets NODE, not SEQUENCE
```

**After:**
```python
if len(parts) >= 5:
    sequence = parts[4]  # CORRECT - gets SEQUENCE
```

### Full Fixed Section

```python
for blob_name in data_blobs:
    # Parse filename: TIMESTAMP-JOBID-SHARD-NODE-SEQUENCE-TABLE+FAMILY-VERSION.parquet
    # Example: 202601070255040508466230000000000-9bf8f51fc303997f-1-31556-00000000-test_parquet_usertable_with_split-1.parquet
    filename = blob_name.split('/')[-1]  # Get just the filename
    parts = filename.split('-')
    
    if len(parts) >= 5:
        sequence = parts[4]  # e.g., "00000000" or "00000001"
        if sequence == "00000000":
            snapshot_files.append(blob_name)
        else:
            cdc_files.append(blob_name)
    else:
        # Can't determine from filename, treat as CDC
        cdc_files.append(blob_name)
```

---

## 🧪 Test Results

### Before Fix (Current Test Run)

```
Test 5/8: parquet_usertable_with_split
  ⚠️  NOTE: Parquet uses 'c' events for BOTH snapshots AND updates (indistinguishable)
  Snapshot rows: 9494  ← WRONG (includes 400 updates)
  Update rows: 0       ← WRONG (should be 400)
```

### After Fix (Expected Next Test Run)

```
Test 5/8: parquet_usertable_with_split
  Snapshot rows: 9094  ← CORRECT (initial data only)
  Update rows: 400     ← CORRECT (400 updates detected)
```

---

## 📝 Next Steps

1. ✅ **Bug fixed** in `cockroachdb.py` line 2857
2. ✅ **Linting passed** - zero errors
3. ⏸️ **Current test still running** (using old buggy code)
4. 🔄 **Re-run test required** after current test completes:
   ```bash
   cd sources/cockroachdb/scripts
   ./test_cdc_matrix.sh
   ```

---

## 📊 Expected Fix Verification

When re-running the test, look for these changes in **Parquet tests (5-8)**:

| Metric | Before (Bug) | After (Fixed) | Expected Change |
|--------|--------------|---------------|-----------------|
| **Snapshot rows (usertable)** | 9494 | 9094 | -400 ✅ |
| **Update rows (usertable)** | 0 | 400 | +400 ✅ |
| **Snapshot rows (simple_test)** | 900 | 500 | -400 ✅ |
| **Update rows (simple_test)** | 0 | 400 | +400 ✅ |

**JSON tests (1-4):** Should remain unchanged (already correct).

---

## 🎯 Root Cause Analysis

### Why This Bug Occurred

1. **Documentation mismatch:** The file format documentation didn't mention the NODE field
2. **No validation:** Didn't verify the parsing logic against real filenames before testing
3. **Silent failure:** The code didn't fail loudly - it just classified everything as CDC

### Lessons Learned

1. ✅ **Always test with real data** before assuming documentation is complete
2. ✅ **Add debug output** to validate intermediate steps
3. ✅ **Verify assumptions** with actual file examples from the system

---

## 📁 Files Modified

1. **`cockroachdb.py`** (line 2857)
   - Changed `parts[3]` to `parts[4]`
   - Updated comment to include NODE field
   - Changed length check from `>= 4` to `>= 5`

---

## 🔗 Related Documentation

- **`TIMESTAMP_BASED_CDC_ANALYSIS.md`** - Original implementation docs (needs update for NODE field)
- **`CDC_ANALYSIS_FIX_SUMMARY.md`** - High-level summary (needs update with bug fix info)
- **`PARQUET_SNAPSHOT_VS_CDC_DETECTION.md`** - Background on detection methods (needs update for NODE field)

---

**Date:** 2026-01-07  
**Status:** ✅ **FIXED** - Awaiting re-test  
**Bug Severity:** HIGH (prevented entire feature from working)  
**Impact:** 100% of Parquet tests were affected


