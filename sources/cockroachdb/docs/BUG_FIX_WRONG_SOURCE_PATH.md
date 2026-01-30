# Bug Fix: Wrong Source Path in Auto Loader (Append-Only Mode)

## Summary

**Bug**: All 4 `ingest_cdc_*` functions in `cockroachdb_autoload.py` had an incorrect `source_path` that included an extra `/{target_table}` suffix, causing Auto Loader to miss CDC files written by CockroachDB changefeed.

**Impact**: In append_only mode with multi-column families, 9 keys (41-49) were missing from the target table even though their CDC files existed in Azure.

**Root Cause**: Path mismatch between changefeed writer and Auto Loader reader.

---

## The Bug

### Incorrect Code (Before)

```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

This constructs a path like:
```
.../parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/
```

### Correct Code (After)

```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}"
```

This constructs a path like:
```
.../parquet/defaultdb/public/usertable_append_only_multi_cf/
```

---

## Diagnosis Evidence

### CDC Files ARE in Azure

The diagnosis showed:
```
✅ Found 27 CDC events for these keys in Azure:

📊 All 27 events for key(s) [41, 42, 43, 44, 45, 46, 47, 48, 49]:
+--------+-------------------+-------------------+-------------------+------------------+------------------------------+
|ycsb_key|field0             |field1             |field2             |__crdb__event_type|__crdb__updated               |
+--------+-------------------+-------------------+-------------------+------------------+------------------------------+
|41      |NULL               |NULL               |NULL               |c                 |1769640898890338610.0000000000|
|41      |inserted_value_41_0|inserted_value_41_1|inserted_value_41_2|c                 |1769640898890338610.0000000000|
|41      |NULL               |NULL               |NULL               |c                 |1769640898890338610.0000000000|
...
```

- All 3 column family fragments exist per key
- Timestamp is consistent across fragments
- All data columns are present

### But NOT in Target Table

The diagnosis showed:
```
❌ 9 keys in source but NOT in target:
+--------+
|ycsb_key|
+--------+
|41      |
|43      |
|44      |
|42      |
|46      |
|45      |
|49      |
|47      |
|48      |
+--------+
```

### Path Analysis

**Actual Azure CDC Path** (from diagnosis):
```
abfss://changefeed-events@cockroachcdc1768934658.dfs.core.windows.net/parquet/defaultdb/public/usertable_append_only_multi_cf
```

**Auto Loader Was Reading From** (incorrect):
```
abfss://changefeed-events@cockroachcdc1768934658.dfs.core.windows.net/parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/
```

**Auto Loader Should Read From** (corrected):
```
abfss://changefeed-events@cockroachcdc1768934658.dfs.core.windows.net/parquet/defaultdb/public/usertable_append_only_multi_cf/
```

---

## Functions Fixed

All 4 ingestion functions in `cockroachdb_autoload.py`:

1. **`ingest_cdc_append_only_single_family()`** (line 58)
2. **`ingest_cdc_with_merge_single_family()`** (line 164)
3. **`ingest_cdc_append_only_multi_family()`** (line 820)
4. **`ingest_cdc_with_merge_multi_family()`** (line 969)

---

## Changes Made

**File**: `sources/cockroachdb/docs/cockroachdb_autoload.py`

**Lines Changed**: 58, 164, 820, 969

**Before**:
```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

**After**:
```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}"
```

---

## Why This Bug Was Hidden

1. **Staging table was dropped** after successful ingestion, so diagnosis couldn't inspect it
2. **Some rows (19) were in target**, suggesting partial ingestion (likely old files in a different location)
3. **Azure diagnosis confirmed CDC files exist**, proving CockroachDB was working correctly
4. **No error messages** - Auto Loader simply returned 0 files from the wrong path

---

## Testing

### Before Fix
```
Source rows: 20
Target rows: 19 (missing keys 41-49)
```

### After Fix
Should see:
```
Source rows: 20
Target rows: 20 (all keys present)
```

### How to Verify

1. **Drop existing tables and checkpoint**:
   ```sql
   DROP TABLE IF EXISTS robert_lee.robert_lee_cockroachdb.usertable_append_only_multi_cf;
   ```
   ```python
   dbutils.fs.rm("/checkpoints/robert_lee_cockroachdb_usertable_append_only_multi_cf", True)
   ```

2. **Re-run ingestion** (Cell 14):
   ```python
   ingest_cdc_append_only_multi_family(...)
   ```

3. **Check row count**:
   ```python
   spark.read.table("robert_lee.robert_lee_cockroachdb.usertable_append_only_multi_cf").count()
   # Should show 20 (or total number of source rows)
   ```

4. **Verify missing keys are now present**:
   ```python
   df = spark.read.table("robert_lee.robert_lee_cockroachdb.usertable_append_only_multi_cf")
   df.filter(df.ycsb_key.isin([41, 42, 43, 44, 45, 46, 47, 48, 49])).show()
   # Should show 9 rows
   ```

---

## Related Files

- `cockroachdb_autoload.py` (fixed)
- `cockroachdb_debug.py` (diagnosis tool that found this)
- `cockroachdb-cdc-tutorial.ipynb` (Cell 14 calls these functions)

---

## Date

2026-01-30
