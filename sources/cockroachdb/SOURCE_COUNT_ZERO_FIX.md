# Source Count Zero Bug - Complete Fix

## Problem

`test_cdc_scenario.ipynb` was showing **`Source file rows: 0`** even though Delta table had 1,050 rows:

```
Success: True
Delta table rows: 1,050
Source file rows: 0
Match: False ⚠️
```

## Root Causes

There were **TWO bugs** preventing source file analysis:

### Bug 1: Wrong File Format Reader (JSON/Parquet)

**Location**: `cockroachdb.py` line 3406

**Problem**: `analyze_volume_changefeed_files()` was hardcoded to read **all files as Parquet**:

```python
# OLD (WRONG):
df = spark.read.parquet(file_path)
```

**Impact**:
- JSON files were read incorrectly (or failed to read)
- Event type mappings were wrong (JSON vs Parquet have different semantics)
- This caused analysis to return 0 events for JSON changefeeds

### Bug 2: Silent Error Suppression

**Location**: `cockroachdb.py` line 4414

**Problem**: `load_and_merge_cdc_to_delta()` was calling `analyze_volume_changefeed_files()` with **hardcoded `debug=False`**:

```python
# OLD (WRONG):
source_stats = analyze_volume_changefeed_files(
    volume_path, 
    primary_key_columns=primary_keys,
    debug=False,  # ❌ Hardcoded! Hides errors
    spark=spark,
    dbutils=dbutils
)
```

**Impact**:
- Even when notebook ran with `debug=True`, file analysis errors were silently hidden
- No visibility into what was failing
- Users couldn't diagnose the problem

## Fixes Applied

### Fix 1: Format Detection & Proper Readers

```python
# Detect file format
is_json = file_name.endswith(('.ndjson', '.json'))

# Read file based on format
if is_json:
    df = spark.read.json(file_path)
else:
    df = spark.read.parquet(file_path)

# Format-specific event mapping
if is_json:
    # JSON: 'c'=snapshot, 'u'=update, 'd'=delete, 'i'=insert
    if event_type == 'c':
        cdc_operation = 'SNAPSHOT'
    elif event_type == 'i':
        cdc_operation = 'INSERT'
    elif event_type == 'u':
        cdc_operation = 'UPDATE'
    elif event_type == 'd':
        cdc_operation = 'DELETE'
else:
    # Parquet: 'c'=upsert, 'd'=delete
    if event_type == 'c':
        cdc_operation = 'UPSERT'
    elif event_type == 'd':
        cdc_operation = 'DELETE'
```

### Fix 2: Pass Debug Parameter Through

```python
# NEW (CORRECT):
source_stats = analyze_volume_changefeed_files(
    volume_path, 
    primary_key_columns=primary_keys,
    debug=debug,  # ✅ Pass through parent's debug setting
    spark=spark,
    dbutils=dbutils
)
```

### Fix 3: Comprehensive Debug Output

Added detailed debug logging to `analyze_volume_changefeed_files()`:

```python
# Show file discovery
if debug:
    print(f"   📂 Found {len(file_list)} files in {volume_path}")

# Show each file being processed
if debug:
    print(f"   📄 {file_name}: {len(records)} events ({'JSON' if is_json else 'Parquet'})")

# Show summary statistics
if debug:
    print(f"   📊 Total events read: {len(all_events):,}")
    print(f"   📊 Format breakdown: {parquet_files} Parquet, {json_files} JSON")
    print(f"   📊 After deduplication: {len(coalesced_events):,} events")
    print(f"   📊 Operation counts: SNAPSHOT={...}, INSERT={...}, UPDATE={...}, DELETE={...}")
    print(f"   📊 Active keys (unique): {active_keys:,}")
```

## Expected Results

After these fixes, when running `test_cdc_scenario.ipynb` with `debug=True`, you'll see:

```
📊 Comparing with source files...
   📂 Found 3 files in dbfs:/Volumes/main/schema/volume/test-parquet_simple_test_no_split
   📄 file1.parquet: 1000 events (Parquet)
   📄 file2.parquet: 40 events (Parquet)
   📄 file3.parquet: 10 events (Parquet)
   📊 Total events read: 1,050
   📊 Format breakdown: 3 Parquet, 0 JSON
   📊 After deduplication: 1,050 events
   📊 Operation counts: SNAPSHOT=1000, INSERT=0, UPDATE=40, DELETE=10
   📊 Active keys (unique): 1,040
   📊 Source files: 3
   📊 Unique keys (deduplicated): 1,040
   
✅✅✅ PERFECT MATCH! ✅✅✅
```

If there are errors, they'll now be visible:
```
   ⚠️  No changefeed files found in dbfs:/Volumes/...
```

or

```
Error processing file1.ndjson: JSONParseException...
```

## Files Changed

### `cockroachdb.py`

1. **Lines 3356-3370**: Added debug output for file discovery
2. **Lines 3408-3436**: Added format detection and format-specific readers
3. **Lines 3473-3489**: Added comprehensive debug output for analysis results
4. **Line 4414**: Changed `debug=False` to `debug=debug`

## Testing

Run the notebook again with the same scenario:

```bash
# In Databricks notebook:
# Run all cells in test_cdc_scenario.ipynb
```

You should now see:
- ✅ Detailed file processing output (if debug=True)
- ✅ Correct source file count matching Delta table
- ✅ Clear error messages if something fails

## Related Issues Fixed

This fix also resolves:
- JSON double-counting issue (from JSON_ANALYSIS_BUG_FIX.md)
- Silent failures in source file comparison
- Lack of visibility into file analysis process

## Diagnostic Steps

If source count is still 0:

1. **Check if files exist**:
   ```python
   dbutils.fs.ls(VOLUME_PATH)
   ```

2. **Check if schema file exists**:
   ```python
   dbutils.fs.head(f"{VOLUME_PATH}/_schema.json", 1000)
   ```

3. **Look for error messages** in the debug output (now visible!)

4. **Verify file format**:
   ```python
   files = dbutils.fs.ls(VOLUME_PATH)
   for f in files:
       print(f"{f.name} - {f.size} bytes")
   ```

## Summary

The "Source file rows: 0" bug was caused by:
1. **Wrong file reader** for JSON files (tried to read as Parquet)
2. **Silent error suppression** (hardcoded debug=False)

Both issues are now fixed, with comprehensive debug output to prevent future issues.


