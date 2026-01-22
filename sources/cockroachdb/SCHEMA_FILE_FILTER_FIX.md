# Schema File Filter Fix

## Issue

When running `load_and_merge_cdc_to_delta()` with `compare_source=True` and `debug=True`, the file comparison logic was attempting to read `_schema.json` as a CDC data file, causing this error:

```
Error processing _schema.json: [UNABLE_TO_INFER_SCHEMA] Unable to infer schema for JSON. 
It must be specified manually. SQLSTATE: 42KD9
```

## Root Cause

The `analyze_volume_changefeed_files()` function:
1. Lists all files in the volume directory
2. Iterates over each file to read CDC data
3. Was **not filtering out metadata files** like `_schema.json`, `_checkpoints/`, etc.

When it tried to read `_schema.json`, Spark's `read.json()` failed because:
- `_schema.json` has a custom structure (primary keys, column families)
- It doesn't match the CDC data format (before/after, key, updated)
- Spark couldn't infer a schema from it

## Solution

Added filtering logic in `analyze_volume_changefeed_files()` to exclude all files starting with underscore:

### 1. Filter File List (Line 3957)

```python
# Use shared file listing method
file_list = temp_connector._list_volume_files(volume_path, spark=spark, dbutils=dbutils)

# Filter out metadata files (_schema.json, _checkpoint dirs, etc.)
file_list = [f for f in file_list if not f['name'].startswith('_')]
```

**Why**: Removes metadata files before processing begins.

### 2. Defensive Check in Loop (Lines 4057-4061)

```python
for file_info in file_list:
    try:
        file_path = file_info['path']
        file_name = file_info['name']
        
        # Skip metadata files (defensive check, should already be filtered)
        if file_name.startswith('_'):
            if debug:
                print(f"   ⏭️  Skipping metadata file: {file_name}")
            continue
```

**Why**: Belt-and-suspenders approach ensures no metadata files are processed.

### 3. Improved Error Handling (Lines 4183-4185)

```python
except Exception as e:
    # Only print errors for actual data files (not metadata files starting with _)
    if debug and not file_info['name'].startswith('_'):
        print(f"   ⚠️  Error processing {file_info['name']}: {e}")
    continue
```

**Why**: Prevents confusing error messages for metadata files that slip through.

## Files Excluded by This Filter

All files/directories starting with underscore are now excluded from CDC analysis:

- `_schema.json` - Table metadata (primary keys, column families)
- `_checkpoints/` - Streaming checkpoint directories
- `_SUCCESS` - Spark success marker files
- `_committed_*` - Databricks commit logs
- `_started_*` - Databricks started markers
- Any other internal metadata files

## Expected Behavior After Fix

### Before:
```
📊 Comparing with source files...
   📂 Found 5 files in /Volumes/.../test-json_usertable_with_split/1769022634
   📄 file1.ndjson: 10000 events (JSON)
   📄 file2.ndjson: 10000 events (JSON)
   📄 file3.ndjson: 550 events (JSON)
   📄 file4.ndjson: 150 events (JSON)
Error processing _schema.json: [UNABLE_TO_INFER_SCHEMA] Unable to infer schema for JSON...
   📊 Total events read: 20,700
```

### After:
```
📊 Comparing with source files...
   📂 Found 4 files in /Volumes/.../test-json_usertable_with_split/1769022634
   📄 file1.ndjson: 10000 events (JSON)
   📄 file2.ndjson: 10000 events (JSON)
   📄 file3.ndjson: 550 events (JSON)
   📄 file4.ndjson: 150 events (JSON)
   📊 Total events read: 20,700
```

**Note**: 
- File count changes from 5 to 4 (excludes `_schema.json`)
- No error message
- Same total events (metadata file has no CDC data)

## Why This Pattern Works

CockroachDB CDC changefeeds and Spark both follow the convention of using underscore-prefixed names for metadata:

| File Pattern | Purpose | Should Read? |
|---|---|---|
| `202601*.ndjson` | CDC data files | ✅ Yes |
| `202601*.parquet` | CDC data files | ✅ Yes |
| `_schema.json` | Metadata | ❌ No |
| `_checkpoints/` | Streaming state | ❌ No |
| `_SUCCESS` | Job marker | ❌ No |

This is an industry-standard convention, making the filter simple and reliable.

## Testing

This fix was validated with:

1. **JSON format with column families**:
   - 4 `.ndjson` CDC files + 1 `_schema.json`
   - ✅ Reads 4 files, skips `_schema.json`, no error

2. **Parquet format**:
   - 3 `.parquet` CDC files + 1 `_schema.json`
   - ✅ Reads 3 files, skips `_schema.json`, no error

3. **Directory with checkpoints**:
   - CDC files + `_checkpoints/` directory + `_schema.json`
   - ✅ Reads only CDC files, skips all `_*` items

## Related Functions

This fix applies to:
- ✅ `analyze_volume_changefeed_files()` - Fixed (filters `_*` files)
- ✅ `analyze_azure_changefeed_files()` - Fixed (filters `_*` blobs)
- ✅ `LakeflowConnect._read_table_from_volume()` - Uses Autoloader (handles this automatically)

## Why Autoloader Doesn't Have This Issue

Databricks Autoloader (`spark.readStream.format("cloudFiles")`) automatically excludes files starting with underscore, following Spark conventions. This manual file analysis needed the same filter.

## Files Modified

### `cockroachdb.py`

**Volume Analysis (`analyze_volume_changefeed_files`):**
- ✅ Line 3960: Filter file list to exclude `_*` files
- ✅ Lines 4060-4064: Defensive skip check in loop
- ✅ Lines 4190-4192: Suppress errors for metadata files

**Volume Iterator (`_read_table_from_volume`):**
- ✅ Line 1311: Filter file list to exclude `_*` files during iteration

**Azure Analysis (`analyze_azure_changefeed_files`):**
- ✅ Lines 3414-3421: Filter blob list to exclude `_*` files

**Azure Iterator (`_list_azure_parquet_files`):**
- ✅ Lines 2794-2797: Filter blob list to exclude `_*` files during iteration

### Documentation
- ✅ `SCHEMA_FILE_FILTER_FIX.md` (this file)

## Summary

**Problem**: CDC file comparison tried to read `_schema.json` as data file.  
**Solution**: Filter out all files starting with `_` before processing.  
**Result**: Clean output, no schema inference errors, correct file counts.
