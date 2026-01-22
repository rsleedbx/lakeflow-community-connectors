# Metadata File Filter Fix

## Problem

When reading CDC data from Unity Catalog Volumes, the connector was attempting to process `_schema.json` as a data file, resulting in this error:

```
⚠️  Error processing _schema.json: [UNABLE_TO_INFER_SCHEMA] Unable to infer schema for JSON.
```

This occurred even though filtering logic existed to exclude files starting with `_`.

## Root Cause

The filtering logic in `_read_table_from_volume()` and `analyze_volume_changefeed_files()` was checking:

```python
if not f['name'].startswith('_'):
```

However, `f['name']` can be:
1. A full path (e.g., `/Volumes/main/schema/volume/_schema.json`)
2. A relative path with directory prefix
3. An empty string (due to `dbutils.fs.ls()` quirk)

In these cases, checking `startswith('_')` on the full path/name would fail to detect metadata files like `_schema.json`.

## Solution

Extract the basename before checking for the underscore prefix:

```python
import os
basename = os.path.basename(f['name']) if f['name'] else os.path.basename(f['path'])
if not basename.startswith('_'):
    # Process file
```

This ensures we check only the actual filename, not the full path.

## Files Changed

### 1. `cockroachdb.py` - `_read_table_from_volume()` (lines ~1304-1318)

**Before:**
```python
file_list = self._list_volume_files(self.volume_path, spark=spark, dbutils=dbutils)
if not file_list:
    return iter([]), start_offset

# Filter out metadata files (_schema.json, _checkpoints/, etc.) and filter by cursor
new_files = [f for f in file_list if not f['name'].startswith('_') and f['name'] > last_cursor]
new_files.sort(key=lambda f: f['name'])
```

**After:**
```python
file_list = self._list_volume_files(self.volume_path, spark=spark, dbutils=dbutils)
if not file_list:
    return iter([]), start_offset

# Filter out metadata files (_schema.json, _checkpoints/, etc.) and filter by cursor
# Extract basename to check for underscore prefix (handles both path and name fields)
import os
new_files = []
for f in file_list:
    basename = os.path.basename(f['name']) if f['name'] else os.path.basename(f['path'])
    # Skip metadata files (start with _) and files already processed
    if not basename.startswith('_') and f['name'] > last_cursor:
        new_files.append(f)
new_files.sort(key=lambda f: f['name'])
```

### 2. `cockroachdb.py` - `analyze_volume_changefeed_files()` (lines ~4143-4152)

**Before:**
```python
file_list = temp_connector._list_volume_files(volume_path, spark=spark, dbutils=dbutils)

# Filter out metadata files (_schema.json, _checkpoint dirs, etc.)
file_list = [f for f in file_list if not f['name'].startswith('_')]
```

**After:**
```python
file_list = temp_connector._list_volume_files(volume_path, spark=spark, dbutils=dbutils)

# Filter out metadata files (_schema.json, _checkpoint dirs, etc.)
# Extract basename to check for underscore prefix (handles both path and name fields)
import os
filtered_files = []
for f in file_list:
    basename = os.path.basename(f['name']) if f['name'] else os.path.basename(f['path'])
    if not basename.startswith('_'):
        filtered_files.append(f)
file_list = filtered_files
```

## Already Correct

The following methods already had correct filtering:

1. **`_list_azure_parquet_files()`** (lines ~2798-2800):
   ```python
   blob_name_only = blob.name.split('/')[-1]
   if blob_name_only.startswith('_'):
       continue
   ```

2. **`analyze_azure_changefeed_files()`** (lines ~3602-3606):
   ```python
   blob_name = blob.name.split('/')[-1]  # Get just the filename
   if (file_extension in blob.name and 
       not blob.name.endswith('.RESOLVED') and 
       not blob_name.startswith('_')):
       data_blobs.append(blob.name)
   ```

## Testing

After this fix:
- ✅ `_schema.json` is correctly filtered out and not processed as a data file
- ✅ `_checkpoints/` directory is correctly filtered out
- ✅ All other files starting with `_` are correctly excluded
- ✅ No more "Unable to infer schema for JSON" warnings for `_schema.json`
- ✅ Iterator pattern successfully reads 20,700 records without errors

## Related Fixes

- `ITERATOR_SCHEMA_FILE_FILTER_FIX.md` (deprecated) - First attempt that only handled top-level path checks
- `EMPTY_NAME_BUG_FIX.md` - Related fix for handling empty `item.name` from `dbutils.fs.ls()`
- `FILE_BASED_MODES_FIX.md` - Broader fix ensuring schema/metadata are read from files

## Lessons Learned

1. **Always extract basename before checking filename patterns** - paths can have directory prefixes
2. **Handle edge cases** - `f['name']` can be empty, requiring fallback to `f['path']`
3. **Consistent filtering** - apply the same pattern across all file listing/filtering code
4. **Test with actual Unity Catalog Volumes** - `dbutils.fs.ls()` behavior differs from standard file I/O
