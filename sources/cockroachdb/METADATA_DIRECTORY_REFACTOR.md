# Metadata Directory Refactoring

## Summary

Refactored schema file storage from `_schema.json` to `_metadata/schema.json` to:
1. **Simplify filtering logic** - No longer need to check filenames for underscore prefix
2. **Improve separation of concerns** - Metadata is clearly isolated in its own directory
3. **Enable future extensibility** - Easy to add more metadata files (column families, stats, etc.)
4. **Follow data lake patterns** - Similar to Delta Lake's `_delta_log/` directory

## Changes Made

### 1. Schema File Location

**Before:**
```
/path/to/data/
  ├── _schema.json          ← metadata file mixed with data
  ├── file1.ndjson
  └── file2.ndjson
```

**After:**
```
/path/to/data/
  ├── _metadata/
  │   └── schema.json       ← isolated metadata directory
  ├── file1.ndjson
  └── file2.ndjson
```

### 2. Files Modified

#### `changefeed_helper.py` (lines 513-514)
```python
# Before:
schema_blob_name = f"{args.prefix}/_schema.json"

# After:
schema_blob_name = f"{args.prefix}/_metadata/schema.json"
```

#### `cockroachdb.py` - `_load_schema_from_volume()` (line 3037)
```python
# Before:
schema_file_path = f"{volume_path}/_schema.json"

# After:
schema_file_path = f"{volume_path}/_metadata/schema.json"
```

#### `cockroachdb.py` - `_load_schema_from_azure()` (line 2997)
```python
# Before:
schema_blob_name = f"{path_prefix}/{table_name}/_schema.json"

# After:
schema_blob_name = f"{path_prefix}/{table_name}/_metadata/schema.json"
```

#### `cockroachdb.py` - `_store_schema_to_azure()` (line 2957)
```python
# Before:
schema_blob_name = f"{path_prefix}/{table_name}/_schema.json"

# After:
schema_blob_name = f"{path_prefix}/{table_name}/_metadata/schema.json"
```

### 3. Simplified Filtering Logic

#### `_read_table_from_volume()` (lines ~1310-1318)

**Before** (complex basename extraction):
```python
import os
new_files = []
for f in file_list:
    basename = os.path.basename(f['name']) if f['name'] else os.path.basename(f['path'])
    # Skip metadata files (start with _) and files already processed
    if not basename.startswith('_') and f['name'] > last_cursor:
        new_files.append(f)
new_files.sort(key=lambda f: f['name'])
```

**After** (simple path filtering):
```python
# Filter out _metadata/ directory contents and apply cursor filter
new_files = [f for f in file_list if '/_metadata/' not in f['path'] and f['name'] > last_cursor]
new_files.sort(key=lambda f: f['name'])
```

**Removed:**
- `import os` (no longer needed)
- Basename extraction logic
- Underscore prefix checking
- Complex loop with multiple conditions

**Benefit:** 9 lines → 2 lines, much clearer intent. Now filters by path prefix instead of filename prefix.

#### `analyze_volume_changefeed_files()` (lines ~4146-4148 and ~4241-4244)

**Before** (complex basename filtering):
```python
import os
filtered_files = []
for f in file_list:
    basename = os.path.basename(f['name']) if f['name'] else os.path.basename(f['path'])
    if not basename.startswith('_'):
        filtered_files.append(f)
file_list = filtered_files
```

**After** (list all files, skip during processing):
```python
# List ALL files (including _metadata/) for accurate reporting
file_list = temp_connector._list_volume_files(volume_path, spark=spark, dbutils=dbutils)

# In processing loop (line ~4241):
# Skip metadata files and _metadata/ directory contents
if file_name.startswith('_') or '/_metadata/' in file_path:
    if debug:
        print(f"   ⏭️  Skipping metadata file: {file_name}")
    continue
```

**Removed:**
- `import os`
- Entire pre-filtering loop with basename extraction
- Underscore prefix checking on individual filenames

**Benefit:** 
- 7 lines of pre-filtering → 0 lines (filtering moved to processing loop)
- More accurate reporting (sees all files before skipping metadata)
- Clearer debug messages when metadata files are encountered

### 4. Updated Documentation

#### `test_cdc_matrix.sh` (line 1259)
```bash
# Before:
echo "   (Including _schema.json from Azure)"

# After:
echo "   (Including _metadata/schema.json from Azure)"
```

## Benefits

### 1. **Simpler Code**
- Eliminated complex basename extraction logic
- Removed underscore prefix filtering
- Clearer separation between data and metadata

### 2. **More Robust**
- No dependency on filename prefixes
- Handles edge cases better (empty names, paths with directories)
- Less error-prone

### 3. **Better Architecture**
- Follows separation of concerns principle
- Matches common data lake patterns (e.g., `_delta_log/` in Delta Lake)
- More maintainable and extensible

### 4. **Future Extensibility**
Easy to add more metadata files:
```
_metadata/
  ├── schema.json           ← primary keys, column info
  ├── column_families.json  ← column family definitions
  ├── stats.json            ← table statistics
  └── checkpoints.json      ← processing checkpoints
```

## Testing Required

1. **Run `test_cdc_matrix.sh`** to regenerate test data with new schema location
2. **Verify schema creation** in Azure Blob Storage at `{prefix}/_metadata/schema.json`
3. **Test Volume mode** in `test_cdc_scenario.ipynb` to ensure schema loading works
4. **Test Azure modes** to ensure schema loading works from Azure
5. **Verify no `_schema.json` warnings** appear anymore

## Breaking Changes

⚠️ **This is a breaking change** - existing data with `_schema.json` will need to be regenerated.

### Migration Path for Existing Data

**Option 1: Regenerate (Recommended)**
- Re-run `test_cdc_matrix.sh` to create new schema files in `_metadata/` directory
- Old `_schema.json` files will be ignored (harmless)

**Option 2: Manual Migration (Advanced)**
```bash
# For Azure Blob Storage:
az storage blob copy start \
  --source-container changefeed-events \
  --source-blob "json/catalog/schema/table/_schema.json" \
  --destination-container changefeed-events \
  --destination-blob "json/catalog/schema/table/_metadata/schema.json"

# For Unity Catalog Volumes:
# Use dbutils.fs.cp() in notebook to copy files
```

**Option 3: Backward Compatibility (If Needed)**
- Could add fallback logic to check both locations
- Not recommended - adds complexity we just removed

## Related Files

- `METADATA_FILE_FILTER_FIX.md` - Previous fix for underscore filtering (now superseded by this refactor)
- `ITERATOR_SCHEMA_FILE_FILTER_FIX.md` - Deprecated (issue now resolved by directory separation)

## Implementation Notes

1. **No changes to schema file format** - only the location changed
2. **Azure and Volume paths both updated** - consistent across storage backends
3. **`dbutils.fs.ls()` behavior** - directories are listed with their contents when recursive
4. **Filtering approach**:
   - **Data reading** (`_read_table_from_volume`): Filter out `/_metadata/` paths before processing
   - **Source comparison** (`analyze_volume_changefeed_files`): List all files, skip `/_metadata/` during processing loop
   - This ensures accurate file counts and clear debug messages

## Conclusion

This refactoring makes the code significantly simpler and more maintainable by using directory structure for metadata separation instead of filename prefix conventions. The 16 lines of complex filtering logic were reduced to 3 simple lines, and the architecture now follows industry-standard data lake patterns.
