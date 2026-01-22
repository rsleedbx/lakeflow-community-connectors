# Volume Sync Directory Structure Fix

## Problem

The volume sync script (`sync_azure_to_volume_compact.py`) was **not preserving the directory structure** when copying files from Azure Blob Storage to Unity Catalog Volumes.

When syncing files like:
```
json/defaultdb/public/test-json_usertable_with_split/1769028478/_metadata/schema.json
```

The script was extracting only the filename (`schema.json`) and placing it in the volume root, instead of preserving the `_metadata/` subdirectory.

### Root Cause

In the `sync_files()` function (line 135):

```python
filename = Path(blob_name).name  # ❌ Strips directory structure!
```

This extracts ONLY the filename, discarding any subdirectory information.

## Solution

### 1. Preserve Directory Structure (lines 113-176)

Updated `sync_files()` to:

1. **Extract relative path** after the timestamp directory:
   ```python
   # Find timestamp directory (10-digit number)
   # Extract everything after it (e.g., "_metadata/schema.json")
   ```

2. **Create subdirectories** in volume as needed:
   ```python
   if relative_path.parent != Path('.'):
       parent_dir = f"{volume_path}/{relative_path.parent}"
       w.files.create_directory(parent_dir)
   ```

3. **Upload to correct path**:
   ```python
   target_path = f"{volume_path}/{relative_path}"
   w.files.upload(target_path, f)
   ```

### 2. Recursive Directory Listing (lines 104-128)

Updated `list_volume_files()` to recursively scan subdirectories and return all filenames.

This ensures the "already in volume" check works correctly for both flat and nested file structures.

## Result

Now when syncing:
```
Azure: json/.../1769028478/_metadata/schema.json
```

The file is correctly placed in:
```
Volume: /Volumes/.../1769028478/_metadata/schema.json
```

✅ Directory structure is preserved  
✅ `_metadata/` subdirectory is created automatically  
✅ Backward compatible with flat file structures

## Testing

To test the fix, run:

```bash
# Re-sync existing test data (with new directory structure)
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb

# Resync a specific test that has _metadata/schema.json in Azure
./scripts/test_cdc_matrix.sh --validate-only 1769028478
```

Or manually sync:
```bash
python3 scripts/sync_azure_to_volume_compact.py \
  --prefix json/defaultdb/public/test-json_usertable_with_split/1769028478 \
  --subdir json/defaultdb/public/test-json_usertable_with_split/1769028478
```

Then verify in the notebook that `_metadata/schema.json` is found:
```python
dbutils.fs.ls("/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769028478/_metadata/")
```

## Related Changes

- `changefeed_helper.py`: Updated to write schema to `_metadata/schema.json` in Azure
- `cockroachdb.py`: Updated `_load_schema_from_volume()` to read from `_metadata/schema.json`
- `cockroachdb.py`: Updated `_load_schema_from_azure()` to read from `_metadata/schema.json`
- `test_cdc_matrix.sh`: All tests now create `_metadata/schema.json` in Azure

## Impact

- **Breaking**: Existing volumes may need to be re-synced to get the new directory structure
- **Backward Compatible**: The sync script still handles flat file structures correctly
- **Forward Compatible**: All new test runs will use `_metadata/` subdirectory
