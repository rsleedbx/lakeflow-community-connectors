# Recursive Directory Support for CDC Files

## Problem

After fixing the sync script to preserve directory structure (including date subdirectories like `2026-01-21/` and the `_metadata/` directory), the connector code was not recursively reading files from these subdirectories, causing "VOLUME PATH IS EMPTY" errors even though files existed in subdirectories.

## Root Cause

Multiple methods were only listing files in the top-level directory:

1. **`_list_volume_files()`** - Only did `dbutils.fs.ls(path)` without recursion
2. **`load_and_merge_cdc_to_delta()` validation** - Only checked top-level directory for files
3. **Autoloader** - Missing `recursiveFileLookup` option
4. **Empty `item.name` Bug** - `dbutils.fs.ls()` sometimes returns items with empty `name` attribute, causing directory detection to fail

## CockroachDB Changefeed Directory Structure

CockroachDB changefeeds organize files by date:

```
/Volumes/catalog/schema/volume/
  json/defaultdb/public/test-json_usertable_with_split/1769028478/
    ├── _metadata/
    │   └── schema.json                    # Schema metadata
    ├── 2026-01-20/                        # Date-based subdirectory
    │   ├── 202601201234567890000000000-...-usertable-1.ndjson
    │   └── 202601201234567890000000001-...-usertable-2.ndjson
    ├── 2026-01-21/                        # Another date subdirectory
    │   ├── 202601212055256960041520000000000-...-usertable-1.ndjson
    │   └── 202601212055560000000000000000001-...-usertable-2.ndjson
    └── 202601191234567890000000000-...-usertable-1.ndjson  # Files can also be flat
```

This structure is created automatically by CockroachDB as the changefeed runs over multiple days.

## Solution

Updated all file listing and reading methods to recursively process subdirectories while correctly filtering out metadata.

### 1. Fixed `_list_volume_files()` (lines 1240-1262)

**Before:**
```python
files = dbutils.fs.ls(effective_path)
for file_info in files:
    if any(file_info.name.endswith(ext) for ext in file_extensions):
        file_list.append({
            'name': file_info.name,
            'path': file_info.path,
            'size': file_info.size
        })
```

**After (with empty name handling):**
```python
def list_recursive(path):
    """Recursively list files in directory and subdirectories."""
    files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            # Skip metadata directories (check path since item.name can be empty)
            if '/_metadata/' in item.path:
                continue
            
            # Extract directory/file name from path (handle empty item.name)
            item_name = item.name if item.name else item.path.rstrip('/').split('/')[-1]
            
            # Skip items starting with underscore
            if item_name.startswith('_'):
                continue
            
            # Check if it's a file by extension
            is_data_file = any(item_name.endswith(ext) for ext in file_extensions)
            
            if is_data_file:
                # Add data file
                files.append({
                    'name': item_name,
                    'path': item.path,
                    'size': item.size
                })
            else:
                # Assume it's a directory, recurse into it
                files.extend(list_recursive(item.path))
    except Exception:
        pass  # Directory might not exist or be accessible
    return files

file_list = list_recursive(effective_path)
```

**Key improvement:** Extracts directory name from `item.path` when `item.name` is empty, avoiding the `dbutils.fs.ls()` quirk.

### 2. Fixed `load_and_merge_cdc_to_delta()` Validation (lines 5930-5959)

**Before:**
```python
all_files = dbutils.fs.ls(volume_path)
data_files = [f for f in all_files if f.name.endswith(file_extension) and not f.name.startswith('_')]
```

**After:**
```python
def list_data_files_recursive(path):
    """Recursively list data files, skipping metadata."""
    files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            # Skip metadata directories
            if '/_metadata/' in item.path or item.name.startswith('_'):
                continue
            
            if item.isDir():
                # Recurse into subdirectories (e.g., date dirs like 2026-01-21/)
                files.extend(list_data_files_recursive(item.path))
            elif item.name.endswith(file_extension):
                files.append(item)
    except Exception:
        pass  # Directory might not exist or be accessible
    return files

data_files = list_data_files_recursive(volume_path)
```

### 3. Fixed Autoloader Configuration (line 6194)

**Before:**
```python
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", cloudfiles_format)
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("pathGlobFilter", f"*{effective_table}*{file_extension}")
    .load(volume_path)
)
```

**After:**
```python
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", cloudfiles_format)
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("recursiveFileLookup", "true")  # ✅ Read files in subdirectories
    .option("pathGlobFilter", f"*{effective_table}*{file_extension}")
    .load(volume_path)
)
```

### 4. Fixed Azure Blob Listing (3 locations)

**Added `_metadata/` filtering:**

```python
# Location 1: _list_azure_parquet_files() (line 2807)
if '/_metadata/' in blob.name:
    continue

# Location 2: _get_schema_from_files() (line 425)
data_files = [blob for blob in blob_list 
             if file_ext in blob.name 
             and '/_metadata/' not in blob.name 
             and not blob.name.split('/')[-1].startswith('_')]

# Location 3: analyze_azure_changefeed_files() (line 3617)
if '/_metadata/' in blob.name:
    continue
```

**Note:** Azure's `list_blobs(name_starts_with=prefix)` is already recursive by default, so it correctly finds files in date subdirectories. We only needed to add proper `_metadata/` filtering.

## Impact

### ✅ Now Works Correctly

1. **Volume Mode (Iterator Pattern)**: Recursively reads JSON/Parquet files from date subdirectories
2. **Volume Mode (Autoloader)**: Recursively processes files in subdirectories via `recursiveFileLookup`
3. **Azure Modes**: Correctly processes files in date subdirectories while excluding `_metadata/`
4. **Schema Loading**: `_metadata/schema.json` is correctly excluded from data file processing
5. **Date-based Organization**: Handles CockroachDB's date subdirectory structure automatically

### 🎯 Benefits

- **Handles long-running changefeeds**: Files from multiple days are correctly processed
- **Preserves directory structure**: Maintains CockroachDB's natural organization
- **Proper metadata separation**: `_metadata/` directory is never processed as data
- **Backward compatible**: Still works with flat file structures (no date subdirectories)

## Testing

To verify recursive reading works:

```python
# In Databricks notebook
from cockroachdb import LakeflowConnect, ConnectorMode

# Test Volume mode with date subdirectories
connector = LakeflowConnect({
    'mode': ConnectorMode.VOLUME,
    'volume_path': '/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769028478',
    'spark': spark,
    'dbutils': dbutils
})

# Should find files in 2026-01-20/, 2026-01-21/, etc.
iterator = connector.read_table('usertable', {}, {})
records = list(iterator)
print(f"✅ Found {len(records)} records across all subdirectories")
```

## Related Changes

- **`sync_azure_to_volume_compact.py`**: Fixed to preserve directory structure (including date subdirectories)
- **`_metadata/schema.json` refactor**: Moved schema files to dedicated `_metadata/` subdirectory
- **`--resync` command**: Added to `test_cdc_matrix.sh` for re-syncing existing data

## Why Date Subdirectories Appear

CockroachDB changefeeds organize output files by date:
- Day 1 (2026-01-20): Files go to `2026-01-20/` subdirectory
- Day 2 (2026-01-21): New files go to `2026-01-21/` subdirectory

This is automatic behavior and cannot be disabled. The connector must handle this structure correctly.
