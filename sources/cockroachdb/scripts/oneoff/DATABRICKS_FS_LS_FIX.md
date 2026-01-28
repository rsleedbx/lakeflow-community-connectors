# Fix: `databricks fs ls` Does Not Work with Unity Catalog Volumes

## Problem

The Databricks CLI command `databricks fs ls` is designed for **DBFS (Databricks File System)** only and **does NOT work with Unity Catalog Volumes**.

Using it with Unity Catalog Volumes results in:
```bash
$ databricks fs ls /Volumes/main/robert_lee_cockroachdb/parquet_files/
Error: no such directory: /Volumes/main/robert_lee_cockroachdb/parquet_files
```

## Root Cause

Three bash scripts were using the incorrect command:
1. **`setup_volume_pipeline.sh`** (line 76-77)
2. **`test_volume_mode.sh`** (line 40)
3. **`sync_azure_to_volume.sh`** (lines 220, 316, 318)

All were using:
```bash
databricks fs ls "dbfs:/Volumes/..." | grep '\.parquet$'
```

## Solution

### 1. Created Helper Script: `list_volume_files.py`

A Python script that uses the **Databricks SDK** to properly list Unity Catalog Volume files:

```bash
# List all files
python3 list_volume_files.py /Volumes/catalog/schema/volume/path/

# Count files matching pattern
python3 list_volume_files.py --count --pattern "*.parquet" /Volumes/catalog/schema/volume/

# List JSON files
python3 list_volume_files.py --pattern "*.ndjson" /Volumes/catalog/schema/volume/
```

**Why it works**: Uses `WorkspaceClient.files.list_directory_contents()` which is the correct API for Unity Catalog Volumes.

### 2. Fixed All Three Scripts

Replaced incorrect `databricks fs ls` calls with calls to `list_volume_files.py`.

#### Before:
```bash
FILE_COUNT=$(databricks fs ls "dbfs:${VOLUME_PATH}/" 2>/dev/null | grep -c '\.parquet$' || echo "0")
```

#### After:
```bash
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FILE_COUNT=$(python3 "$SCRIPTS_DIR/list_volume_files.py" --count --pattern "*.parquet" "${VOLUME_PATH}/" 2>/dev/null || echo "0")
```

## Verification

Tested with actual volume paths:

```bash
# List files (works!)
$ python3 scripts/list_volume_files.py "/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769022634/"
/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769022634/202601211910497616069700000000000-21f6691d43a62e56-2-16-00000000-test_json_usertable_with_split+data-1.ndjson
/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769022634/202601211910497616069700000000000-21f6691d43a62e56-2-16-00000001-test_json_usertable_with_split+pk-1.ndjson
...
/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769022634/_schema.json

# Count JSON files (works!)
$ python3 scripts/list_volume_files.py --count --pattern "*.ndjson" "/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769022634/"
4

# Count Parquet files (works!)
$ python3 scripts/list_volume_files.py --count --pattern "*.parquet" "/Volumes/main/robert_lee_cockroachdb/parquet_files/parquet/defaultdb/public/test-parquet_usertable_with_split/1769022634/"
4
```

## How to Access Unity Catalog Volumes

| Method | Tool | Works? |
|--------|------|--------|
| **Databricks CLI** | `databricks fs ls /Volumes/...` | ❌ NO |
| **Databricks SDK (Python)** | `w.files.list_directory_contents("/Volumes/...")` | ✅ YES |
| **Databricks Notebooks** | `dbutils.fs.ls("/Volumes/...")` | ✅ YES |
| **Bash (our helper)** | `python3 list_volume_files.py /Volumes/...` | ✅ YES |

## Files Modified

1. ✅ Created: `scripts/list_volume_files.py`
2. ✅ Fixed: `scripts/setup_volume_pipeline.sh` (lines 76-79)
3. ✅ Fixed: `scripts/test_volume_mode.sh` (lines 40-42)
4. ✅ Fixed: `scripts/sync_azure_to_volume.sh` (lines 220, 316, 318)

## Note on `sync_azure_to_volume_compact.py`

The newer Python sync script **already uses the correct SDK method** and was not affected by this issue. Only the older bash scripts needed fixing.
