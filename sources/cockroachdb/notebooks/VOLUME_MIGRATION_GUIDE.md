# Volume Structure Migration Guide

## Current Structure (Old)

```
dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/
├── 202512191714242809831900000000000-...-usertable+fam_0_ycsb_key-4.parquet
├── 202512191714242809831900000000000-...-usertable+fam_1_field0-4.parquet
├── 202512191714242809831900000000000-...-usertable+fam_2_field1-4.parquet
├── ... (more parquet files)
└── _checkpoints/
    └── usertable/
        ├── schema/
        └── delta/
```

## New Structure (Required for test_cdc_scenario.ipynb)

```
dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/
├── existing-parquet-files/                              ⭐ Move base files here
│   ├── 202512191714242809831900000000000-...-usertable+fam_0_ycsb_key-4.parquet
│   ├── 202512191714242809831900000000000-...-usertable+fam_1_field0-4.parquet
│   ├── 202512191714242809831900000000000-...-usertable+fam_2_field1-4.parquet
│   └── ... (all existing parquet files)
│
├── test-parquet_usertable_with_split/                   ⭐ From test_cdc_matrix.sh
│   ├── 202512191714242809831900000000000-...-usertable+fam_0_ycsb_key-4.parquet
│   ├── 202512191714242809831900000000000-...-usertable+fam_1_field0-4.parquet
│   └── ... (test scenario files)
│
├── test-parquet_usertable_no_split/                     ⭐ From test_cdc_matrix.sh
│   └── 202512191714242809831900000000000-...-usertable-4.parquet
│
└── _checkpoints/                                         ⭐ Keep at base level
    ├── existing-parquet-files/
    │   └── usertable/
    └── test-parquet_usertable_with_split/
        └── usertable/
```

## Migration Options

### Option 1: Move Existing Files to Scenario (Recommended)

**Create a scenario for your existing data:**

```python
# In Databricks notebook or Python
TEST_SCENARIO = "existing-parquet-files"  # Or any name you choose

# Move files
source_path = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/"
target_path = f"dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/{TEST_SCENARIO}/"

# Create target directory
dbutils.fs.mkdirs(target_path)

# Move all parquet files
files = dbutils.fs.ls(source_path)
for file in files:
    if file.name.endswith('.parquet'):
        source = file.path
        target = target_path + file.name
        dbutils.fs.mv(source, target)
        print(f"Moved: {file.name}")
```

**Or using Databricks CLI:**

```bash
# List files
databricks fs ls dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/

# Create scenario directory
databricks fs mkdirs dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/existing-parquet-files/

# Move files (repeat for each file)
databricks fs mv \
  dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/202512191714242809831900000000000-...-usertable+fam_0_ycsb_key-4.parquet \
  dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/existing-parquet-files/
```

**Or using Azure Storage Explorer / Azure CLI:**

```bash
# If syncing from Azure Blob Storage, update sync script
# In sync_azure_to_volume_compact.py, add --subdir flag:

python3 sync_azure_to_volume_compact.py \
  --prefix "parquet/defaultdb/public/usertable" \
  --subdir "existing-parquet-files"
```

### Option 2: Keep Files in Place (Not Compatible)

**This won't work** with `test_cdc_scenario.ipynb` because it expects:
```
VOLUME_PATH = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{TEST_SCENARIO}"
```

If you want to keep files at the base level, you would need to:
1. Use the old `load_parquet_with_merge.ipynb` notebook, OR
2. Modify `test_cdc_scenario.ipynb` (but you said no complexity!)

### Option 3: Copy (Not Move) - Keep Both

```python
# Copy existing files to a scenario without deleting originals
source_path = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/"
target_path = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/existing-parquet-files/"

dbutils.fs.mkdirs(target_path)

files = dbutils.fs.ls(source_path)
for file in files:
    if file.name.endswith('.parquet'):
        source = file.path
        target = target_path + file.name
        dbutils.fs.cp(source, target)
        print(f"Copied: {file.name}")
```

## After Migration

### Test with Existing Data

```python
# In test_cdc_scenario.ipynb Cell 4:
TEST_SCENARIO = "existing-parquet-files"  # ⭐ Use your scenario name
SOURCE_TABLE = "usertable"

# Volume path will be:
# dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/existing-parquet-files
```

### Test with New Scenarios

```python
# After running test_cdc_matrix.sh:
TEST_SCENARIO = "test-parquet_usertable_with_split"
SOURCE_TABLE = "usertable"

# Volume path will be:
# dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/test-parquet_usertable_with_split
```

## Checkpoint Migration

**Important**: Checkpoints should move too!

### Old checkpoint location:
```
dbfs:/Volumes/.../parquet_files/_checkpoints/usertable/
```

### New checkpoint locations:
```
dbfs:/Volumes/.../parquet_files/_checkpoints/existing-parquet-files/usertable/
dbfs:/Volumes/.../parquet_files/_checkpoints/test-parquet_usertable_with_split/usertable/
```

**The automated function handles this automatically:**
```python
# Checkpoint path is constructed from volume path:
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"

# For existing-parquet-files:
# dbfs:/Volumes/.../parquet_files/existing-parquet-files/_checkpoints
```

**You don't need to move checkpoints manually** - just clear and recreate:
```python
result = load_and_merge_cdc_to_delta(
    ...,
    clear_checkpoint=True  # ⭐ Clears old checkpoint and creates new one
)
```

## Summary

### What Must Change

| Item | Old Location | New Location |
|------|-------------|--------------|
| **Parquet files** | `parquet_files/*.parquet` | `parquet_files/{scenario}/*.parquet` |
| **Checkpoints** | Auto-created per scenario | Auto-created per scenario |
| **TEST_SCENARIO** | Not used | Must specify (e.g., `"existing-parquet-files"`) |

### What Stays the Same

| Item | Location |
|------|----------|
| **Volume base path** | `dbfs:/Volumes/main/schema/parquet_files` |
| **Configuration files** | `.env/cockroachdb_*.json` |
| **Python code** | `sources/cockroachdb/cockroachdb.py` |

## Quick Migration Script

```python
# Run this in a Databricks notebook to migrate existing files

# Configuration
CATALOG = "main"
SCHEMA = "robert_lee_cockroachdb"
VOLUME = "parquet_files"
NEW_SCENARIO = "existing-parquet-files"  # Choose your name

base_path = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
scenario_path = f"{base_path}/{NEW_SCENARIO}"

# Create scenario directory
dbutils.fs.mkdirs(scenario_path)

# Move only parquet files
files = dbutils.fs.ls(base_path)
moved_count = 0
for file in files:
    if file.name.endswith('.parquet') and not file.name.startswith('_'):
        try:
            dbutils.fs.mv(file.path, f"{scenario_path}/{file.name}")
            print(f"✅ Moved: {file.name}")
            moved_count += 1
        except Exception as e:
            print(f"❌ Failed: {file.name} - {e}")

print(f"\n{'='*80}")
print(f"Migration complete!")
print(f"Moved {moved_count} files to: {scenario_path}")
print(f"{'='*80}")
print(f"\nTo test, set in notebook Cell 4:")
print(f'TEST_SCENARIO = "{NEW_SCENARIO}"')
```

## Recommended Approach

1. **Move existing files** to `existing-parquet-files/` subdirectory
2. **Run test_cdc_matrix.sh** to generate new scenario data
3. **Test each scenario** independently using the notebook
4. **Clean up** old base-level files once verified

This keeps everything organized and makes it easy to test different configurations! 🎯


