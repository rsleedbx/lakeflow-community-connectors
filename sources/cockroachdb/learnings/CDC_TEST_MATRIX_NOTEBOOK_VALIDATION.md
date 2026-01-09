# CDC Test Matrix - Notebook Validation Guide

This guide explains how to validate CDC test results using the `load_parquet_with_merge.ipynb` notebook.

## Overview

The `test_cdc_matrix.sh` script now:
- ✅ Shows detailed CDC operation statistics (snapshot, insert, update, delete counts)
- ✅ Keeps changefeeds **running** after tests (not cancelled)
- ✅ Keeps Azure container data **intact** for notebook validation
- ✅ Provides instructions for testing each changefeed with the notebook

## Test Workflow

### Step 1: Run CDC Test Matrix

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

**Output includes:**
- File counts (snapshot vs CDC files)
- **CDC operation statistics** (snapshot, insert, update, delete rows)
- **Unique keys** (deduplicated count for Parquet with split_column_families)
- Path prefix for each test (e.g., `test-parquet_usertable_with_split`)
- Job IDs for active changefeeds

### Step 2: Verify Data is Synced to Unity Catalog Volume

**Good news:** The `test_cdc_matrix.sh` script now **automatically syncs** each test to the Unity Catalog Volume!

Each test is stored in its own subdirectory:
```
/Volumes/{catalog}/{schema}/{volume}/
  ├── test-json_usertable_with_split/
  ├── test-json_usertable_no_split/
  ├── test-parquet_usertable_with_split/
  ├── test-parquet_usertable_no_split/
  ├── test-json_simple_test_no_split/
  └── test-parquet_simple_test_no_split/
```

**Manual resync (if needed):**
```bash
cd sources/cockroachdb/scripts
python3 sync_azure_to_volume_compact.py \
  --prefix test-parquet_usertable_with_split \
  --subdir test-parquet_usertable_with_split
```

### Step 3: Validate with Notebook

#### A. Configure the Notebook

Open `sources/cockroachdb/notebooks/load_parquet_with_merge.ipynb` and update **Cell 1** (Setup Configuration):

```python
# 🔧 CHANGE THESE TO TEST DIFFERENT SCENARIOS
TEST_SCENARIO = "test-parquet_usertable_with_split"  # Scenario name from test_cdc_matrix.sh
SOURCE_TABLE = "usertable"                            # "usertable" or "simple_test"
PRIMARY_KEY_COLUMNS = ["ycsb_key"]                   # ["ycsb_key"] or ["id"]

# Derived paths
VOLUME_PATH = f"dbfs:/Volumes/{TARGET_CATALOG}/{TARGET_SCHEMA}/{VOLUME_NAME}/{TEST_SCENARIO}"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"
```

#### B. Test Configurations

| Test Name | TEST_SCENARIO | SOURCE_TABLE | PRIMARY_KEY_COLUMNS | Volume Path |
|-----------|---------------|--------------|---------------------|-------------|
| JSON, usertable, split | `test-json_usertable_with_split` | `usertable` | `["ycsb_key"]` | `.../test-json_usertable_with_split/` |
| JSON, usertable, no split | `test-json_usertable_no_split` | `usertable` | `["ycsb_key"]` | `.../test-json_usertable_no_split/` |
| **Parquet, usertable, split** | `test-parquet_usertable_with_split` | `usertable` | `["ycsb_key"]` | `.../test-parquet_usertable_with_split/` |
| Parquet, usertable, no split | `test-parquet_usertable_no_split` | `usertable` | `["ycsb_key"]` | `.../test-parquet_usertable_no_split/` |
| JSON, simple_test, no split | `test-json_simple_test_no_split` | `simple_test` | `["id"]` | `.../test-json_simple_test_no_split/` |
| Parquet, simple_test, no split | `test-parquet_simple_test_no_split` | `simple_test` | `["id"]` | `.../test-parquet_simple_test_no_split/` |

**Note:** The Parquet with split families test is the most critical - it tests the column family merge logic.

#### C. Run the Notebook

1. **Cell 1**: Setup Configuration → Verify test name and table
2. **Cell 2**: Load with Autoloader → Loads from Unity Catalog Volume
3. **Cell 3**: Add CDC Metadata → Maps event types
4. **Cell 4**: Merge Column Family Fragments → **Critical for split_column_families tests**
5. **Cell 5**: Clear Checkpoint (optional) → Only if rerunning
6. **Cell 6**: Write to Delta Table → Creates/updates Delta table
7. **Cell 7**: Verify Results → Compares Delta table count
8. **Cell 8**: Compare with Source Files → Validates against raw Parquet files

#### D. Verification Checklist

For each test, verify:

| Check | What to Verify | Expected Result |
|-------|---------------|-----------------|
| ✅ Delta table row count | Cell 7 output | Should match `unique_keys` from test_cdc_matrix.sh |
| ✅ Volume analysis | Cell 8 output | Should match Delta table count |
| ✅ Operation breakdown | Cell 7 display | Snapshot, Insert, Update, Delete counts |
| ✅ No null data columns | Cell 7 sample | All field0-field9 should have values (not NULL) |
| ✅ Merge applied (if split) | Cell 4 output | Should show "Streaming mode: Applying merge" |

### Step 4: Compare Results

Cross-reference the notebook results with `test_cdc_matrix.sh` output:

```bash
# From test_cdc_matrix.sh output:
📊 CDC Operation Statistics:
  Snapshot rows: 9995
  Insert rows: 0
  Update rows: 0
  Delete rows: 0
  Unique keys (deduplicated): 9995

# From notebook Cell 7:
📊 Delta table: 9,995 rows
   ✅ Merge worked!

# From notebook Cell 8:
📊 Volume files: 11
   Unique keys: 9995
   UPSERT: 9,995
   DELETE: 0
   Total: 9,995

📊 Comparison:
   Delta: 9,995
   Volume: 9,995

   ✅✅✅ PERFECT MATCH! ✅✅✅
```

**Expected Behavior:**
- `test_cdc_matrix.sh` unique keys = Notebook Delta table rows = Notebook Volume analysis

## Expected Test Results

Based on historical testing (from `CDC_TEST_MATRIX_RESULTS.md`):

| Format | Table | Split Families | Files | CDC Events | Status |
|--------|-------|----------------|-------|------------|--------|
| JSON | usertable | ❌ No | Snapshot + CDC | ✅ All captured | ✅ SUCCESS |
| JSON | usertable | ✅ Yes | Snapshot + CDC | ✅ All captured | ✅ SUCCESS |
| Parquet | usertable | ❌ No | Snapshot + CDC | ✅ All captured | ✅ SUCCESS |
| Parquet | usertable | ✅ Yes | Snapshot + CDC | ✅ All captured (11x fragments) | ✅ SUCCESS (with merge) |
| JSON | simple_test | ❌ No | Snapshot + CDC | ✅ All captured | ✅ SUCCESS |
| Parquet | simple_test | ❌ No | Snapshot + CDC | ✅ All captured | ✅ SUCCESS |

**Note:** Parquet with `split_column_families=true` requires the merge function to avoid 11x data inflation.

## Cleanup (When Testing Complete)

After validating all tests, clean up:

### Cancel All Changefeeds

```bash
# List running changefeeds
python3 sources/cockroachdb/scripts/changefeed_helper.py find-changefeeds \
  --table usertable \
  --json sources/cockroachdb/.env/cockroachdb_credentials.json

python3 sources/cockroachdb/scripts/changefeed_helper.py find-changefeeds \
  --table simple_test \
  --json sources/cockroachdb/.env/cockroachdb_credentials.json

# Cancel each one
python3 sources/cockroachdb/scripts/changefeed_helper.py cancel-changefeed \
  --job-id <JOB_ID> \
  --json sources/cockroachdb/.env/cockroachdb_credentials.json
```

### Delete Test Data from Azure (Optional)

```bash
# Load credentials
source sources/cockroachdb/scripts/00_lakeflow_connect_env.sh
AZ_INIT

# Delete all test-* blobs
az storage blob delete-batch \
  --account-name "$AZURE_STORAGE_ACCOUNT" \
  --account-key "$AZURE_STORAGE_KEY" \
  --source changefeed-events \
  --pattern 'test-*'
```

### Clean Up Unity Catalog Volume (Optional)

```python
# In Databricks notebook
dbutils.fs.rm("dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files", True)
dbutils.fs.mkdirs("dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files")
```

### Drop Test Delta Tables

```sql
-- In Databricks SQL
DROP TABLE IF EXISTS main.robert_lee_cockroachdb.usertable_parquet_usertable_with_split_delta;
DROP TABLE IF EXISTS main.robert_lee_cockroachdb.usertable_parquet_usertable_no_split_delta;
DROP TABLE IF EXISTS main.robert_lee_cockroachdb.usertable_json_usertable_with_split_delta;
DROP TABLE IF EXISTS main.robert_lee_cockroachdb.usertable_json_usertable_no_split_delta;
DROP TABLE IF EXISTS main.robert_lee_cockroachdb.simple_test_parquet_simple_test_no_split_delta;
DROP TABLE IF EXISTS main.robert_lee_cockroachdb.simple_test_json_simple_test_no_split_delta;
```

## Troubleshooting

### Issue: Notebook shows 11x inflation (e.g., 109,945 rows instead of 9,995)

**Cause:** Testing Parquet with `split_column_families=true` but merge function not applied.

**Fix:** 
1. Verify Cell 4 runs the `merge_column_family_fragments()` function
2. Clear checkpoint (Cell 5) and rerun Cell 6

### Issue: "File not found" in notebook

**Cause:** Data not synced to Unity Catalog Volume.

**Fix:**
```bash
cd sources/cockroachdb/scripts
./sync_azure_to_volume.sh
```

### Issue: Notebook Delta count doesn't match test_cdc_matrix.sh

**Possible causes:**
1. Wrong `PATH_PREFIX` - verify you're testing the correct changefeed
2. Checkpoint not cleared - old data mixed with new
3. Volume not refreshed - run `sync_azure_to_volume.sh` again

## Key Learnings

1. **Parquet with split_column_families requires merge logic** - Without it, you get 11x data inflation
2. **JSON format doesn't fragment** - Works correctly without merge
3. **Merge is a no-op for non-split tables** - Safe to always apply
4. **Unity Catalog volumes simplify testing** - No need to manage credentials in notebook
5. **`complete` output mode works with streaming aggregations** - Required for merge function with Spark Connect

## References

- Main test script: `sources/cockroachdb/scripts/test_cdc_matrix.sh`
- Validation notebook: `sources/cockroachdb/notebooks/load_parquet_with_merge.ipynb`
- Historical results: `sources/cockroachdb/learnings/CDC_TEST_MATRIX_RESULTS.md`
- Merge function docs: `sources/cockroachdb/notebooks/NOTEBOOK_COLUMN_FAMILY_FIX.md`

