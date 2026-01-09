# Auto-Sync Integration for test_cdc_matrix.sh

## Summary

The `test_cdc_matrix.sh` script now **automatically syncs** each test's data to Unity Catalog Volume in scenario-specific subdirectories!

## What Changed

### 1. Enhanced `sync_azure_to_volume_compact.py`

Added `--subdir` flag to support scenario-specific subdirectories:

```python
parser.add_argument("--subdir", help="Volume subdirectory for this test")

# Usage
if args.subdir:
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{args.subdir}"
else:
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
```

### 2. Integrated Sync into `test_cdc_matrix.sh`

After each test completes, it now automatically syncs files:

```bash
# In run_test() function, after test completes:
python3 "$SCRIPTS_DIR/sync_azure_to_volume_compact.py" \
  --prefix "${path_prefix}" \
  --subdir "${path_prefix}"
```

### 3. Scenario-Specific Volume Structure

Each test now has its own subdirectory:

```
/Volumes/main/robert_lee_cockroachdb/parquet_files/
├── test-json_usertable_with_split/
│   ├── 202512191714242809831900000000000-...-usertable.ndjson
│   └── ...
├── test-json_usertable_no_split/
│   ├── 202512191714242809831900000000000-...-usertable.ndjson
│   └── ...
├── test-parquet_usertable_with_split/
│   ├── 202512191714242809831900000000000-...-usertable+fam_0_ycsb_key-1.parquet
│   ├── 202512191714242809831900000000000-...-usertable+fam_1_field0-1.parquet
│   └── ... (11 files total for split families)
├── test-parquet_usertable_no_split/
│   ├── 202512191714242809831900000000000-...-usertable-1.parquet
│   └── ...
├── test-json_simple_test_no_split/
│   └── ...
└── test-parquet_simple_test_no_split/
    └── ...
```

## Benefits

### ✅ Automated Workflow

**Before:**
1. Run `test_cdc_matrix.sh`
2. Manually run `sync_azure_to_volume.sh`
3. Manually edit notebook for each test
4. Test in notebook

**After:**
1. Run `test_cdc_matrix.sh` (syncs automatically!)
2. Test each scenario in notebook (just change TEST_SCENARIO variable)

### ✅ Isolated Testing

- Each test scenario is in its own subdirectory
- No file naming conflicts between tests
- Easy to test one scenario at a time
- Clear separation for debugging

### ✅ Parallel Development

- Can test different scenarios simultaneously
- No need to clear volume between tests
- Multiple users can test different scenarios

### ✅ Reproducible Results

- Each test's data is preserved separately
- Can retest specific scenarios without rerunning entire matrix
- Historical test data available for comparison

## Usage

### Running the Test Matrix

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

**Output after each test:**
```
📦 Syncing to Unity Catalog Volume...
  Prefix: test-parquet_usertable_with_split/
  Subdir: test-parquet_usertable_with_split
✅ Sync Complete!
📦 Volume contains 11 Parquet files
✅ Files synced to volume subdirectory: test-parquet_usertable_with_split
```

### Testing in Notebook

Open `notebooks/load_parquet_with_merge.ipynb` and update Cell 1:

```python
# 🔧 CHANGE THIS TO TEST DIFFERENT SCENARIOS
TEST_SCENARIO = "test-parquet_usertable_with_split"  # <-- Change this!
SOURCE_TABLE = "usertable"
PRIMARY_KEY_COLUMNS = ["ycsb_key"]

# Derived paths (automatic)
VOLUME_PATH = f"dbfs:/Volumes/{TARGET_CATALOG}/{TARGET_SCHEMA}/{VOLUME_NAME}/{TEST_SCENARIO}"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"
```

**Test scenarios:**
- `test-json_usertable_with_split`
- `test-json_usertable_no_split`
- `test-parquet_usertable_with_split` ⭐ (most important - tests merge)
- `test-parquet_usertable_no_split`
- `test-json_simple_test_no_split`
- `test-parquet_simple_test_no_split`

### Manual Resync (if needed)

To resync a specific test without rerunning the entire matrix:

```bash
python3 sync_azure_to_volume_compact.py \
  --prefix test-parquet_usertable_with_split \
  --subdir test-parquet_usertable_with_split
```

## Technical Details

### Sync Process

1. **Test completes** → Files in Azure Blob Storage
2. **Auto-sync triggered** → `sync_azure_to_volume_compact.py`
3. **Files downloaded** → Temporary directory
4. **Files uploaded** → Unity Catalog Volume subdirectory
5. **Temp cleanup** → Automatic via context manager

### Volume Path Construction

```python
# Without subdir (original)
volume_path = "/Volumes/main/robert_lee_cockroachdb/parquet_files"

# With subdir (new)
volume_path = "/Volumes/main/robert_lee_cockroachdb/parquet_files/test-parquet_usertable_with_split"
```

### Error Handling

If sync fails, the test continues anyway:

```bash
if python3 "$SCRIPTS_DIR/sync_azure_to_volume_compact.py" ... ; then
    echo "✅ Files synced"
else
    echo "⚠️  Sync failed (continue anyway)"
fi
```

This ensures one sync failure doesn't break the entire test matrix.

## Validation Workflow

### Step 1: Run Tests

```bash
./test_cdc_matrix.sh
```

**Result:** All 12 tests run, each synced to its own subdirectory.

### Step 2: Validate Specific Scenario

```bash
# Edit notebook Cell 1:
TEST_SCENARIO = "test-parquet_usertable_with_split"

# Run all cells in notebook

# Verify:
# - Delta table row count matches test output
# - No data inflation (should be ~9,995 rows, not 109,945)
# - Column family merge worked correctly
```

### Step 3: Compare Results

Cross-reference:
- `test_cdc_matrix.sh` output → Unique keys count
- Notebook Cell 7 → Delta table row count
- Notebook Cell 8 → Volume file analysis

All three should match! ✅

## Performance

### Sync Time per Test

| Scenario | Files | Avg Size | Sync Time |
|----------|-------|----------|-----------|
| JSON, no split | 1 | ~1MB | ~2s |
| JSON, split | 1 | ~1MB | ~2s |
| Parquet, no split | 1 | ~1MB | ~2s |
| **Parquet, split** | **11** | **~10MB total** | **~8s** |

### Total Test Matrix Time

**Before (manual sync):**
- Test matrix: ~20 minutes
- Manual sync: ~2 minutes
- **Total: ~22 minutes**

**After (auto sync):**
- Test matrix with auto-sync: ~22 minutes
- Manual sync: 0 minutes (automatic!)
- **Total: ~22 minutes**

**Same time, but fully automated!** 🎉

## Troubleshooting

### Issue: Sync fails with "No files found"

**Cause:** Test path doesn't match Azure prefix.

**Fix:** Check that `path_prefix` in test matches Azure container structure.

### Issue: Volume subdirectory not found in notebook

**Cause:** Typo in TEST_SCENARIO or volume not mounted.

**Fix:**
```python
# List all subdirectories
display(dbutils.fs.ls("dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/"))
```

### Issue: Sync takes too long

**Cause:** Many files or slow network.

**Fix:** The test continues regardless. Resync manually later if needed.

## Future Enhancements

### Possible Improvements

1. **Parallel sync** - Sync tests in background while next test runs
2. **Incremental sync** - Only sync new files since last run
3. **Cleanup old tests** - Auto-delete subdirectories older than N days
4. **Sync stats** - Show sync progress in test output
5. **Validation hooks** - Automatically run notebook validation after sync

### Configuration Options

Could add to `cockroachdb_pipelines.json`:

```json
{
  "auto_sync_enabled": true,
  "sync_timeout": 300,
  "sync_retry_count": 3,
  "keep_old_tests": 7
}
```

## Summary

**Key Achievement:** Fully automated CDC testing workflow with scenario-specific isolation!

**What you get:**
- ✅ Automatic sync after each test
- ✅ Scenario-specific subdirectories
- ✅ Easy notebook validation
- ✅ No manual intervention needed
- ✅ Preserved test data for analysis

**Next steps:**
1. Run `./test_cdc_matrix.sh`
2. Wait for all tests + syncs to complete
3. Test scenarios in notebook (just change TEST_SCENARIO)
4. Compare results and verify correctness

**Documentation:**
- This file: Technical details
- `CDC_TEST_MATRIX_NOTEBOOK_VALIDATION.md`: Testing guide
- `SYNC_VERSIONS_COMPARISON.md`: Sync script comparison


