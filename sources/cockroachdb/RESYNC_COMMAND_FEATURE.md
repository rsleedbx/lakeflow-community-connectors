# Resync Command Feature for test_cdc_matrix.sh

## Overview

Added a new `--resync` command to `test_cdc_matrix.sh` that re-syncs existing test data from Azure Blob Storage to Unity Catalog Volumes **without running new tests**. This is particularly useful after making changes to the sync script or directory structure.

## Use Case

When the `sync_azure_to_volume_compact.py` script was updated to support the new `_metadata/` directory structure, existing test data in Azure needed to be re-synced to the Volume to pick up the new directory layout. The `--resync` command allows this without having to:
1. Cancel all existing changefeeds
2. Drop all test tables
3. Re-run the entire test suite (which takes 30+ minutes)

## Usage

```bash
# Re-sync latest test data (all formats)
./test_cdc_matrix.sh --resync

# Re-sync specific timestamp
./test_cdc_matrix.sh --resync 1769028478

# Re-sync only JSON format
./test_cdc_matrix.sh --resync json

# Short form
./test_cdc_matrix.sh -r
```

## What It Does

1. **Finds test data in Azure** - Locates existing test data at the specified timestamp
2. **Calls sync script** - Runs `sync_azure_to_volume_compact.py` for each test scenario
3. **Preserves Azure data** - Does not modify or delete any data in Azure
4. **Preserves database state** - Does NOT cancel changefeeds or drop tables (unlike full test mode)
5. **Updates Volume structure** - Copies files to Volume with latest directory layout

## Implementation Details

### New Command-Line Flag

```bash
--resync|-r [TIMESTAMP]
```

If no timestamp is provided, automatically finds and uses the latest test run.

### New Function: `resync_test()`

Similar to `validate_test()`, but instead of analyzing files, it:
1. Checks if files exist in Azure for the test scenario
2. Calls the sync script to copy them to the Volume
3. Reports success/failure

### What Resync Mode Skips

Resync mode is **read-only** with respect to the database. It skips:
- ❌ Cleanup (canceling changefeeds, dropping tables)
- ❌ Table creation
- ❌ Changefeed creation
- ❌ Workload generation

It **only** performs:
- ✅ File sync from Azure → Volume

### Test Loop Integration

The main test loop now handles three modes:
```bash
if $VALIDATE_ONLY; then
    validate_test "$format" "$table" "$split_option"
elif $RESYNC_MODE; then
    resync_test "$format" "$table" "$split_option"
else
    run_test "$format" "$table" "$split_option"
fi
```

### Summary Output

The resync summary shows:
- ✅ **RESYNCED**: Successfully synced from Azure to Volume
- ⚠️  **SKIPPED**: No data found in Azure for this test
- ❌ **FAILED**: Sync script failed

## Example Output

```bash
$ ./test_cdc_matrix.sh --resync

🔄 RESYNC MODE - Re-syncing Azure data to Volume
   Timestamp: 1769028478
   ⚡ Will preserve existing Azure data
   ⚡ Will update Volume directory structure

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Resync 1/8: json_usertable_with_split
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Format: json
  Base Table: usertable
  Split Option: with_split
  Path: json/defaultdb/public/test-json_usertable_with_split/1769028478/

📊 Found 307 json files in Azure

📦 Syncing to Unity Catalog Volume (preserving hierarchy)...
  Prefix: json/defaultdb/public/test-json_usertable_with_split/1769028478/
  Subdir: json/defaultdb/public/test-json_usertable_with_split/1769028478
  Structure: format/catalog/schema/test-scenario/

✅ Files synced to volume: json/defaultdb/public/test-json_usertable_with_split/1769028478
   (Including _metadata/schema.json from Azure)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 RESYNC SUMMARY (Timestamp: 1769028478)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Test 1/8: json_usertable_with_split - RESYNCED (files=307)
Test 2/8: json_usertable_no_split - RESYNCED (files=4)
Test 3/8: json_simple_test_with_split - RESYNCED (files=4)
Test 4/8: json_simple_test_no_split - RESYNCED (files=4)
Test 5/8: parquet_usertable_with_split - RESYNCED (files=307)
Test 6/8: parquet_usertable_no_split - RESYNCED (files=4)
Test 7/8: parquet_simple_test_with_split - RESYNCED (files=4)
Test 8/8: parquet_simple_test_no_split - RESYNCED (files=4)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ All resyncs complete!
📄 Full results saved to: /tmp/cdc_test_results.txt

Summary:
  ✅ RESYNCED: 8/8
  ⚠️  SKIPPED (no Azure data): 0/8
  ❌ FAILED: 0/8
```

## Benefits

1. **Fast** - Takes seconds instead of 30+ minutes for full test suite
2. **Safe** - Preserves existing test data and running changefeeds
3. **Targeted** - Can resync specific formats or timestamps
4. **Idempotent** - Safe to run multiple times

## Related Files

- `test_cdc_matrix.sh` - Main test script (added --resync flag and resync_test() function)
- `sync_azure_to_volume_compact.py` - Sync script (updated to preserve _metadata/ directory)
- `VOLUME_SYNC_DIRECTORY_FIX.md` - Documents the _metadata/ directory structure change

## Future Enhancements

Possible improvements:
1. Add `--force` flag to overwrite even if files already exist in Volume
2. Add `--dry-run` flag to preview what would be synced
3. Show diff between Azure and Volume before syncing
4. Parallel sync for faster execution
