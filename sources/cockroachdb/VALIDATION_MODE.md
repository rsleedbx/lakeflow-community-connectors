# Test Matrix Validation Mode

## Overview
The `test_cdc_matrix.sh` script now supports a **Validation Mode** that allows you to quickly test code changes against existing Azure changefeed data without creating new changefeeds or running workloads.

This is significantly faster than the full test mode (seconds vs. minutes) and is ideal for:
- Testing code changes to the analysis logic
- Verifying fixes without re-running the full test suite
- Debugging coalescing and operation detection logic
- Iterating on primary key extraction or event classification

## Usage

### Validate Latest Test Run (Auto-detect)
```bash
./test_cdc_matrix.sh --validate-only
```
Automatically finds the most recent test data in Azure and validates against it.

### Validate Specific Timestamp
```bash
./test_cdc_matrix.sh --validate-only 1767895046
```
Validates against test data from a specific test run timestamp.

### Validate Single Format
```bash
./test_cdc_matrix.sh --validate-only json
./test_cdc_matrix.sh -v parquet  # Short form
```
Validates only JSON or Parquet tests from the latest run.

### Show Help
```bash
./test_cdc_matrix.sh --help
```

## What It Does

### ✅ In Validation Mode (Fast - ~30 seconds for 8 tests)
1. **Finds test data** - Auto-detects latest timestamp or uses provided one
2. **Analyzes existing files** - Reads changefeed data from Azure
3. **Runs coalescing** - Applies the current code logic to existing data
4. **Reports results** - Shows operation counts and validates expected values

### ❌ Skips (Compared to Full Test Mode)
- ❌ Table creation
- ❌ Changefeed creation
- ❌ Workload execution (UPDATEs, DELETEs, INSERTs)
- ❌ File syncing to Unity Catalog Volume
- ❌ Cleanup operations
- ❌ Wait times between operations

## Comparison

| Aspect | Full Test Mode | Validation Mode |
|--------|----------------|-----------------|
| **Duration** | ~30 minutes (8 tests) | ~30 seconds (8 tests) |
| **Creates changefeeds** | ✅ Yes | ❌ No |
| **Runs workloads** | ✅ Yes | ❌ No |
| **Tests analysis logic** | ✅ Yes | ✅ Yes |
| **Requires CockroachDB** | ✅ Yes | ❌ No (read-only) |
| **Modifies Azure data** | ✅ Yes | ❌ No (read-only) |
| **Use case** | Full integration test | Code iteration |

## Example Workflow

### Initial Test Run (Full Mode)
```bash
# Run full test suite once to generate baseline data
./test_cdc_matrix.sh
# Takes ~30 minutes, creates changefeeds, runs workloads
```

### Iterative Development (Validation Mode)
```bash
# 1. Make code changes to cockroachdb.py (e.g., fix coalescing logic)
vim ../cockroachdb.py

# 2. Test changes against existing data (fast!)
./test_cdc_matrix.sh --validate-only
# Takes ~30 seconds, validates all 8 scenarios

# 3. If tests pass, you're done!
# If tests fail, repeat steps 1-2
```

### Re-validate After Fixes
```bash
# After fixing PRIMARY_KEY_EXTRACTION_FIX.md issue:
./test_cdc_matrix.sh -v 1767895046

# Expected output:
# Validation 1/8: json_usertable_with_split - VALIDATED ✅
#   snap=9500 ins=50 upd=400 del=100
# Validation 2/8: json_usertable_no_split - VALIDATED ✅
#   snap=9500 ins=50 upd=400 del=100
# ... etc
```

## Expected Results

### For `usertable` tests (10,000 initial rows):
- **Snapshot:** 9,500 rows (after coalescing with UPDATEs and DELETEs)
- **Inserts:** 50 new rows
- **Updates:** 400 modified rows
- **Deletes:** 100 removed rows
- **Unique keys:** 9,950 (10,000 - 100 deleted + 50 inserted)

### For `simple_test` tests (1,000 initial rows):
- **Snapshot:** 950 rows (after coalescing)
- **Inserts:** 50 new rows
- **Updates:** 400 modified rows
- **Deletes:** 100 removed rows
- **Unique keys:** 950 (1,000 - 100 + 50)

## Troubleshooting

### "No test data found in Azure"
```bash
❌ No test data found in Azure
   Run a full test first: ./test_cdc_matrix.sh
```
**Solution:** Run the full test suite once to generate baseline data.

### "No files found at path"
```bash
⚠️  No files found at path: json/defaultdb/public/test-json_usertable_with_split/1767895046/
   Skipping validation for this test
```
**Cause:** The specified timestamp doesn't have data for that test combination.

**Solution:** Use `--validate-only` without a timestamp to auto-detect the latest run, or check available timestamps:
```bash
az storage blob list \
  --account-name <account> \
  --account-key <key> \
  --container-name changefeed-events \
  --query "[].name" | grep -oE '[0-9]{10}/' | sort -u
```

### Validation shows unexpected counts
```bash
⚠️  Unique keys (9900) ≠ Expected (9950)
```
**Cause:** The analysis logic may have a bug, or the test data is from an older run with different workload parameters.

**Solution:**
1. Verify you're using the correct timestamp
2. Check that the code fix addresses the right issue
3. Run a fresh full test if the data is stale

## Implementation Details

### How Timestamp Auto-Detection Works
```bash
# Finds the most recent timestamp across all test paths
az storage blob list \
  --prefix "json/" \
  --query "[].name" \
  | grep -oE '[0-9]{10}/' \
  | sort -u -r \
  | head -1
```

### What Gets Validated
The validation mode calls the same `analyze_azure_changefeed_files()` function used in production, ensuring:
- **Primary key extraction** is correct
- **Event classification** (SNAPSHOT/INSERT/UPDATE/DELETE) is accurate
- **Coalescing logic** properly deduplicates column family fragments
- **Operation counts** match expected values

## Related Files
- **Main script:** `test_cdc_matrix.sh`
- **Analysis function:** `cockroachdb.py::analyze_azure_changefeed_files()`
- **Changefeed helper:** `changefeed_helper.py`
- **Fix documentation:** `PRIMARY_KEY_EXTRACTION_FIX.md`

## Benefits

1. **Speed:** 60x faster than full test (30s vs. 30min)
2. **Safety:** Read-only, no side effects on database or changefeeds
3. **Reproducibility:** Test against the same data repeatedly
4. **Debugging:** Add print statements, re-run instantly
5. **CI/CD Ready:** Can be used in automated testing pipelines

## Future Enhancements

Potential improvements to validation mode:
- [ ] Support for specifying specific test combinations
- [ ] Comparison mode (before/after code changes)
- [ ] JSON export of validation results for CI/CD
- [ ] Performance benchmarking mode
- [ ] Validation against custom Azure paths

