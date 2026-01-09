# Timestamp-Based Path Isolation - Implementation Summary
**Date:** January 7, 2026  
**Status:** ✅ IMPLEMENTED

## What Was Implemented

Added unique run timestamp to changefeed paths to ensure data isolation between test runs.

## Changes Made

### 1. Test Script (`test_cdc_matrix.sh`)

**Line 17:** Generate unique timestamp at start of test run
```bash
# Generate unique test run timestamp for isolated test data
TEST_RUN_TIMESTAMP=$(date +%s)
```

**Line 341:** Include timestamp in path
```bash
# Before
local path_prefix="${format}/${catalog}/${schema}/test-${test_name}"

# After
local path_prefix="${format}/${catalog}/${schema}/test-${test_name}/${TEST_RUN_TIMESTAMP}"
```

### 2. Documentation Updated

**`CDC_FILE_ORGANIZATION.md`:**
- Updated path structure diagrams to include run timestamp
- Added new section "Run Timestamp for Data Isolation"
- Updated all path examples
- Updated path parsing examples

**`COALESCE_FIX_SUMMARY.md`:**
- Documented the timestamp-based solution
- Explained why this approach was chosen

**`DELETE_DOUBLING_ROOT_CAUSE.md`:**
- Updated with implemented solution
- Documented alternatives considered

## New Path Structure

### Before
```
format/catalog/schema/test-scenario/file.parquet
```

### After
```
format/catalog/schema/test-scenario/timestamp/file.parquet
```

### Example
```bash
# Test run at 1704672000 (2024-01-07 12:00:00 UTC)
parquet/defaultdb/public/test-json_usertable_with_split/1704672000/202512...parquet

# Test run at 1704672100 (2024-01-07 12:01:40 UTC)
parquet/defaultdb/public/test-json_usertable_with_split/1704672100/202512...parquet
```

## Benefits

✅ **Data Isolation**: Each test run has completely isolated data  
✅ **No Stale Data**: Tests always use fresh data from current run  
✅ **Deterministic**: Test results are reproducible  
✅ **Historical Analysis**: All test runs preserved for comparison  
✅ **No Cleanup Needed**: Runs don't interfere with each other  
✅ **Simple**: No complex cleanup logic or race conditions  

## How It Works

1. **Test starts**: `TEST_RUN_TIMESTAMP=$(date +%s)` generates unique ID (e.g., `1704672000`)
2. **Changefeed created**: Path includes timestamp: `parquet/.../test-scenario/1704672000/`
3. **CockroachDB writes**: All files go to timestamped directory
4. **Analysis runs**: Uses same `path_prefix` with timestamp
5. **Results**: Only includes data from current run

## Impact on Existing Code

### Minimal Changes Required
- ✅ Test script: 2 lines changed
- ✅ Analysis: No changes (uses `${path_prefix}` variable)
- ✅ Connector code: No changes needed
- ✅ Notebooks: Will work with new paths automatically

### Path Parsing Updates
If you have code that parses paths, update from 4 parts to 5:

```python
# Old parsing (4 parts)
format, catalog, schema, filename = path.split('/')

# New parsing (5 parts)
format, catalog, schema, run_timestamp, filename = path.split('/')
```

## Testing the Fix

### Before Running Tests
No cleanup needed! Just run the test matrix:

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

### Expected Results
All 8 tests should show correct counts:
- `snap` ≈ expected (500 or 10000)
- `ins` = 50 (JSON) or 0 (Parquet)
- `upd` = 400 (JSON) or 450 (Parquet)
- `del` = 100 ✅ (not 200 anymore!)

### Verification
Each test run will create new timestamped directories:

```bash
# List Azure directories to see isolation
az storage blob directory list \
    --account-name "$AZURE_STORAGE_ACCOUNT" \
    --container changefeed-events \
    --prefix "json/defaultdb/public/test-json_usertable_with_split/"

# Output will show:
# test-json_usertable_with_split/1704672000/  (run 1)
# test-json_usertable_with_split/1704672100/  (run 2)
# test-json_usertable_with_split/1704672200/  (run 3)
```

## Storage Management

### Growth Over Time
Each test run creates a new directory. To manage storage:

**Option 1: Manual Cleanup**
```bash
# Delete old test runs, keep only recent ones
az storage blob delete-batch \
    --account-name "$AZURE_STORAGE_ACCOUNT" \
    --source changefeed-events \
    --pattern "*/test-*" \
    --if-unmodified-since "2024-01-01" \
    --auth-mode key
```

**Option 2: Azure Lifecycle Policy**
Set up automatic deletion of old test data:

```json
{
  "rules": [{
    "name": "DeleteOldTestData",
    "type": "Lifecycle",
    "definition": {
      "filters": {
        "blobTypes": ["blockBlob"],
        "prefixMatch": ["json/defaultdb/public/test-", "parquet/defaultdb/public/test-"]
      },
      "actions": {
        "baseBlob": {
          "delete": {
            "daysAfterModificationGreaterThan": 30
          }
        }
      }
    }
  }]
}
```

**Option 3: Keep Everything**
Test data is relatively small. A year of daily test runs ≈ 365 directories ≈ low storage cost.

## Comparison with Other Approaches

| Approach | Isolation | History | Cleanup | Complexity | Selected |
|----------|-----------|---------|---------|------------|----------|
| Timestamp paths | ✅ Perfect | ✅ Yes | ❌ Manual | ✅ Simple | ✅ Yes |
| Azure cleanup | ✅ Perfect | ❌ No | ✅ Auto | ⚠️ Medium | ❌ No |
| Cumulative data | ❌ None | ✅ Yes | ✅ None | ✅ Simple | ❌ No |

## Files Modified

1. **`sources/cockroachdb/scripts/test_cdc_matrix.sh`**
   - Line 17: Added `TEST_RUN_TIMESTAMP` variable
   - Line 341: Updated `path_prefix` to include timestamp

2. **`sources/cockroachdb/notebooks/CDC_FILE_ORGANIZATION.md`**
   - Updated path structure documentation
   - Added run timestamp benefits section
   - Updated all examples

3. **`sources/cockroachdb/COALESCE_FIX_SUMMARY.md`**
   - Documented implemented solution

4. **`sources/cockroachdb/DELETE_DOUBLING_ROOT_CAUSE.md`**
   - Updated with solution details

5. **`sources/cockroachdb/TIMESTAMP_PATH_IMPLEMENTATION.md`** (this file)
   - Implementation guide

## Next Steps

1. ✅ **Implementation Complete** - Code changes applied
2. ✅ **Documentation Updated** - All docs reflect new structure
3. ⏭️ **Run Test Matrix** - Verify fix works with clean data
4. ⏭️ **Monitor Storage** - Set up lifecycle policy if needed

## Success Criteria

After running the test matrix with this fix:

✅ All 8 tests show correct operation counts  
✅ `del=100` for all tests (not 200)  
✅ Each run has isolated Azure directory  
✅ Tests are deterministic and reproducible  

## Questions?

**Q: Will this work with production changefeeds?**  
A: Yes! The timestamp is only added to test scenarios. Production paths remain: `format/catalog/schema/table_name/`

**Q: How do I analyze a specific test run?**  
A: Use the full path including timestamp:
```python
analyze_azure_changefeed_files(
    path_prefix="json/defaultdb/public/test-scenario/1704672000/"
)
```

**Q: Can I still compare across test runs?**  
A: Yes! List all timestamps and analyze each:
```bash
# Find all test runs
az storage blob directory list \
    --container changefeed-events \
    --prefix "json/defaultdb/public/test-scenario/"
```

**Q: Do I need to delete old test data?**  
A: Not urgent. Test data is small. Consider lifecycle policies for automatic cleanup after 30+ days.

## References

- Test script: `sources/cockroachdb/scripts/test_cdc_matrix.sh`
- Path docs: `sources/cockroachdb/notebooks/CDC_FILE_ORGANIZATION.md`
- Root cause: `sources/cockroachdb/DELETE_DOUBLING_ROOT_CAUSE.md`
- Fix summary: `sources/cockroachdb/COALESCE_FIX_SUMMARY.md`

