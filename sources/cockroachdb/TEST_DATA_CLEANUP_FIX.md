# Test Data Cleanup - Implementation Guide
**Date:** January 7, 2026  
**Purpose:** Fix the `del=200` issue by cleaning up stale Azure data before each test

## Problem

The test matrix shows `del=200` for `usertable` because Azure blob storage contains data from multiple test runs:
- Old run: deleted `user0000009901` through `user0000010000` 
- New run: deleted `user9065999390998836560` etc.
- Result: 200 unique deleted rows in storage → `del=200` in analysis

## Solution

Clean up Azure data for each test scenario before creating the changefeed.

## Implementation

### Option 1: Per-Scenario Cleanup (Recommended)

Add cleanup step right before changefeed creation in `test_cdc_matrix.sh`:

```bash
# Find this section (around line 555):
create_changefeed() {
    local table=$1
    local format=$2
    local split=$3
    
    echo ""
    echo "📡 Creating $format changefeed for $table (split=$split)..."
    
    # ADD THIS: Clean up previous test data for this scenario
    local test_name="${format}_${table}_${split}"
    local path_pattern="${format}/${database}/${schema}/test-${test_name}*"
    
    echo "   🧹 Cleaning up previous test data..."
    echo "      Pattern: ${path_pattern}"
    
    # Use Azure CLI to delete blobs matching pattern
    az storage blob delete-batch \
        --account-name "$azure_azure_storage_account" \
        --source "$azure_azure_storage_container" \
        --pattern "$path_pattern" \
        --auth-mode key \
        --output none \
        2>/dev/null || true  # Don't fail if no files exist
    
    echo "   ✅ Cleanup complete"
    
    # Existing changefeed creation code continues...
    local changefeed_name="test_${test_name}"
    # ...
}
```

### Option 2: Bulk Cleanup at Start

Add cleanup of ALL test data at the beginning of the test run:

```bash
# Add after loading Azure credentials (around line 275):

cleanup_all_test_data() {
    echo ""
    echo "🧹 Cleaning up ALL previous test data from Azure..."
    echo "   Container: $azure_azure_storage_container"
    echo "   Pattern: */test-*"
    
    # Count files before cleanup
    local file_count
    file_count=$(az storage blob list \
        --account-name "$azure_azure_storage_account" \
        --container-name "$azure_azure_storage_container" \
        --prefix "json/defaultdb/public/test-" \
        --query "length(@)" \
        --output tsv 2>/dev/null || echo "0")
    
    if [ "$file_count" -gt 0 ]; then
        echo "   Found $file_count JSON test files to clean"
        
        # Delete JSON test files
        az storage blob delete-batch \
            --account-name "$azure_azure_storage_account" \
            --source "$azure_azure_storage_container" \
            --pattern "json/*/test-*" \
            --auth-mode key \
            --output none \
            2>/dev/null || true
        
        # Delete Parquet test files  
        az storage blob delete-batch \
            --account-name "$azure_azure_storage_account" \
            --source "$azure_azure_storage_container" \
            --pattern "parquet/*/test-*" \
            --auth-mode key \
            --output none \
            2>/dev/null || true
        
        echo "   ✅ Cleanup complete"
    else
        echo "   ℹ️  No test files found to clean"
    fi
}

# Call cleanup before starting tests
cleanup_all_test_data
```

### Option 3: Unique Paths Per Run (Alternative)

Instead of cleaning up, use unique paths for each test run:

```bash
# At top of script, generate run ID
TEST_RUN_ID=$(date +%Y%m%d_%H%M%S)

# In create_changefeed function, modify path:
local test_name="${format}_${table}_${split}"
local path_formatted="${format}/${database}/${schema}/test-${test_name}-${TEST_RUN_ID}/"

# Pro: No cleanup needed, all test data preserved
# Con: Azure storage grows over time, analysis must specify correct run ID
```

## Recommended Approach

**Use Option 1 (Per-Scenario Cleanup)** because:
- ✅ Ensures clean data for each test scenario
- ✅ Only affects current test scenario (isolated)
- ✅ Doesn't interfere with other tests
- ✅ Simple to implement
- ✅ No storage growth over time

## Testing the Fix

1. Apply the cleanup code to `test_cdc_matrix.sh`
2. Run a single test to verify cleanup works:
   ```bash
   cd sources/cockroachdb/scripts
   # Run just one test scenario
   ./test_cdc_matrix.sh
   ```
3. Check the output - should see:
   ```
   🧹 Cleaning up previous test data...
      Pattern: json/defaultdb/public/test-json_usertable_with_split*
   ✅ Cleanup complete
   ```
4. After full test matrix completes, verify results:
   ```
   Test 1/8: json_usertable_with_split   - del=100 ✅ (was 200)
   Test 2/8: json_usertable_no_split     - del=100 ✅ (was 200)
   ```

## Verification

Use the fast diagnostic script to verify Azure data is clean:

```bash
cd sources/cockroachdb
python scripts/test_coalesce_fix.py
```

Expected output after cleanup:
```
✅ PASS: JSON usertable WITH split (del=100)
✅ PASS: JSON usertable NO split (del=100)
✅ PASS: JSON simple_test WITH split (del=100)
✅ PASS: JSON simple_test NO split (del=100)
...
Result: 8/8 tests passed 🎉
```

## Alternative: Azure CLI Installation

If `az` CLI is not available, use Python + Azure SDK:

```python
# Add to scripts/changefeed_helper.py
def cmd_cleanup_test_data(args):
    """Clean up test data from Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient
    
    conn_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={args.account_name};"
        f"AccountKey={args.account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    
    client = BlobServiceClient.from_connection_string(conn_string)
    container = client.get_container_client(args.container)
    
    # List and delete blobs matching pattern
    pattern = args.pattern  # e.g., "json/defaultdb/public/test-*"
    deleted_count = 0
    
    for blob in container.list_blobs(name_starts_with=pattern):
        container.delete_blob(blob.name)
        deleted_count += 1
    
    print(f"Deleted {deleted_count} blobs matching pattern: {pattern}")
```

Then call from shell script:
```bash
python3 changefeed_helper.py cleanup \
    --account-name "$azure_azure_storage_account" \
    --account-key "$azure_azure_storage_key" \
    --container "$azure_azure_storage_container" \
    --pattern "json/defaultdb/public/test-${test_name}"
```

## Next Steps

1. Choose cleanup approach (recommend Option 1)
2. Implement the cleanup code in `test_cdc_matrix.sh`
3. Test with a single scenario first
4. Run full test matrix
5. Verify all 8 tests show correct counts
6. Document the cleanup in the test README

## Success Criteria

✅ All 8 tests in matrix show correct operation counts:
- `snap` ≈ expected (500 or 10000)
- `ins` = 50 (JSON) or 0 (Parquet)
- `upd` = 400 (JSON) or 450 (Parquet)
- `del` = 100 ✅ (not 200)

✅ Fast diagnostic script shows 8/8 tests passed

✅ Azure storage only contains data from current test run

