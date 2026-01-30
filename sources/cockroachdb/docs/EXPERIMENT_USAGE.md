# Experiment Usage Guide

## Quick Test - Add to Notebook

### Test 1: `path_prefix` Query Parameter

Add this cell after Cell 8 in the notebook:

```python
# ============================================================================
# EXPERIMENT 1: Test path_prefix parameter for Azure
# ============================================================================

from cockroachdb_experiments import test_azure_path_prefix

# Run test
results = test_azure_path_prefix(
    storage_account_name=storage_account_name,
    storage_account_key=storage_account_key,
    container_name=container_name,
    source_catalog=source_catalog,
    source_schema=source_schema,
    source_table=source_table,
    target_table=target_table,
    column_family_mode=column_family_mode,
    get_cockroachdb_connection=get_cockroachdb_connection,
    storage_account_key_encoded=storage_account_key_encoded
)

# Check results
if results['success']:
    print("\n" + "=" * 80)
    print("✅ EXPERIMENT SUCCESS - Use path_prefix parameter!")
    print("=" * 80)
    print(f"Files found at: {results['full_path']}")
    print(f"Count: {results['file_count']}")
else:
    print("\n" + "=" * 80)
    print("❌ EXPERIMENT FAILED - path_prefix doesn't work")
    print("=" * 80)
    if results['files_found_at_short_path']:
        print(f"Files found at truncated path: {results['short_path']}")
    else:
        print("No files found - wait longer or check changefeed")
```

---

### Test 2: Shallow Path Structure

If Test 1 fails, try this:

```python
# ============================================================================
# EXPERIMENT 2: Test shallow path structure (2 levels)
# ============================================================================

from cockroachdb_experiments import test_shallow_path_structure

# Cancel previous test changefeed first
conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        cur.execute(f"CANCEL JOB {results['test_job_id']}")
        print(f"✅ Cancelled test changefeed")
finally:
    conn.close()

import time
time.sleep(3)

# Run shallow path test
results2 = test_shallow_path_structure(
    storage_account_name=storage_account_name,
    storage_account_key=storage_account_key,
    container_name=container_name,
    source_catalog=source_catalog,
    source_schema=source_schema,
    source_table=source_table,
    target_table=target_table,
    column_family_mode=column_family_mode,
    get_cockroachdb_connection=get_cockroachdb_connection,
    storage_account_key_encoded=storage_account_key_encoded,
    depth=2  # Try 2-level path: parquet/table_target
)

if results2['success']:
    print(f"\n✅ Shallow path (depth {results2['depth']}) WORKS!")
    print(f"   Use: {results2['path']}")
else:
    print(f"\n❌ Shallow path (depth {results2['depth']}) also failed")
    
    # Try 3-level path
    print("\n🧪 Trying 3-level path...")
    results3 = test_shallow_path_structure(
        storage_account_name=storage_account_name,
        storage_account_key=storage_account_key,
        container_name=container_name,
        source_catalog=source_catalog,
        source_schema=source_schema,
        source_table=source_table,
        target_table=target_table,
        column_family_mode=column_family_mode,
        get_cockroachdb_connection=get_cockroachdb_connection,
        storage_account_key_encoded=storage_account_key_encoded,
        depth=3  # Try 3-level path: parquet/schema/table_target
    )
```

---

## One-Liner Test (Minimal)

Just want to quickly test? Add this single cell:

```python
from cockroachdb_experiments import test_azure_path_prefix

test_azure_path_prefix(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    column_family_mode, get_cockroachdb_connection, storage_account_key_encoded
)
```

The function will print results and return a dict with detailed info.

---

## Understanding Results

### Result Dictionary Structure

```python
{
    'success': True/False,              # Overall success
    'files_found_at_full_path': bool,   # Files at .../table/target/
    'files_found_at_short_path': bool,  # Files at .../table/
    'full_path': str,                   # Expected full path
    'short_path': str,                  # Expected short path
    'file_count': int,                  # Number of files found
    'sample_files': List[str],          # First 5 file names
    'test_job_id': int                  # Changefeed job ID
}
```

### Success Scenarios

**✅ Scenario 1**: `success=True` and `files_found_at_full_path=True`
- **Meaning**: `path_prefix` parameter works!
- **Action**: Update notebook to use this format permanently

**❌ Scenario 2**: `files_found_at_short_path=True`
- **Meaning**: Path is still being truncated
- **Action**: Try Test 2 (shallow paths) or file CockroachDB bug

**⚠️ Scenario 3**: Both `False`
- **Meaning**: No files found yet
- **Action**: Wait longer (check `test_job_id` status)

---

## Clean Up After Testing

```python
# Drop test changefeed
conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        # Cancel test job
        cur.execute(f"CANCEL JOB {results['test_job_id']}")
        print("✅ Test changefeed cancelled")
finally:
    conn.close()

# Delete test files from Azure
from azure.storage.blob import BlobServiceClient

connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
blob_service = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service.get_container_client(container_name)

# Delete files at test path
for blob in container_client.list_blobs(name_starts_with=results['full_path']):
    container_client.delete_blob(blob.name)
    print(f"Deleted: {blob.name}")

print("✅ Test files cleaned up")
```

---

## Next Steps Based on Results

### If `path_prefix` Works (Success)

1. **Update Cell 8** - Changefeed creation:
   ```python
   base_container = f"azure://{container_name}/"
   path_prefix = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
   changefeed_path = f"{base_container}?path_prefix={path_prefix}&AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..."
   ```

2. **Keep `cockroachdb_autoload.py` unchanged**:
   - Already uses correct path with `/{target_table}`
   - No changes needed!

3. **Update documentation**:
   - Add note that `path_prefix` works for Azure
   - Update comment in Cell 8

### If `path_prefix` Fails

**Option A**: Use shallower paths
```python
# Change from: parquet/db/schema/table/target (5 levels)
# To:          parquet/table_target           (2 levels)
```

**Option B**: File CockroachDB bug
- Use template from `AZURE_PATH_TEST_PLAN.md`
- Include test results
- Request fix or documentation update

**Option C**: Use workaround
- Separate Azure containers per target
- Or use timestamp-based filtering in Auto Loader

---

## Date

2026-01-30
