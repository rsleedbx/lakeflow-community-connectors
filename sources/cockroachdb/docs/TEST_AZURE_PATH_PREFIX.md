# Test: Azure `path_prefix` Query Parameter

## Quick Test - Run in Notebook

Add this as a new cell after Cell 8 to test if `path_prefix` works for Azure:

```python
# ============================================================================
# TEST: Azure path_prefix Parameter
# ============================================================================

print("🧪 Testing Azure path_prefix parameter...")
print("=" * 80)

# Drop existing changefeed first
conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        # Find existing changefeed
        cur.execute(f"""
            SELECT job_id FROM [SHOW CHANGEFEED JOBS]
            WHERE description LIKE '%{source_table}%'
            AND status = 'running'
        """)
        result = cur.fetchone()
        
        if result:
            job_id = result[0]
            print(f"📋 Dropping existing changefeed: Job ID {job_id}")
            cur.execute(f"CANCEL JOB {job_id}")
            print(f"✅ Changefeed dropped")
        else:
            print(f"ℹ️  No existing changefeed found")
finally:
    conn.close()

# Wait for job to fully stop
import time
time.sleep(3)

# ============================================================================
# Test Approach 1: path_prefix as Query Parameter (S3-style for Azure)
# ============================================================================

print("\n" + "=" * 80)
print("Test 1: Using path_prefix query parameter")
print("=" * 80)

# Construct URI with path_prefix parameter
base_container_uri = f"azure://{container_name}/"
path_prefix_value = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"

changefeed_path_test1 = f"{base_container_uri}?path_prefix={path_prefix_value}&AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key_encoded}"

print(f"\n📝 Test URI:")
print(f"   Base: azure://{container_name}/")
print(f"   path_prefix: {path_prefix_value}")
print(f"   Expected path: {path_prefix_value}/")

# Build changefeed options
if column_family_mode == "multi_cf":
    test_options = """
    format='parquet',
    updated,
    resolved='10s',
    split_column_families
"""
else:
    test_options = """
    format='parquet',
    updated,
    resolved='10s'
"""

# Create changefeed with path_prefix
create_test_sql = f"""
CREATE CHANGEFEED FOR TABLE {source_table}
INTO '{changefeed_path_test1}'
WITH {test_options}
"""

print(f"\n🔧 Creating test changefeed...")

conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        cur.execute(create_test_sql)
        print(f"✅ Changefeed created with path_prefix parameter")
        
        # Get job ID
        cur.execute(f"""
            SELECT job_id FROM [SHOW CHANGEFEED JOBS]
            WHERE description LIKE '%{source_table}%'
            AND status = 'running'
            ORDER BY created DESC
            LIMIT 1
        """)
        result = cur.fetchone()
        if result:
            test_job_id = result[0]
            print(f"   Job ID: {test_job_id}")
finally:
    conn.close()

# Wait for files to appear
print(f"\n⏳ Waiting 30 seconds for CDC files to appear...")
time.sleep(30)

# Check both possible paths
print(f"\n📁 Checking Azure for files...")

expected_path_full = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/"
expected_path_short = f"parquet/{source_catalog}/{source_schema}/{source_table}/"

from azure.storage.blob import BlobServiceClient

connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
blob_service = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service.get_container_client(container_name)

# Check full path (with target_table)
blobs_full = list(container_client.list_blobs(name_starts_with=expected_path_full))
# Check short path (without target_table)
blobs_short = list(container_client.list_blobs(name_starts_with=expected_path_short))

print(f"\n" + "=" * 80)
print("TEST RESULTS")
print("=" * 80)

if blobs_full:
    print(f"✅ SUCCESS! Files found at FULL path:")
    print(f"   {expected_path_full}")
    print(f"   File count: {len(blobs_full)}")
    print(f"\n   Sample files:")
    for blob in blobs_full[:3]:
        print(f"      {blob.name}")
    print(f"\n🎉 path_prefix parameter WORKS for Azure!")
    print(f"   CockroachDB honored the full nested path structure")
elif blobs_short:
    print(f"❌ FAILED - Files found at SHORT path (truncated):")
    print(f"   {expected_path_short}")
    print(f"   File count: {len(blobs_short)}")
    print(f"\n   Sample files:")
    for blob in blobs_short[:3]:
        print(f"      {blob.name}")
    print(f"\n⚠️  path_prefix parameter did NOT work - CockroachDB still strips path")
    print(f"   Falling back to embedded path in URI")
else:
    print(f"⚠️  NO FILES FOUND at either path!")
    print(f"   Expected (full):  {expected_path_full}")
    print(f"   Expected (short): {expected_path_short}")
    print(f"\n   Wait longer or check changefeed status:")
    print(f"   SHOW CHANGEFEED JOB {test_job_id};")

print(f"\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)

if blobs_full:
    print(f"1. Update notebook Cell 8 to use path_prefix parameter")
    print(f"2. Update cockroachdb_autoload.py to match new path")
    print(f"3. Document this as the solution")
else:
    print(f"1. Drop this test changefeed: CANCEL JOB {test_job_id};")
    print(f"2. Try Test 2: Shallower path structure")
    print(f"3. Or file CockroachDB bug report")
    print(f"   https://github.com/cockroachdb/cockroach/issues/new")
```

---

## What This Test Does

1. **Drops existing changefeed** to start clean
2. **Creates new changefeed** with `path_prefix` as a query parameter (S3-style)
3. **Waits 30 seconds** for files to appear
4. **Checks BOTH paths**:
   - Full path: `parquet/db/schema/table/target_table/` (SUCCESS if files here)
   - Short path: `parquet/db/schema/table/` (FAILURE if files here)
5. **Reports results** with clear next steps

---

## Expected Outcomes

### Outcome A: ✅ SUCCESS - `path_prefix` Works
```
✅ SUCCESS! Files found at FULL path:
   parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/
   
🎉 path_prefix parameter WORKS for Azure!
```

**Action**: Update notebook to use `path_prefix` parameter permanently

### Outcome B: ❌ FAILURE - Path Still Truncated
```
❌ FAILED - Files found at SHORT path (truncated):
   parquet/defaultdb/public/usertable_append_only_multi_cf/
   
⚠️  path_prefix parameter did NOT work
```

**Action**: Either:
1. Try flatter path structure
2. File CockroachDB bug
3. Use workaround (separate containers or timestamp-based filtering)

### Outcome C: ⚠️ No Files
```
⚠️  NO FILES FOUND at either path!
```

**Action**: Wait longer or check changefeed status

---

## Date

2026-01-30
