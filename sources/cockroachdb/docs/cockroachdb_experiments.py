"""
CockroachDB CDC Experiments

This module contains experimental functions for testing CockroachDB changefeed behavior,
particularly around Azure path handling and URI formats.
"""

import time
from typing import Dict, Any, Optional
from azure.storage.blob import BlobServiceClient


def test_azure_path_prefix(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    column_family_mode: str,
    get_cockroachdb_connection,
    storage_account_key_encoded: str
) -> Dict[str, Any]:
    """
    Test if CockroachDB supports path_prefix query parameter for Azure changefeeds.
    
    This tests whether using path_prefix as a query parameter (S3-style) works for Azure,
    even though documentation suggests it doesn't.
    
    Args:
        storage_account_name: Azure storage account name
        storage_account_key: Azure storage account key (decoded)
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        column_family_mode: "single_cf" or "multi_cf"
        get_cockroachdb_connection: Function that returns a CockroachDB connection
        storage_account_key_encoded: URL-encoded Azure storage account key
    
    Returns:
        Dict with test results:
        {
            'success': bool,
            'files_found_at_full_path': bool,
            'files_found_at_short_path': bool,
            'full_path': str,
            'short_path': str,
            'file_count': int,
            'sample_files': List[str],
            'test_job_id': int
        }
    """
    print("🧪 Testing Azure path_prefix parameter...")
    print("=" * 80)
    
    results = {
        'success': False,
        'files_found_at_full_path': False,
        'files_found_at_short_path': False,
        'full_path': '',
        'short_path': '',
        'file_count': 0,
        'sample_files': [],
        'test_job_id': None
    }
    
    # Step 1: Drop existing changefeed
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
    print(f"⏳ Waiting 3 seconds for job to stop...")
    time.sleep(3)
    
    # Step 2: Verify table exists
    print("\n" + "=" * 80)
    print("Verifying Table Exists")
    print("=" * 80)
    
    fully_qualified_table = f"{source_catalog}.{source_schema}.{source_table}"
    
    conn = get_cockroachdb_connection()
    try:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute(f"SELECT COUNT(*) FROM {fully_qualified_table} LIMIT 1")
            print(f"✅ Table exists: {fully_qualified_table}")
    except Exception as e:
        print(f"❌ ERROR: Table does not exist!")
        print(f"   Table: {fully_qualified_table}")
        print(f"   Error: {e}")
        print(f"\n💡 Solution: Run Cell 7 to create and populate the table first")
        results['success'] = False
        return results
    finally:
        conn.close()
    
    # Step 3: Create changefeed with path_prefix parameter
    print("\n" + "=" * 80)
    print("Creating Changefeed with path_prefix Query Parameter")
    print("=" * 80)
    
    # Construct URI with path_prefix parameter
    base_container_uri = f"azure://{container_name}/"
    path_prefix_value = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
    
    changefeed_path_test = f"{base_container_uri}?path_prefix={path_prefix_value}&AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key_encoded}"
    
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
CREATE CHANGEFEED FOR TABLE {fully_qualified_table}
INTO '{changefeed_path_test}'
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
                results['test_job_id'] = test_job_id
                print(f"   Job ID: {test_job_id}")
    finally:
        conn.close()
    
    # Step 4: Wait for files to appear
    wait_time = 30
    print(f"\n⏳ Waiting {wait_time} seconds for CDC files to appear...")
    time.sleep(wait_time)
    
    # Step 5: Check both possible paths
    print(f"\n📁 Checking Azure for files...")
    
    expected_path_full = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/"
    expected_path_short = f"parquet/{source_catalog}/{source_schema}/{source_table}/"
    
    results['full_path'] = expected_path_full
    results['short_path'] = expected_path_short
    
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)
    
    # Check full path (with target_table)
    blobs_full = list(container_client.list_blobs(name_starts_with=expected_path_full))
    # Check short path (without target_table)
    blobs_short = list(container_client.list_blobs(name_starts_with=expected_path_short))
    
    # Step 6: Report results
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
        
        results['success'] = True
        results['files_found_at_full_path'] = True
        results['file_count'] = len(blobs_full)
        results['sample_files'] = [blob.name for blob in blobs_full[:5]]
        
    elif blobs_short:
        print(f"❌ FAILED - Files found at SHORT path (truncated):")
        print(f"   {expected_path_short}")
        print(f"   File count: {len(blobs_short)}")
        print(f"\n   Sample files:")
        for blob in blobs_short[:3]:
            print(f"      {blob.name}")
        print(f"\n⚠️  path_prefix parameter did NOT work - CockroachDB still strips path")
        print(f"   Falling back to embedded path in URI")
        
        results['files_found_at_short_path'] = True
        results['file_count'] = len(blobs_short)
        results['sample_files'] = [blob.name for blob in blobs_short[:5]]
        
    else:
        print(f"⚠️  NO FILES FOUND at either path!")
        print(f"   Expected (full):  {expected_path_full}")
        print(f"   Expected (short): {expected_path_short}")
        print(f"\n   Wait longer or check changefeed status:")
        if results['test_job_id']:
            print(f"   SHOW CHANGEFEED JOB {results['test_job_id']};")
    
    # Step 7: Next steps
    print(f"\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    
    if results['success']:
        print(f"1. ✅ Update notebook Cell 8 to use path_prefix parameter")
        print(f"2. ✅ Update cockroachdb_autoload.py to match new path")
        print(f"3. ✅ Document this as the solution")
    else:
        print(f"1. Drop this test changefeed: CANCEL JOB {results['test_job_id']};")
        print(f"2. Try alternative approaches (see AZURE_PATH_TEST_PLAN.md)")
        print(f"3. Or file CockroachDB bug report:")
        print(f"   https://github.com/cockroachdb/cockroach/issues/new")
    
    return results


def test_shallow_path_structure(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    column_family_mode: str,
    get_cockroachdb_connection,
    storage_account_key_encoded: str,
    depth: int = 2
) -> Dict[str, Any]:
    """
    Test if using a shallower path structure (2-3 levels) works better than 5 levels.
    
    Args:
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        depth: Path depth to test (2 or 3)
            - depth=2: parquet/table_target (2 levels)
            - depth=3: parquet/schema/table_target (3 levels)
    
    Returns:
        Dict with test results similar to test_azure_path_prefix()
    """
    print(f"🧪 Testing Shallow Path Structure (depth={depth})...")
    print("=" * 80)
    
    # Construct path based on depth
    if depth == 2:
        path = f"parquet/{source_table}_{target_table}"
    elif depth == 3:
        # Assuming public schema for this test
        path = f"parquet/public/{source_table}_{target_table}"
    else:
        raise ValueError(f"Unsupported depth: {depth}. Use 2 or 3.")
    
    print(f"\n📝 Testing path structure:")
    print(f"   Path: {path}")
    print(f"   Depth: {depth} levels")
    
    changefeed_path = f"azure://{container_name}/{path}?AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key_encoded}"
    
    # Build changefeed options
    if column_family_mode == "multi_cf":
        options = """
    format='parquet',
    updated,
    resolved='10s',
    split_column_families
"""
    else:
        options = """
    format='parquet',
    updated,
    resolved='10s'
"""
    
    # Use fully qualified table name
    fully_qualified_table = f"{source_catalog}.{source_schema}.{source_table}"
    create_sql = f"""
CREATE CHANGEFEED FOR TABLE {fully_qualified_table}
INTO '{changefeed_path}'
WITH {options}
"""
    
    print(f"\n🔧 Creating changefeed...")
    conn = get_cockroachdb_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            print(f"✅ Changefeed created")
            
            cur.execute(f"""
                SELECT job_id FROM [SHOW CHANGEFEED JOBS]
                WHERE description LIKE '%{source_table}%'
                AND status = 'running'
                ORDER BY created DESC
                LIMIT 1
            """)
            result = cur.fetchone()
            test_job_id = result[0] if result else None
            print(f"   Job ID: {test_job_id}")
    finally:
        conn.close()
    
    # Wait and check
    wait_time = 30
    print(f"\n⏳ Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    
    print(f"\n📁 Checking Azure for files at: {path}/")
    
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)
    
    blobs = list(container_client.list_blobs(name_starts_with=f"{path}/"))
    
    results = {
        'success': len(blobs) > 0,
        'path': path,
        'depth': depth,
        'file_count': len(blobs),
        'sample_files': [blob.name for blob in blobs[:5]],
        'test_job_id': test_job_id
    }
    
    if blobs:
        print(f"✅ SUCCESS! Found {len(blobs)} files at depth {depth}")
        for blob in blobs[:3]:
            print(f"   {blob.name}")
    else:
        print(f"❌ No files found at depth {depth}")
        print(f"   Job ID: {test_job_id}")
    
    return results
