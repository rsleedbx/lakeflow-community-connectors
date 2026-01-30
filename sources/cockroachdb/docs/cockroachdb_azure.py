"""
CockroachDB CDC Azure Utilities

This module provides Azure Blob Storage utilities for CockroachDB CDC changefeeds.
"""

import time
from typing import Dict, Any, List
from azure.storage.blob import BlobServiceClient


def check_azure_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    verbose: bool = True,
    format: str = "parquet"
) -> Dict[str, Any]:
    """
    Check for changefeed files in Azure Blob Storage.
    
    Args:
        storage_account_name: Azure storage account name
        storage_account_key: Azure storage account key
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        verbose: Print detailed output
        format: Changefeed format (default: "parquet")
    
    Returns:
        dict with 'data_files' and 'resolved_files' lists
    
    Example:
        >>> result = check_azure_files(
        ...     storage_account_name="mystorageaccount",
        ...     storage_account_key="<key>",
        ...     container_name="cockroachcdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable_append_only_multi_cf"
        ... )
        >>> print(f"Found {len(result['data_files'])} data files")
    """
    # Connect to Azure
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)
    
    # Build path - list ALL files recursively under this changefeed path
    prefix = f"{format}/{source_catalog}/{source_schema}/{source_table}/{target_table}/"
    
    # List all blobs recursively (no date filtering)
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    
    # Categorize files (using same filtering logic as cockroachdb.py)
    # Data files: .parquet files, excluding:
    #   - .RESOLVED files (CDC watermarks)
    #   - _metadata/ directory (schema files)
    #   - Files starting with _ (_SUCCESS, _committed_*, etc.)
    data_files = [
        b for b in blobs 
        if b.name.endswith('.parquet') 
        and '.RESOLVED' not in b.name
        and '/_metadata/' not in b.name
        and not b.name.split('/')[-1].startswith('_')
    ]
    resolved_files = [b for b in blobs if '.RESOLVED' in b.name]
    
    if verbose:
        print(f"📁 Files in Azure changefeed path:")
        print(f"   Path: {prefix}")
        print(f"   📄 Data files: {len(data_files)}")
        print(f"   🕐 Resolved files: {len(resolved_files)}")
        print(f"   📊 Total: {len(blobs)}")
        
        if data_files:
            print(f"\n   Example data file:")
            print(f"   {data_files[0].name}")
    
    return {
        'data_files': data_files,
        'resolved_files': resolved_files,
        'total': len(blobs)
    }


def wait_for_changefeed_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    max_wait: int = 120,
    check_interval: int = 5,
    stabilization_wait: int = 5,
    format: str = "parquet"
) -> bool:
    """
    Wait for changefeed files to appear in Azure with timeout and stabilization period.
    
    This function:
    1. Polls Azure until first file(s) appear
    2. Once files are detected, waits for additional files (important for column families)
    3. Exits when no new files appear for 'stabilization_wait' seconds
    
    Args:
        storage_account_name: Azure storage account name
        storage_account_key: Azure storage account key
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        max_wait: Maximum seconds to wait for initial files (default: 120)
        check_interval: Seconds between checks (default: 5)
        stabilization_wait: Seconds to wait for file count to stabilize (default: 5)
                           Important for column family mode where multiple files are written
        format: Changefeed format (default: "parquet")
    
    Returns:
        True if files found, False if timeout
    
    Example:
        >>> success = wait_for_changefeed_files(
        ...     storage_account_name="mystorageaccount",
        ...     storage_account_key="<key>",
        ...     container_name="cockroachcdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable_append_only_multi_cf",
        ...     max_wait=300,
        ...     check_interval=5,
        ...     stabilization_wait=5
        ... )
        >>> if success:
        ...     print("Files are ready for ingestion!")
    """
    print(f"⏳ Waiting for initial snapshot files to appear in Azure...")
    
    elapsed = 0
    files_found = False
    last_file_count = 0
    stable_elapsed = 0
    
    while elapsed < max_wait:
        result = check_azure_files(
            storage_account_name, storage_account_key, container_name,
            source_catalog, source_schema, source_table, target_table,
            verbose=False,
            format=format
        )
        
        current_file_count = len(result['data_files'])
        
        if not files_found and current_file_count > 0:
            # First files detected - switch to stabilization mode
            files_found = True
            last_file_count = current_file_count
            stable_elapsed = 0
            print(f"\n✅ First files appeared after {elapsed} seconds!")
            print(f"   Found {current_file_count} file(s) so far...")
            print(f"   Waiting {stabilization_wait}s for more files (column family fragments)...")
        
        elif files_found:
            # In stabilization mode - check if file count is stable
            if current_file_count > last_file_count:
                # More files arrived - reset stabilization timer
                print(f"   📄 File count increased: {last_file_count} → {current_file_count}")
                last_file_count = current_file_count
                stable_elapsed = 0
            else:
                # File count unchanged - increment stabilization timer
                stable_elapsed += check_interval
                
                if stable_elapsed >= stabilization_wait:
                    # Stabilization period complete - all files have landed
                    print(f"\n✅ File count stable at {current_file_count} for {stabilization_wait}s")
                    print(f"   Total wait time: {elapsed + stable_elapsed}s")
                    print(f"   Example: {result['data_files'][0].name}")
                    return True
        
        if not files_found:
            print(f"   Checking... ({elapsed}s elapsed)", end='\r')
        
        time.sleep(check_interval)
        elapsed += check_interval
    
    if files_found:
        # Files were found but stabilization didn't complete within max_wait
        print(f"\n⚠️  Timeout after {max_wait}s (found {last_file_count} files but more may still be generating)")
    else:
        print(f"\n⚠️  Timeout after {max_wait}s - no files appeared")
    
    print(f"   Run Cell 11 to check manually")
    return files_found  # Return True if we found at least some files
