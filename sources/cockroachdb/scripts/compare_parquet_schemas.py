#!/usr/bin/env python3
"""
Compare CockroachDB Parquet schema encodings between two data sources.

This script verifies Hypothesis B: Different CockroachDB instances encode
__crdb__updated timestamps differently (INT64 vs DECIMAL).

Usage:
    # Default (uses sources/cockroachdb/.env/cockroachdb_cdc_azure.json)
    python compare_parquet_schemas.py
    
    # With custom config
    python compare_parquet_schemas.py --config /path/to/config.json
    
    # With custom data paths
    python compare_parquet_schemas.py \
        --blog-path "parquet/defaultdb/public/usertable/2026-01-27" \
        --test-path "parquet/defaultdb/public/test-parquet_usertable_with_split"

Requirements:
    pip install pyarrow azure-storage-blob

Expected Output:
    ✅ DIFFERENT encodings detected!
       Blog:  decimal(2147483647, 0)
       Test:  int64
       ✅ Hypothesis B is CONFIRMED!
"""

import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any

try:
    import pyarrow.parquet as pq
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("❌ Missing dependencies. Install with:")
    print("   pip install pyarrow azure-storage-blob")
    sys.exit(1)


def load_azure_config(config_path: str = None) -> Dict[str, Any]:
    """Load Azure credentials from JSON file."""
    if config_path is None:
        # Default path
        script_dir = Path(__file__).parent.parent
        config_path = script_dir / ".env" / "cockroachdb_cdc_azure.json"
    
    config_path = Path(config_path)
    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        print(f"   Looking for: {config_path.absolute()}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return json.load(f)


def check_crdb_updated_type(
    container_client,
    path_prefix: str,
    label: str
) -> Optional[Any]:
    """
    Check __crdb__updated type in first Parquet file at path.
    
    Args:
        container_client: Azure BlobServiceClient container client
        path_prefix: Path prefix to search (e.g., "parquet/defaultdb/public/usertable/2026-01-27")
        label: Label for output (e.g., "Blog" or "Test")
    
    Returns:
        PyArrow field type or None if not found
    """
    try:
        # List blobs with prefix
        blob_list = list(container_client.list_blobs(name_starts_with=path_prefix))
        
        # Find Parquet files
        parquet_files = [blob for blob in blob_list if blob.name.endswith('.parquet')]
        
        if not parquet_files:
            print(f"❌ {label}: No Parquet files found at {path_prefix}")
            return None
        
        # Read first Parquet file
        first_blob = parquet_files[0]
        print(f"   📄 Reading: {first_blob.name}")
        
        blob_client = container_client.get_blob_client(first_blob.name)
        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall()
        
        # Parse Parquet schema
        import io
        table = pq.read_table(io.BytesIO(blob_data))
        
        # Find __crdb__updated column
        for field in table.schema:
            if field.name == "__crdb__updated":
                return field.type
        
        print(f"⚠️  {label}: __crdb__updated column not found in schema")
        return None
        
    except Exception as e:
        print(f"❌ {label}: Error - {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Main comparison logic."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Compare CockroachDB Parquet schema encodings"
    )
    parser.add_argument(
        '--config',
        help='Path to cockroachdb_cdc_azure.json config file',
        default=None
    )
    parser.add_argument(
        '--blog-path',
        help='Path prefix for blog data',
        default='parquet/defaultdb/public/usertable/2026-01-27'
    )
    parser.add_argument(
        '--test-path',
        help='Path prefix for test data',
        default='parquet/defaultdb/public/test-parquet_usertable_with_split'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    print("📋 Loading Azure credentials...")
    config = load_azure_config(args.config)
    
    storage_account = config['azure_storage_account']
    storage_key = config['azure_storage_key']
    container_name = config['azure_storage_container']
    
    print(f"   Storage Account: {storage_account}")
    print(f"   Container: {container_name}")
    
    # Connect to Azure Blob Storage
    print("\n🔗 Connecting to Azure Blob Storage...")
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={storage_account};"
        f"AccountKey={storage_key};"
        f"EndpointSuffix=core.windows.net"
    )
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Run comparison
    print("\n" + "=" * 80)
    print("🔍 COMPARING COCKROACHDB PARQUET ENCODINGS")
    print("=" * 80)
    
    # Check blog post data
    print(f"\n📄 Blog Post Data: {args.blog_path}")
    blog_type = check_crdb_updated_type(container_client, args.blog_path, "Blog")
    
    if blog_type:
        print(f"   __crdb__updated type: {blog_type}")
        if 'decimal' in str(blog_type).lower():
            print(f"   ⚠️  DECIMAL detected!")
            if hasattr(blog_type, 'precision'):
                print(f"   Precision: {blog_type.precision}, Scale: {blog_type.scale}")
                if blog_type.precision > 38:
                    print(f"   ❌ Exceeds Spark max precision (38) - THIS IS THE ERROR!")
        else:
            print(f"   ✅ Not DECIMAL - should work fine")
    
    # Check test scenario data
    print(f"\n📄 Test Scenario Data: {args.test_path}")
    test_type = check_crdb_updated_type(container_client, args.test_path, "Test")
    
    if test_type:
        print(f"   __crdb__updated type: {test_type}")
        if 'decimal' in str(test_type).lower():
            print(f"   ⚠️  DECIMAL detected!")
            if hasattr(test_type, 'precision'):
                print(f"   Precision: {test_type.precision}, Scale: {test_type.scale}")
                if test_type.precision > 38:
                    print(f"   ❌ Exceeds Spark max precision (38)")
        else:
            print(f"   ✅ Not DECIMAL - explains why it works!")
    
    # Compare results
    print(f"\n" + "=" * 80)
    print(f"📊 COMPARISON RESULTS")
    print("=" * 80)
    
    if blog_type and test_type:
        if str(blog_type) == str(test_type):
            print(f"\n❌ SAME encoding: {blog_type}")
            print(f"   Hypothesis B is FALSE - encoding is the same!")
            print(f"   Need to investigate other hypotheses...")
            return 1
        else:
            print(f"\n✅ DIFFERENT encodings detected!")
            print(f"   Blog:  {blog_type}")
            print(f"   Test:  {test_type}")
            print(f"\n✅ Hypothesis B is CONFIRMED!")
            print(f"   Different CockroachDB instances use different encodings.")
            print(f"\n💡 SOLUTION OPTIONS:")
            print(f"   1. Use CockroachDB version that encodes as {test_type}")
            print(f"   2. Use JSON format instead of Parquet (no DECIMAL issue)")
            print(f"   3. Report to CockroachDB about Parquet encoding compatibility")
            return 0
    else:
        print(f"\n⚠️  Could not compare - check paths and try again")
        return 1
    
    print("=" * 80)


if __name__ == "__main__":
    sys.exit(main())
