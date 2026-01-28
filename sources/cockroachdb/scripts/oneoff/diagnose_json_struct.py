#!/usr/bin/env python3
"""
Diagnose JSON struct representation in CockroachDB CDC files.

This script reads JSON CDC files directly (local or Azure blob) and inspects 
the structure of 'before' and 'after' fields to understand:
1. Are they None/null or empty dict {}?
2. How does Python represent them vs how Spark might?
3. What are the actual values for each CDC operation type?

Usage:
    # Local file:
    python diagnose_json_struct.py <path_to_json_file>
    
    # Azure blob:
    python diagnose_json_struct.py wasbs://path/to/file.ndjson
    
Example:
    python diagnose_json_struct.py wasbs://cockroachdb@databrickslakeflow.blob.core.windows.net/json/defaultdb/public/test-json_usertable_no_split/1767823340/202601072206035945830580000000001-d07db17c86874707-1-37369-00000001-test_json_usertable_no_split-1.ndjson
"""

import json
import sys
import os
from typing import Dict, Any, List
from collections import defaultdict
from io import BytesIO

def inspect_json_record(record: Dict[str, Any], record_num: int) -> Dict[str, Any]:
    """
    Inspect a single JSON record and extract diagnostic info.
    
    Returns:
        Dict with diagnostic information about the record
    """
    before = record.get('before')
    after = record.get('after')
    updated = record.get('updated')
    key = record.get('key')
    
    # Determine CDC operation (using Python logic - same as batch analysis)
    if after and not before:
        operation = 'SNAPSHOT/INSERT'
    elif after and before:
        operation = 'UPDATE'
    elif before and not after:
        operation = 'DELETE'
    else:
        operation = 'UNKNOWN'
    
    # Get detailed type information
    before_info = {
        'value': before,
        'type': type(before).__name__,
        'is_none': before is None,
        'is_empty_dict': before == {},
        'bool_value': bool(before),
        'repr': repr(before)[:100] if before else repr(before),
        'json_dumps': json.dumps(before) if before is not None else 'null'
    }
    
    after_info = {
        'value': after,
        'type': type(after).__name__,
        'is_none': after is None,
        'is_empty_dict': after == {},
        'bool_value': bool(after),
        'repr': repr(after)[:100] if after else repr(after),
        'json_dumps': json.dumps(after) if after is not None else 'null'
    }
    
    return {
        'record_num': record_num,
        'operation': operation,
        'updated': updated,
        'key': key,
        'before': before_info,
        'after': after_info
    }

def read_file_content(file_path: str) -> List[str]:
    """
    Read file content from local path or Azure blob.
    
    Args:
        file_path: Local path or Azure wasbs:// URL
        
    Returns:
        List of lines from the file
    """
    if file_path.startswith('wasbs://'):
        # Azure blob storage
        from azure.storage.blob import BlobServiceClient
        
        # Parse Azure path: wasbs://container@account.blob.core.windows.net/path/to/file
        parts = file_path.replace('wasbs://', '').split('/')
        container_and_account = parts[0]
        blob_path = '/'.join(parts[1:])
        
        container = container_and_account.split('@')[0]
        account_url = f"https://{container_and_account.split('@')[1]}"
        
        # Get credentials from environment
        account_key = os.environ.get('AZURE_STORAGE_KEY')
        if not account_key:
            raise ValueError("AZURE_STORAGE_KEY environment variable not set")
        
        # Download blob
        blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)
        
        print(f"📥 Downloading from Azure blob storage...")
        blob_data = blob_client.download_blob()
        content = blob_data.readall().decode('utf-8')
        return content.splitlines()
    else:
        # Local file
        with open(file_path, 'r') as f:
            return f.readlines()

def analyze_json_file(file_path: str, max_samples: int = 5):
    """
    Analyze a JSON CDC file and print diagnostic information.
    
    Args:
        file_path: Path to the JSON file (local or wasbs://)
        max_samples: Maximum number of samples to show per operation type
    """
    print("=" * 80)
    print("JSON STRUCT DIAGNOSTIC ANALYSIS")
    print("=" * 80)
    print(f"\n📁 File: {file_path}\n")
    
    # Collect samples by operation type
    samples_by_op = defaultdict(list)
    operation_counts = defaultdict(int)
    
    # Read file content
    try:
        lines = read_file_content(file_path)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        raise
    
    # Analyze lines
    for i, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
            
        try:
            record = json.loads(line)
            info = inspect_json_record(record, i)
            
            operation = info['operation']
            operation_counts[operation] += 1
            
            # Collect samples (max per operation)
            if len(samples_by_op[operation]) < max_samples:
                samples_by_op[operation].append(info)
                
        except json.JSONDecodeError as e:
            print(f"⚠️  Line {i}: JSON decode error: {e}")
            continue
    
    # Print summary
    print("\n" + "=" * 80)
    print("OPERATION COUNTS")
    print("=" * 80)
    total = sum(operation_counts.values())
    for op, count in sorted(operation_counts.items()):
        pct = (count / total * 100) if total > 0 else 0
        print(f"  {op:20s}: {count:6d} ({pct:5.1f}%)")
    print(f"  {'TOTAL':20s}: {total:6d}")
    
    # Print detailed samples for each operation type
    for operation in ['SNAPSHOT/INSERT', 'UPDATE', 'DELETE', 'UNKNOWN']:
        if operation not in samples_by_op:
            continue
            
        samples = samples_by_op[operation]
        if not samples:
            continue
        
        print("\n" + "=" * 80)
        print(f"{operation} SAMPLES (showing {len(samples)} of {operation_counts[operation]})")
        print("=" * 80)
        
        for i, info in enumerate(samples, 1):
            print(f"\n📋 Sample #{i} (record #{info['record_num']})")
            print(f"   Operation: {info['operation']}")
            print(f"   Updated: {info['updated']}")
            print(f"   Key: {info['key']}")
            
            print(f"\n   🔵 BEFORE:")
            print(f"      Type: {info['before']['type']}")
            print(f"      Is None: {info['before']['is_none']}")
            print(f"      Is Empty Dict ({{}}): {info['before']['is_empty_dict']}")
            print(f"      Bool Value: {info['before']['bool_value']}")
            print(f"      Repr: {info['before']['repr']}")
            print(f"      JSON dumps: {info['before']['json_dumps']}")
            
            print(f"\n   🟢 AFTER:")
            print(f"      Type: {info['after']['type']}")
            print(f"      Is None: {info['after']['is_none']}")
            print(f"      Is Empty Dict ({{}}): {info['after']['is_empty_dict']}")
            print(f"      Bool Value: {info['after']['bool_value']}")
            print(f"      Repr: {info['after']['repr']}")
            print(f"      JSON dumps: {info['after']['json_dumps']}")
            
            print(f"\n   ✅ Python Logic Check:")
            after = info['after']['value']
            before = info['before']['value']
            if after and not before:
                print(f"      ✅ Would classify as: SNAPSHOT/INSERT")
            elif after and before:
                print(f"      ✅ Would classify as: UPDATE")
            elif before and not after:
                print(f"      ✅ Would classify as: DELETE")
            else:
                print(f"      ❌ Would classify as: UNKNOWN")
    
    # Print key insights
    print("\n" + "=" * 80)
    print("KEY INSIGHTS FOR SPARK IMPLEMENTATION")
    print("=" * 80)
    
    # Analyze DELETE events specifically
    if 'DELETE' in samples_by_op:
        print("\n🔍 DELETE Events Analysis:")
        for sample in samples_by_op['DELETE']:
            before = sample['before']
            after = sample['after']
            
            print(f"\n   Record #{sample['record_num']}:")
            print(f"      before: is_none={before['is_none']}, is_empty={before['is_empty_dict']}, type={before['type']}")
            print(f"      after:  is_none={after['is_none']}, is_empty={after['is_empty_dict']}, type={after['type']}")
            
            # What would Spark see?
            print(f"\n   🎯 What Spark would see:")
            print(f"      F.to_json(before) → \"{before['json_dumps']}\"")
            print(f"      F.to_json(after) → \"{after['json_dumps']}\"")
            
            # Our fix logic
            before_empty_check = (before['json_dumps'] == "null") or (before['json_dumps'] == "{}")
            after_empty_check = (after['json_dumps'] == "null") or (after['json_dumps'] == "{}")
            
            print(f"\n   ✅ Our fix logic:")
            print(f"      before_empty = (to_json == 'null') | (to_json == '{{}}') → {before_empty_check}")
            print(f"      after_empty  = (to_json == 'null') | (to_json == '{{}}') → {after_empty_check}")
            print(f"      Would match DELETE pattern? {(not after_empty_check) and before_empty_check or after_empty_check and (not before_empty_check)}")
    
    # Analyze SNAPSHOT/INSERT events
    if 'SNAPSHOT/INSERT' in samples_by_op:
        print(f"\n🔍 SNAPSHOT/INSERT Events Analysis:")
        for sample in samples_by_op['SNAPSHOT/INSERT'][:2]:  # Just first 2
            before = sample['before']
            after = sample['after']
            
            print(f"\n   Record #{sample['record_num']}:")
            print(f"      before: is_none={before['is_none']}, is_empty={before['is_empty_dict']}")
            print(f"      after:  is_none={after['is_none']}, is_empty={after['is_empty_dict']}")
            print(f"      F.to_json(before) → \"{before['json_dumps'][:50]}...\"")
            print(f"      F.to_json(after) → \"{after['json_dumps'][:50]}...\"")

def main():
    if len(sys.argv) < 2:
        print("Usage: python diagnose_json_struct.py <path_to_json_file>")
        print("\nExamples:")
        print("  # Local file:")
        print("  python diagnose_json_struct.py /path/to/file.ndjson")
        print("")
        print("  # Azure blob (requires AZURE_STORAGE_KEY env var):")
        print("  export AZURE_STORAGE_KEY='your_key'")
        print("  python diagnose_json_struct.py wasbs://cockroachdb@databrickslakeflow.blob.core.windows.net/json/defaultdb/public/test-json_usertable_no_split/1767823340/202601072206035945830580000000001-d07db17c86874707-1-37369-00000001-test_json_usertable_no_split-1.ndjson")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Check for Azure credentials if using wasbs://
    if file_path.startswith('wasbs://') and not os.environ.get('AZURE_STORAGE_KEY'):
        print("❌ Error: AZURE_STORAGE_KEY environment variable not set")
        print("\nFor Azure blob storage access, set:")
        print("  export AZURE_STORAGE_KEY='your_storage_account_key'")
        print("\nOr get it from .env file:")
        print("  source sources/cockroachdb/scripts/.env")
        sys.exit(1)
    
    try:
        analyze_json_file(file_path)
    except FileNotFoundError:
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

