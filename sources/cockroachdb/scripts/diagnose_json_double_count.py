#!/usr/bin/env python3
"""
Diagnostic script to investigate why json_usertable_no_split shows upd=800 instead of upd=400.

This script samples JSON changefeed events to check for duplicate events per primary key.
"""

import json
import sys
from collections import defaultdict
from azure.storage.blob import BlobServiceClient

def diagnose_json_changefeeds(account_name, account_key, container_name, path_prefix, sample_size=50):
    """
    Analyze JSON changefeed files to detect duplicate events.
    
    Args:
        account_name: Azure storage account name
        account_key: Azure storage account key
        container_name: Container name
        path_prefix: Path prefix (e.g., 'json/defaultdb/public/test-json_usertable_no_split')
        sample_size: Number of events to sample per category
    """
    # Validate credentials
    if not account_name or not account_key:
        print("❌ ERROR: Azure credentials not provided")
        print()
        print("Please ensure you've loaded credentials first:")
        print()
        print("  # Load from JSON config")
        print("  source <(jq -r 'to_entries|map(\"azure_\\(.key)=\\(.value|tostring)\")|.[]' \\")
        print("      ../../../.env/cockroachdb_cdc_azure.json)")
        print()
        print("  # Verify they're loaded")
        print("  echo \"Account: $azure_azure_storage_account\"")
        print()
        sys.exit(1)
    
    if not account_name.strip() or account_name == 'None':
        print("❌ ERROR: Invalid Azure storage account name")
        print(f"   Received: '{account_name}'")
        print()
        print("This usually means credentials weren't loaded properly.")
        sys.exit(1)
    
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
    except Exception as e:
        print(f"❌ ERROR: Failed to connect to Azure storage")
        print(f"   Account: {account_name}")
        print(f"   Container: {container_name}")
        print(f"   Error: {e}")
        sys.exit(1)
    
    print("=" * 80)
    print("JSON CHANGEFEED DUPLICATE EVENT DIAGNOSTIC")
    print("=" * 80)
    print()
    print(f"Path prefix: {path_prefix}")
    print()
    
    # List all JSON files
    print("📂 Listing JSON files in Azure...", flush=True)
    data_blobs = []
    blob_count = 0
    try:
        for blob in container_client.list_blobs(name_starts_with=path_prefix):
            blob_count += 1
            if blob_count % 10 == 0:
                print(f"   Listed {blob_count} blobs...", flush=True)
            if blob.name.endswith('.ndjson') or blob.name.endswith('.json'):
                if '_schema.json' not in blob.name:
                    data_blobs.append(blob.name)
    except Exception as e:
        print(f"\n❌ ERROR: Failed to list blobs: {e}")
        sys.exit(1)
    
    print(f"✅ Found {len(data_blobs)} JSON changefeed files (out of {blob_count} total blobs)")
    print()
    
    if len(data_blobs) == 0:
        print("❌ No JSON changefeed files found!")
        print()
        print("Possible reasons:")
        print("  • Wrong path prefix (check test scenario name)")
        print("  • Files not synced to Azure yet")
        print("  • Changefeed not created for this scenario")
        sys.exit(1)
    
    # Collect events by primary key
    update_events_by_key = defaultdict(list)
    total_updates = 0
    
    print(f"📥 Downloading and analyzing files (sampling {sample_size} unique keys)...")
    print()
    
    for i, blob_name in enumerate(data_blobs, 1):
        print(f"   [{i}/{len(data_blobs)}] {blob_name.split('/')[-1]}...", flush=True)
        
        try:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            download_stream = blob_client.download_blob()
            content = download_stream.readall().decode('utf-8')
            
            lines_in_file = 0
            updates_in_file = 0
            
            for line in content.strip().split('\n'):
                if not line:
                    continue
                lines_in_file += 1
                try:
                    event = json.loads(line)
                    
                    # Look for UPDATE events (before + after)
                    after = event.get('after')
                    before = event.get('before')
                    
                    if after and before:
                        total_updates += 1
                        updates_in_file += 1
                        # Extract primary key (ycsb_key)
                        pk_value = after.get('ycsb_key')
                        if pk_value:
                            update_events_by_key[pk_value].append({
                                'file': blob_name.split('/')[-1],
                                'after': after,
                                'before': before,
                                'updated': event.get('updated', 'N/A')
                            })
                            
                            # Stop if we have enough samples
                            if len(update_events_by_key) >= sample_size:
                                print(f"       ✅ Reached {sample_size} unique keys, stopping early")
                                break
                    
                except json.JSONDecodeError:
                    continue
            
            print(f"       {lines_in_file} events, {updates_in_file} updates")
                
        except Exception as e:
            print(f"       ⚠️  Error: {e}")
            continue
        
        if len(update_events_by_key) >= sample_size:
            break
    
    print()
    
    print(f"Total UPDATE events found: {total_updates}")
    print(f"Unique keys with UPDATEs: {len(update_events_by_key)}")
    print()
    
    # Find keys with multiple UPDATE events
    duplicates = {k: v for k, v in update_events_by_key.items() if len(v) > 1}
    
    print("=" * 80)
    print("DUPLICATE DETECTION RESULTS")
    print("=" * 80)
    print()
    
    if duplicates:
        print(f"❌ Found {len(duplicates)} keys with multiple UPDATE events!")
        print()
        print("Sample duplicates:")
        print()
        
        for i, (key, events) in enumerate(list(duplicates.items())[:5]):
            print(f"Key: {key} ({len(events)} UPDATE events)")
            for j, evt in enumerate(events, 1):
                print(f"  Event {j}:")
                print(f"    File: {evt['file']}")
                print(f"    Updated: {evt['updated']}")
                print(f"    Columns in 'after': {list(evt['after'].keys())}")
                print(f"    Columns in 'before': {list(evt['before'].keys())}")
            print()
        
        # Check if events differ
        first_dup_key = list(duplicates.keys())[0]
        first_dup_events = duplicates[first_dup_key]
        
        if len(first_dup_events) >= 2:
            evt1 = first_dup_events[0]
            evt2 = first_dup_events[1]
            
            print("Detailed comparison of first 2 events for same key:")
            print()
            print("Event 1 'after' columns:", sorted(evt1['after'].keys()))
            print("Event 2 'after' columns:", sorted(evt2['after'].keys()))
            print()
            
            # Check if column sets differ (column family indicator)
            cols1 = set(evt1['after'].keys())
            cols2 = set(evt2['after'].keys())
            
            only_in_1 = cols1 - cols2
            only_in_2 = cols2 - cols1
            
            if only_in_1 or only_in_2:
                print("❗ COLUMN FAMILY FRAGMENTS DETECTED:")
                if only_in_1:
                    print(f"  Columns only in event 1: {sorted(only_in_1)}")
                if only_in_2:
                    print(f"  Columns only in event 2: {sorted(only_in_2)}")
                print()
                print("This suggests CockroachDB is emitting separate events per column family")
                print("even with split_column_families=false.")
            else:
                print("❗ Events have IDENTICAL column sets - possible duplicate emission")
        
        # Calculate ratio
        ratio = total_updates / len(update_events_by_key)
        print()
        print(f"Average events per key: {ratio:.2f}")
        if abs(ratio - 2.0) < 0.1:
            print("⚠️  Ratio is ~2.0, confirming double-counting hypothesis!")
        
    else:
        print("✅ No duplicate UPDATE events detected")
        print("   Each key has exactly 1 UPDATE event")
    
    print()
    print("=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    print()
    
    if duplicates:
        avg_ratio = total_updates / max(len(update_events_by_key), 1)
        if avg_ratio > 1.5:
            print("The analysis shows that JSON changefeeds with column families emit")
            print("multiple events per logical UPDATE even without split_column_families.")
            print()
            print("FIX NEEDED: Enhance deduplication logic to handle this case by:")
            print("1. Detecting column family fragments in JSON format")
            print("2. Merging fragments with identical PK but different column sets")
            print("3. Counting merged events only once")
    else:
        print("No issue detected. Update counts should be accurate.")


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python diagnose_json_double_count.py <account_name> <account_key> <container> <path_prefix> <sample_size>")
        print()
        print("Step-by-step example:")
        print()
        print("1. Navigate to scripts directory:")
        print("   cd sources/cockroachdb/scripts")
        print()
        print("2. Load Azure credentials from JSON config:")
        print("   source <(jq -r 'to_entries|map(\"azure_\\(.key)=\\(.value|tostring)\")|.[]' \\")
        print("       ../../../.env/cockroachdb_cdc_azure.json)")
        print()
        print("3. Verify credentials are loaded:")
        print("   echo \"Account: $azure_azure_storage_account\"")
        print()
        print("4. Run diagnostic:")
        print("   python3 diagnose_json_double_count.py \\")
        print("       \"$azure_azure_storage_account\" \\")
        print("       \"$azure_azure_storage_key\" \\")
        print("       \"changefeed-events\" \\")
        print("       \"json/defaultdb/public/test-json_usertable_no_split\" \\")
        print("       50")
        sys.exit(1)
    
    account_name = sys.argv[1]
    account_key = sys.argv[2]
    container = sys.argv[3]
    path_prefix = sys.argv[4]
    sample_size = int(sys.argv[5])
    
    diagnose_json_changefeeds(account_name, account_key, container, path_prefix, sample_size)

