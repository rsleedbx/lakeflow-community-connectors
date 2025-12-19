#!/usr/bin/env python3
"""
Test if CockroachDB sends resolved timestamps in incremental mode.
This will help diagnose if the issue is:
  - CockroachDB not sending resolved timestamps
  - pg8000 not receiving them
  - Our code not handling them correctly
"""
import pg8000
import time
import signal
import sys

# Connection details
COCKROACHDB_URL = "postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

def signal_handler(sig, frame):
    print('\n\n🛑 Interrupted by user (Ctrl+C)')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def parse_connection_string(url):
    """Parse PostgreSQL connection string."""
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 26257,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/').split('?')[0],
        'ssl_context': True  # Enable SSL
    }

print("=" * 80)
print("🧪 Testing CockroachDB Resolved Timestamps")
print("=" * 80)
print("\nThis test will:")
print("  1. Create a changefeed with initial_scan='yes' and resolved_interval='5s'")
print("  2. Use a cursor (simulates incremental mode)")
print("  3. Monitor for resolved timestamps (should arrive every 5 seconds)")
print("  4. Run for 30 seconds or until first resolved timestamp")
print("\n💡 Press Ctrl+C to stop early\n")

# Connect
conn_params = parse_connection_string(COCKROACHDB_URL)
print(f"📡 Connecting to: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
conn = pg8000.connect(**conn_params)
cursor = conn.cursor()

try:
    # Get a recent timestamp to use as cursor
    print("\n⏱️  Step 1: Getting recent timestamp for cursor...")
    cursor.execute("SELECT now()")
    cursor_timestamp = cursor.fetchone()[0]
    print(f"  Using cursor: {cursor_timestamp}")
    
    # Build changefeed query (incremental mode)
    print("\n⏱️  Step 2: Creating changefeed query...")
    changefeed_query = f"""
        EXPERIMENTAL CHANGEFEED FOR usertable
        WITH 
            initial_scan = 'yes',
            updated,
            resolved = '5s',
            cursor = '{cursor_timestamp}'
    """
    print(f"  Query: {changefeed_query.strip()}")
    
    # Set shorter timeout for testing
    print("\n⏱️  Step 3: Setting statement timeout...")
    cursor.execute("SET statement_timeout = '45s'")
    print("  ✅ Timeout set to 45s")
    
    # Execute changefeed
    print("\n⏱️  Step 4: Executing changefeed...")
    print("=" * 80)
    cursor.execute(changefeed_query)
    
    print("\n📊 Monitoring for events and resolved timestamps...")
    print("   (Waiting up to 30 seconds...)\n")
    
    start_time = time.time()
    event_count = 0
    resolved_count = 0
    max_wait = 30  # seconds
    
    # Monitor for events/resolved timestamps
    while (time.time() - start_time) < max_wait:
        try:
            row = cursor.fetchone()
            
            if row is None:
                print("   ℹ️  No more rows (changefeed ended)")
                break
            
            # Changefeed format: (table, key, value, updated, topic)
            table_name = row[0]
            key_json = row[1]
            value_json = row[2]
            updated = row[3]
            
            # Check if this is a resolved timestamp
            if key_json is None and value_json is None:
                resolved_count += 1
                elapsed = time.time() - start_time
                print(f"   ✅ RESOLVED TIMESTAMP #{resolved_count}: {updated} (after {elapsed:.1f}s)")
                
                # Success! Resolved timestamps are working
                if resolved_count == 1:
                    print("\n" + "=" * 80)
                    print("🎉 SUCCESS: Resolved timestamps are being sent by CockroachDB!")
                    print("=" * 80)
                    print(f"\n✅ First resolved timestamp arrived after {elapsed:.1f}s")
                    print(f"   Expected interval: 5s")
                    print(f"   Actual interval: {elapsed:.1f}s")
                    if elapsed > 10:
                        print(f"   ⚠️  Slower than expected (should be ~5s)")
                    break
            else:
                # Regular data event
                event_count += 1
                if event_count <= 3:
                    print(f"   📄 Event #{event_count}: table={table_name}, updated={updated}")
        
        except Exception as e:
            print(f"\n❌ Error during fetch: {e}")
            break
    
    elapsed_total = time.time() - start_time
    
    print("\n" + "=" * 80)
    print("📊 Test Summary")
    print("=" * 80)
    print(f"  Duration: {elapsed_total:.1f}s")
    print(f"  Data events: {event_count}")
    print(f"  Resolved timestamps: {resolved_count}")
    
    if resolved_count > 0:
        print("\n✅ DIAGNOSIS: Resolved timestamps ARE being sent by CockroachDB")
        print("   → The hang issue is likely in pg8000's socket handling")
        print("   → Switching to asyncpg would likely fix this")
    else:
        print("\n❌ DIAGNOSIS: No resolved timestamps received")
        print("   Possible causes:")
        print("   1. CockroachDB not sending them (check version/configuration)")
        print("   2. pg8000 not reading them (socket buffer issue)")
        print("   3. Cursor timestamp is too recent (no changes to resolve)")
    
    if event_count == 0 and resolved_count == 0:
        print("\n💡 TIP: The table has no changes since the cursor timestamp")
        print("   This is expected for incremental mode with no updates")

except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
finally:
    cursor.close()
    conn.close()
    print("\n🔌 Connection closed")

print("\n" + "=" * 80)

