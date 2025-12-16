#!/usr/bin/env python3
"""
Direct test of CockroachDB changefeed to diagnose blocking issues.
This bypasses the connector to test the raw changefeed behavior.

Run this if test_local.py hangs to diagnose the issue.
"""

import psycopg2
import json
import time
import signal
import sys

def timeout_handler(signum, frame):
    print("\n\n⏱️  TIMEOUT: Changefeed query is taking too long!")
    print("   This suggests the changefeed is blocking/waiting.")
    sys.exit(1)

print("="*60)
print("Direct CockroachDB Changefeed Test")
print("="*60)
print("\nThis test will:")
print("  1. Connect to local CockroachDB")
print("  2. Check table row count")
print("  3. Run a changefeed with initial_scan='yes'")
print("  4. Timeout after 10 seconds if it hangs")
print("\nPress Ctrl+C to stop at any time")
print("="*60)

try:
    # Connect to CockroachDB
    print("\n1. Connecting to CockroachDB...")
    conn = psycopg2.connect(
        host="localhost",
        port=26257,
        database="ycsb",
        user="root",
        password="",
        sslmode="disable"
    )
    conn.set_session(autocommit=True)
    print("✅ Connected")

    # Check table row count
    print("\n2. Checking table row count...")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM events")
        count = cur.fetchone()[0]
        print(f"✅ events table has {count} rows")
    
    if count == 0:
        print("\n⚠️  WARNING: events table is empty!")
        print("   Run: ./local_setup.sh start  to populate data")
        sys.exit(0)

    # Test changefeed with initial_scan
    print("\n3. Testing changefeed with initial_scan='only'...")
    print("   Query: EXPERIMENTAL CHANGEFEED FOR events")
    print("          WITH initial_scan='only'")
    print("\n   Note: initial_scan='only' returns existing rows as a snapshot (no streaming).")
    print("   Cannot be combined with 'updated' or 'resolved' options.")
    print("   Setting 10-second timeout...")

    changefeed_cursor = conn.cursor()

    print("\n   ⏱️  Executing changefeed query...")
    start_time = time.time()
    
    # Set alarm for 10 seconds
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(10)

    try:
        changefeed_cursor.execute("""
            EXPERIMENTAL CHANGEFEED FOR events 
            WITH initial_scan='only'
        """)
        
        exec_time = time.time() - start_time
        print(f"   ✅ Query executed in {exec_time:.2f}s")
        
        print("\n   📖 Fetching first result (this tests if data is flowing)...")
        row = changefeed_cursor.fetchone()
        
        fetch_time = time.time() - start_time
        signal.alarm(0)  # Cancel timeout
        
        if row is None:
            print(f"   ❌ No rows returned!")
            print(f"   Time elapsed: {fetch_time:.2f}s")
        else:
            print(f"   ✅ Got first row in {fetch_time:.2f}s")
            
            # Inspect the row structure
            print(f"\n   Row structure:")
            print(f"     Number of columns: {len(row)}")
            for i, col in enumerate(row):
                col_preview = str(col)[:50] if col else "None"
                print(f"     Column {i}: {col_preview}...")
            
            # Try to parse as changefeed format
            if len(row) >= 4:
                print(f"\n   Parsing as changefeed WITH updated: (key, value, updated, topic)...")
                key = row[0]
                value = row[1]
                updated = row[2]
                topic = row[3]
                
                if key is None and value is None:
                    print(f"     This is a RESOLVED TIMESTAMP: {updated}")
                else:
                    key_parsed = json.loads(key) if key else []
                    value_parsed = json.loads(value) if value else None
                    print(f"     key: {key_parsed}")
                    print(f"     value keys: {list(value_parsed.keys()) if value_parsed else 'None'}")
                    print(f"     updated: {updated}")
                    print(f"     topic: {topic}")
            elif len(row) == 2:
                print(f"\n   Parsing as changefeed WITHOUT updated: (key, value)...")
                key = row[0]
                value = row[1]
                
                key_parsed = json.loads(key) if key else []
                value_parsed = json.loads(value) if value else None
                print(f"     key: {key_parsed}")
                print(f"     value keys: {list(value_parsed.keys()) if value_parsed else 'None'}")
                print(f"     (No 'updated' or 'topic' columns in initial_scan='only' mode)")
            else:
                print(f"\n   ⚠️  Unexpected number of columns: {len(row)}")
            
            print(f"\n   ✅ Changefeed is working correctly!")
        
    except Exception as e:
        signal.alarm(0)
        exec_time = time.time() - start_time
        print(f"\n   ❌ Failed after {exec_time:.2f}s: {e}")
        import traceback
        traceback.print_exc()
    finally:
        changefeed_cursor.close()
        conn.close()

    print("\n" + "="*60)
    print("✅ Test complete!")
    print("="*60)
    print("\nConclusion:")
    print("  If this test succeeded, the changefeed is working.")
    print("  If it timed out, there may be an issue with:")
    print("    - Rangefeeds not enabled (run: ./check_setup.sh)")
    print("    - CockroachDB version compatibility")
    print("    - Network/connection issues")

except KeyboardInterrupt:
    print("\n\n🛑 Test interrupted by user")
    sys.exit(0)
except Exception as e:
    print(f"\n\n❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

