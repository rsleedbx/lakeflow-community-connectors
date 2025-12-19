#!/usr/bin/env python3
"""
Simple test: Can asyncpg handle CockroachDB changefeeds at all?
Test with a LIMIT to make it finish quickly.
"""
import asyncio
import asyncpg
from urllib.parse import urlparse

COCKROACHDB_URL = "postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

def parse_connection_url(url):
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 26257,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/').split('?')[0],
        'ssl': 'require'
    }

async def test_changefeed_basic():
    """Test if asyncpg can execute a changefeed query at all."""
    conn_params = parse_connection_url(COCKROACHDB_URL)
    
    print("=" * 80)
    print("🧪 Simple Changefeed Test with asyncpg")
    print("=" * 80)
    
    conn = await asyncpg.connect(**conn_params)
    
    try:
        # Get cursor
        cursor_ts = await conn.fetchval("SELECT now()::timestamptz")
        print(f"\nCursor: {cursor_ts}")
        
        # Simple changefeed with resolved timestamps
        # This will run indefinitely unless we add some control
        changefeed_query = f"""
            EXPERIMENTAL CHANGEFEED FOR usertable
            WITH 
                initial_scan = 'yes',
                updated,
                resolved = '5s',
                split_column_families,
                cursor = '{cursor_ts}'
        """
        
        print("\n⏱️  Running changefeed for up to 15 seconds...")
        print("    Watching for resolved timestamps...")
        print()
        
        start = asyncio.get_event_loop().time()
        event_count = 0
        resolved_count = 0
        
        # Run query with overall timeout
        try:
            # Execute the query
            async with asyncio.timeout(15):  # Python 3.11+ timeout
                # Use fetch which should return rows as they become available
                async with conn.transaction():
                    # We can't use fetch() because it waits for all results
                    # We need to use a cursor, but CockroachDB changefeeds don't support cursors well
                    
                    # Alternative: use execute() and manually fetch
                    await conn.execute(f"SET statement_timeout = '15s'")
                    
                    # This is a streaming query - asyncpg might not handle it well
                    rows = await conn.fetch(changefeed_query)
                    
                    for row in rows:
                        event_count += 1
                        if len(row) >= 3:
                            key = row[1]
                            val = row[2]
                            if key is None and val is None:
                                resolved_count += 1
                                elapsed = asyncio.get_event_loop().time() - start
                                print(f"   ✅ Resolved timestamp #{resolved_count} (after {elapsed:.1f}s)")
                                if resolved_count == 1:
                                    print("   ⏹️  Got first resolved timestamp, stopping")
                                    break
                        
                        if event_count <= 3:
                            print(f"   📄 Event #{event_count}")
                        
                        # Safety limit
                        if event_count >= 100:
                            print("   ⚠️  Reached 100 events, stopping")
                            break
        
        except asyncio.TimeoutError:
            print(f"\n⏰ Timeout after 15 seconds")
            print(f"   Events processed: {event_count}")
            print(f"   Resolved timestamps: {resolved_count}")
        
        duration = asyncio.get_event_loop().time() - start
        
        print("\n" + "=" * 80)
        print("📊 Test Results")
        print("=" * 80)
        print(f"Duration: {duration:.1f}s")
        print(f"Events: {event_count}")
        print(f"Resolved: {resolved_count}")
        
        if resolved_count > 0:
            print("\n✅ SUCCESS: Resolved timestamps are working with asyncpg!")
        else:
            print("\n❌ No resolved timestamps received")
            print("   This might be a CockroachDB/asyncpg compatibility issue")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(test_changefeed_basic())
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")

