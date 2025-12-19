#!/usr/bin/env python3
"""
Find a way to query column family counts for ALL tables at once.
"""
import pg8000
import ssl

CONNECTION_URL = "postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

print("=" * 80)
print("🔍 FINDING BATCH QUERY FOR COLUMN FAMILIES")
print("=" * 80)
print()

# Parse and connect
from urllib.parse import urlparse
parsed = urlparse(CONNECTION_URL)

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

conn = pg8000.connect(
    host=parsed.hostname,
    port=parsed.port,
    database=parsed.path.lstrip('/'),
    user=parsed.username,
    password=parsed.password,
    ssl_context=ssl_context,
)
print("✅ Connected!")
print()

def run_query(description, query):
    """Run a query and print results."""
    print("=" * 80)
    print(f"📊 {description}")
    print("=" * 80)
    print(f"Query:\n{query}")
    print()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            print(f"Columns: {columns}")
            print()
        
        if rows:
            for i, row in enumerate(rows[:30]):
                print(f"  {row}")
            if len(rows) > 30:
                print(f"  ... ({len(rows) - 30} more rows)")
        else:
            print("  (No rows returned)")
        
        cursor.close()
        print()
        return rows
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        return None

# Test 1: Check crdb_internal schema
print("\n" + "=" * 80)
print("TEST 1: Explore crdb_internal tables")
print("=" * 80)
print()

run_query(
    "What tables are in crdb_internal?",
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'crdb_internal'
      AND table_name LIKE '%family%'
    ORDER BY table_name
    """
)

run_query(
    "All crdb_internal tables (first 30)",
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'crdb_internal'
    ORDER BY table_name
    LIMIT 30
    """
)

# Test 2: Check table_indexes (might have family info)
print("\n" + "=" * 80)
print("TEST 2: Check crdb_internal.table_indexes")
print("=" * 80)
print()

run_query(
    "Columns in table_indexes",
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'crdb_internal'
      AND table_name = 'table_indexes'
    ORDER BY ordinal_position
    """
)

# Test 3: Look for descriptor-related tables
print("\n" + "=" * 80)
print("TEST 3: Check descriptor tables")
print("=" * 80)
print()

run_query(
    "Tables with 'descriptor' in name",
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'crdb_internal'
      AND table_name LIKE '%descriptor%'
    ORDER BY table_name
    """
)

# Test 4: Try zones/partitions tables
run_query(
    "Check zones table",
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'crdb_internal'
      AND table_name = 'zones'
    ORDER BY ordinal_position
    """
)

# Test 5: MOST IMPORTANT - Can we aggregate crdb_internal.table_columns?
print("\n" + "=" * 80)
print("TEST 5: Try to extract family info from column_type")
print("=" * 80)
print()

run_query(
    "Sample column_type values",
    """
    SELECT 
        descriptor_name,
        column_name,
        column_type
    FROM crdb_internal.table_columns
    WHERE descriptor_name = 'usertable'
    LIMIT 5
    """
)

# Test 6: Check if there's a pattern in column_id that correlates with families
print("\n" + "=" * 80)
print("TEST 6: Analyze column_id patterns")
print("=" * 80)
print()

run_query(
    "Column IDs for usertable",
    """
    SELECT 
        column_id,
        column_name
    FROM crdb_internal.table_columns
    WHERE descriptor_name = 'usertable'
    ORDER BY column_id
    """
)

# Test 7: Check system.namespace or system.descriptor
print("\n" + "=" * 80)
print("TEST 7: Check system schema")
print("=" * 80)
print()

run_query(
    "Tables in system schema",
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'system'
    ORDER BY table_name
    """
)

# Test 8: CRITICAL - Try to query internal descriptors
print("\n" + "=" * 80)
print("TEST 8: Query internal descriptors")
print("=" * 80)
print()

run_query(
    "Check crdb_internal.table_descriptors",
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'crdb_internal'
      AND table_name = 'table_descriptors'
    ORDER BY ordinal_position
    """
)

run_query(
    "Sample from table_descriptors",
    """
    SELECT descriptor_id, descriptor_name
    FROM crdb_internal.table_descriptors
    WHERE descriptor_name = 'usertable'
    """
)

# Test 9: Ultimate test - count families per table in a single query
print("\n" + "=" * 80)
print("TEST 9: Attempt batch query for family counts")
print("=" * 80)
print()

# Try using column count as proxy (1 column = 1 family in YCSB pattern)
run_query(
    "Count columns per table (proxy for families in YCSB pattern)",
    """
    SELECT 
        descriptor_name AS table_name,
        COUNT(*) AS column_count
    FROM crdb_internal.table_columns
    WHERE descriptor_name IN ('usertable')
    GROUP BY descriptor_name
    """
)

print()
print("=" * 80)
print("✅ EXPLORATION COMPLETE")
print("=" * 80)
print()
print("KEY FINDINGS:")
print("  1. crdb_internal.table_columns has column info but NO family_id")
print("  2. column_type field contains 'family:StringFamily' but not family ID")
print("  3. For YCSB pattern: 1 column = 1 family")
print("  4. Need to check if there's a hidden family column or descriptor table")
print()

conn.close()

