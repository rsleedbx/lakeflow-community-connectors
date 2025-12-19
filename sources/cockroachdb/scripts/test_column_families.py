#!/usr/bin/env python3
"""
Test script to discover how to query column family information from CockroachDB.
"""
import pg8000
import ssl

# Connection parameters
CONNECTION_URL = "postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

print("=" * 80)
print("🔍 EXPLORING COCKROACHDB COLUMN FAMILY INFORMATION")
print("=" * 80)
print()

# Parse connection URL
from urllib.parse import urlparse
parsed = urlparse(CONNECTION_URL)
host = parsed.hostname
port = parsed.port
database = parsed.path.lstrip('/')
user = parsed.username
password = parsed.password

# Connect
print("Connecting to CockroachDB...")
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

conn = pg8000.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password,
    ssl_context=ssl_context,
)
print("✅ Connected!")
print()

def run_query(description, query):
    """Run a query and print results."""
    print("=" * 80)
    print(f"📊 {description}")
    print("=" * 80)
    print(f"Query: {query}")
    print()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Get column names
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            print(f"Columns: {columns}")
            print()
        
        if rows:
            for i, row in enumerate(rows[:20]):  # Limit to 20 rows
                print(f"  Row {i+1}: {row}")
            if len(rows) > 20:
                print(f"  ... ({len(rows) - 20} more rows)")
        else:
            print("  (No rows returned)")
        
        cursor.close()
        print()
        return rows
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        return None

# Test 1: Check crdb_internal.table_columns
print("\n" + "=" * 80)
print("TEST 1: Explore crdb_internal.table_columns")
print("=" * 80)
print()

run_query(
    "What columns are available in crdb_internal.table_columns?",
    """
    SELECT column_name, data_type
    FROM information_schema.columns 
    WHERE table_schema = 'crdb_internal' 
      AND table_name = 'table_columns'
    ORDER BY ordinal_position
    """
)

run_query(
    "Sample data from crdb_internal.table_columns for usertable",
    """
    SELECT *
    FROM crdb_internal.table_columns
    WHERE descriptor_name = 'usertable'
    LIMIT 5
    """
)

# Test 2: SHOW CREATE TABLE
print("\n" + "=" * 80)
print("TEST 2: SHOW CREATE TABLE")
print("=" * 80)
print()

result = run_query(
    "Full table definition for usertable",
    "SHOW CREATE TABLE public.usertable"
)

if result:
    create_statement = result[0][1]
    print("Full CREATE TABLE statement:")
    print(create_statement)
    print()
    
    # Count FAMILY occurrences
    family_count = create_statement.upper().count('FAMILY ')
    print(f"🎯 Detected {family_count} column families by counting 'FAMILY ' keywords")
    print()

# Test 3: information_schema approach
print("\n" + "=" * 80)
print("TEST 3: Check standard information_schema")
print("=" * 80)
print()

run_query(
    "Columns in usertable from information_schema.columns",
    """
    SELECT column_name, ordinal_position, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'usertable'
    ORDER BY ordinal_position
    """
)

# Test 4: Check pg_catalog (PostgreSQL compatibility layer)
print("\n" + "=" * 80)
print("TEST 4: Check pg_catalog tables")
print("=" * 80)
print()

run_query(
    "Available pg_catalog tables",
    """
    SELECT tablename
    FROM pg_catalog.pg_tables
    WHERE schemaname = 'pg_catalog'
    ORDER BY tablename
    LIMIT 20
    """
)

# Test 5: Try to find table OID and query pg_attribute
print("\n" + "=" * 80)
print("TEST 5: Query pg_attribute (PostgreSQL-style)")
print("=" * 80)
print()

run_query(
    "Table OID for usertable",
    """
    SELECT oid, relname
    FROM pg_catalog.pg_class
    WHERE relname = 'usertable'
    """
)

# Test 6: Check if there's any family-related info in pg_attribute
run_query(
    "Check pg_attribute for usertable",
    """
    SELECT a.attname, a.attnum, a.atttypid
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
    WHERE c.relname = 'usertable'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
    """
)

print()
print("=" * 80)
print("✅ EXPLORATION COMPLETE")
print("=" * 80)
print()
print("Summary:")
print("  1. Check crdb_internal.table_columns columns")
print("  2. SHOW CREATE TABLE output shows FAMILY definitions")
print("  3. Count 'FAMILY ' keywords for family count")
print()

conn.close()

