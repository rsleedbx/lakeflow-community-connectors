#!/usr/bin/env python3
"""
Local test script for pg8000 connection to CockroachDB.
Tests the parameter passing and query execution.
"""

import ssl
import os

# Check for required environment variables
required_vars = ['COCKROACH_HOST', 'COCKROACH_PORT', 'COCKROACH_USER', 'COCKROACH_PASSWORD', 'COCKROACH_DATABASE']
missing_vars = [var for var in required_vars if var not in os.environ]

if missing_vars:
    print("❌ Missing required environment variables:")
    for var in missing_vars:
        print(f"   - {var}")
    print("\nExample usage:")
    print("export COCKROACH_HOST='battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud'")
    print("export COCKROACH_PORT='26257'")
    print("export COCKROACH_USER='rslee'")
    print("export COCKROACH_PASSWORD='your-password'")
    print("export COCKROACH_DATABASE='ycsb'")
    print("\npython test_pg8000_local.py")
    exit(1)

# Try to import pg8000
try:
    import pg8000.native as pg8000_native
    print("✅ pg8000 is installed")
except ImportError:
    print("❌ pg8000 not installed. Installing...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pg8000>=1.30.0"])
    import pg8000.native as pg8000_native
    print("✅ pg8000 installed successfully")

# Get connection parameters from environment
host = os.environ['COCKROACH_HOST']
port = int(os.environ['COCKROACH_PORT'])
user = os.environ['COCKROACH_USER']
password = os.environ['COCKROACH_PASSWORD']
database = os.environ['COCKROACH_DATABASE']

print(f"\n🔌 Connecting to CockroachDB:")
print(f"   Host: {host}")
print(f"   Port: {port}")
print(f"   Database: {database}")
print(f"   User: {user}")
print(f"   Password: {'*' * len(password)}")

# Create SSL context (sslmode=require)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Connect
try:
    conn = pg8000_native.Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        ssl_context=ssl_context,
    )
    print("✅ Connected successfully!\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

# Test 1: Query without parameters
print("=" * 60)
print("TEST 1: Query without parameters")
print("=" * 60)
try:
    query = "SELECT current_database(), current_schema()"
    print(f"Query: {query}")
    rows = conn.run(query)
    print(f"Result: {rows}")
    print("✅ Test 1 passed\n")
except Exception as e:
    print(f"❌ Test 1 failed: {e}\n")

# Test 2: Query with one parameter (psycopg2 style %s)
print("=" * 60)
print("TEST 2: Query with %s placeholder (WRONG for pg8000)")
print("=" * 60)
try:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s LIMIT 3"
    print(f"Query: {query}")
    rows = conn.run(query, 'public')
    print(f"Result: {rows}")
    print("✅ Test 2 passed\n")
except Exception as e:
    print(f"❌ Test 2 failed (expected): {e}\n")

# Test 3: Query with $1 placeholder (pg8000 style)
print("=" * 60)
print("TEST 3: Query with $1 placeholder (CORRECT for pg8000)")
print("=" * 60)
try:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 LIMIT 3"
    print(f"Query: {query}")
    rows = conn.run(query, 'public')
    print(f"Result: {rows}")
    print("✅ Test 3 passed\n")
except Exception as e:
    print(f"❌ Test 3 failed: {e}\n")

# Test 4: Query with multiple parameters
print("=" * 60)
print("TEST 4: Query with multiple $1, $2 placeholders")
print("=" * 60)
try:
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 
          AND table_type = $2
        LIMIT 3
    """
    print(f"Query: {query}")
    rows = conn.run(query, 'public', 'BASE TABLE')
    print(f"Result: {rows}")
    print("✅ Test 4 passed\n")
except Exception as e:
    print(f"❌ Test 4 failed: {e}\n")

# Test 5: Query with tuple unpacking (like our code does)
print("=" * 60)
print("TEST 5: Query with tuple unpacking (our current approach)")
print("=" * 60)
try:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 LIMIT 3"
    params = ('public',)  # This is what we pass in our code
    print(f"Query: {query}")
    print(f"Params: {params}")
    rows = conn.run(query, *params)  # Unpack the tuple
    print(f"Result: {rows}")
    print("✅ Test 5 passed\n")
except Exception as e:
    print(f"❌ Test 5 failed: {e}\n")

# Test 6: Try 0-based indexing (Python style)
print("=" * 60)
print("TEST 6: Query with $0 placeholder (0-based, Python style)")
print("=" * 60)
try:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = $0 LIMIT 3"
    print(f"Query: {query}")
    rows = conn.run(query, 'public')
    print(f"Result: {rows}")
    print("✅ Test 6 passed\n")
except Exception as e:
    print(f"❌ Test 6 failed: {e}\n")

# Test 7: Try using : style parameters (another pg8000 option)
print("=" * 60)
print("TEST 7: Query with :param placeholder (named style)")
print("=" * 60)
try:
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema LIMIT 3"
    print(f"Query: {query}")
    rows = conn.run(query, schema='public')
    print(f"Result: {rows}")
    print("✅ Test 7 passed\n")
except Exception as e:
    print(f"❌ Test 7 failed: {e}\n")

# Close connection
conn.close()
print("=" * 60)
print("✅ All tests complete!")
print("=" * 60)

