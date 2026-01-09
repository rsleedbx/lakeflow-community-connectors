#!/usr/bin/env python3
"""
Direct test of pg8000 parameter passing.
Run this to determine the correct API usage.
"""

import ssl

# Install pg8000 if needed
try:
    import pg8000.native as pg8000_native
except ImportError:
    print("Installing pg8000...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pg8000>=1.30.0"])
    import pg8000.native as pg8000_native

# REPLACE THESE WITH YOUR ACTUAL CONNECTION DETAILS
# (or set them from environment if you prefer)
import os
HOST = os.getenv('COCKROACH_HOST', 'battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud')
PORT = int(os.getenv('COCKROACH_PORT', '26257'))
USER = os.getenv('COCKROACH_USER', 'rslee')
PASSWORD = os.getenv('COCKROACH_PASSWORD', '')  # SET THIS!
DATABASE = os.getenv('COCKROACH_DATABASE', 'ycsb')

if not PASSWORD:
    print("❌ Please set COCKROACH_PASSWORD environment variable or edit this script")
    print("   export COCKROACH_PASSWORD='your-password'")
    exit(1)

print("🔌 Connecting to CockroachDB...")
print(f"   Host: {HOST}")
print(f"   Port: {PORT}")
print(f"   Database: {DATABASE}")
print(f"   User: {USER}\n")

# Create SSL context (sslmode=require)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Connect
try:
    conn = pg8000_native.Connection(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DATABASE,
        ssl_context=ssl_context,
    )
    print("✅ Connected!\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

# Test different parameter passing methods
tests = []

# Test 1: No parameters
print("=" * 70)
print("TEST 1: Query without parameters")
print("=" * 70)
try:
    sql = "SELECT current_schema()"
    print(f"SQL: {sql}")
    print(f"Call: conn.run(sql)")
    result = conn.run(sql)
    print(f"✅ Result: {result}")
    tests.append(("No params", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("No params", False))

# Test 2: One parameter with $1, passed as individual arg
print("\n" + "=" * 70)
print("TEST 2: One parameter, passed as individual arg")
print("=" * 70)
try:
    sql = "SELECT $1::text"
    print(f"SQL: {sql}")
    print(f"Call: conn.run(sql, 'hello')")
    result = conn.run(sql, 'hello')
    print(f"✅ Result: {result}")
    tests.append(("$1, individual arg", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("$1, individual arg", False))

# Test 3: One parameter with $1, passed with *tuple
print("\n" + "=" * 70)
print("TEST 3: One parameter, passed with *tuple unpacking")
print("=" * 70)
try:
    sql = "SELECT $1::text"
    params = ('hello',)
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print(f"Call: conn.run(sql, *params)")
    result = conn.run(sql, *params)
    print(f"✅ Result: {result}")
    tests.append(("$1, *tuple", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("$1, *tuple", False))

# Test 4: One parameter with $1, passed as list
print("\n" + "=" * 70)
print("TEST 4: One parameter, passed as list")
print("=" * 70)
try:
    sql = "SELECT $1::text"
    params = ['hello']
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print(f"Call: conn.run(sql, params)")
    result = conn.run(sql, params)
    print(f"✅ Result: {result}")
    tests.append(("$1, list(params)", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("$1, list(params)", False))

# Test 5: Multiple parameters with $1, $2, passed as individual args
print("\n" + "=" * 70)
print("TEST 5: Multiple parameters, individual args")
print("=" * 70)
try:
    sql = "SELECT $1::text, $2::int"
    print(f"SQL: {sql}")
    print(f"Call: conn.run(sql, 'hello', 42)")
    result = conn.run(sql, 'hello', 42)
    print(f"✅ Result: {result}")
    tests.append(("$1,$2 individual args", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("$1,$2 individual args", False))

# Test 6: Multiple parameters with $1, $2, passed with *tuple
print("\n" + "=" * 70)
print("TEST 6: Multiple parameters, *tuple unpacking")
print("=" * 70)
try:
    sql = "SELECT $1::text, $2::int"
    params = ('hello', 42)
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print(f"Call: conn.run(sql, *params)")
    result = conn.run(sql, *params)
    print(f"✅ Result: {result}")
    tests.append(("$1,$2 *tuple", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("$1,$2 *tuple", False))

# Test 7: Real world query - list tables with $1 (1-based, PostgreSQL standard)
print("\n" + "=" * 70)
print("TEST 7: Real query with $1 (1-based, PostgreSQL standard)")
print("=" * 70)
try:
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 
          AND table_type = 'BASE TABLE'
        LIMIT 3
    """
    params = ('public',)
    print(f"SQL: {sql.strip()}")
    print(f"Params: {params}")
    print(f"Call: conn.run(sql, *params)")
    result = conn.run(sql, *params)
    print(f"✅ Result: {result}")
    tests.append(("Real query, $1, *tuple", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("Real query, $1, *tuple", False))

# Test 8: Real world query - list tables with $0 (0-based, Python style)
print("\n" + "=" * 70)
print("TEST 8: Real query with $0 (0-based, Python style)")
print("=" * 70)
try:
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $0 
          AND table_type = 'BASE TABLE'
        LIMIT 3
    """
    params = ('public',)
    print(f"SQL: {sql.strip()}")
    print(f"Params: {params}")
    print(f"Call: conn.run(sql, *params)")
    result = conn.run(sql, *params)
    print(f"✅ Result: {result}")
    tests.append(("Real query, $0, *tuple", True))
except Exception as e:
    print(f"❌ Error: {e}")
    tests.append(("Real query, $0, *tuple", False))

# Close connection
conn.close()

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
for test_name, passed in tests:
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{status}: {test_name}")

print("\n" + "=" * 70)
print("CONCLUSION")
print("=" * 70)
if len(tests) > 7 and tests[7][1]:  # Test 8: $0 with *tuple
    print("✅ Use: conn.run(sql, *params) where params is a tuple")
    print("   SQL placeholders: $0, $1, $2, ... (0-based, Python style)")
    print("   ⚠️  WARNING: This is NON-STANDARD! PostgreSQL uses 1-based $1, $2, $3")
elif len(tests) > 6 and tests[6][1]:  # Test 7: $1 with *tuple
    print("✅ Use: conn.run(sql, *params) where params is a tuple")
    print("   SQL placeholders: $1, $2, $3, ... (1-based, PostgreSQL standard)")
elif tests[2][1]:  # Test 3: *tuple unpacking
    print("✅ Use: conn.run(sql, *params) where params is a tuple")
    print("   SQL placeholders: $1, $2, $3, ... (1-based)")
elif tests[1][1]:  # Test 2: individual args
    print("✅ Use: conn.run(sql, arg1, arg2, arg3, ...)")
    print("   SQL placeholders: $1, $2, $3, ... (1-based)")
else:
    print("❌ None of the expected methods worked!")
    print("   Check the errors above for details")

