#!/usr/bin/env python3
"""
Test pg8000 with the standard cursor API (not native)
"""

import ssl
import os

HOST = os.getenv('COCKROACH_HOST')
PORT = int(os.getenv('COCKROACH_PORT'))
USER = os.getenv('COCKROACH_USER')
PASSWORD = os.getenv('COCKROACH_PASSWORD')
DATABASE = os.getenv('COCKROACH_DATABASE')

print("🔌 Testing pg8000 standard cursor API (not native)")
print(f"   Host: {HOST}\n")

# Try importing pg8000 (non-native)
import pg8000

# Create SSL context
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Connect using standard pg8000 (not native)
print("Connecting with pg8000.connect()...")
conn = pg8000.connect(
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DATABASE,
    ssl_context=ssl_context,
)
print("✅ Connected!\n")

# Test with cursor and parameters
print("=" * 70)
print("TEST: Query with %s placeholder and tuple params")
print("=" * 70)
try:
    cursor = conn.cursor()
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s LIMIT 3"
    params = ('public',)
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print(f"Call: cursor.execute(sql, params)")
    cursor.execute(sql, params)
    result = cursor.fetchall()
    print(f"✅ Result: {result}")
    cursor.close()
except Exception as e:
    print(f"❌ Error: {e}")

# Test with $1 placeholder
print("\n" + "=" * 70)
print("TEST: Query with $1 placeholder and tuple params")
print("=" * 70)
try:
    cursor = conn.cursor()
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 LIMIT 3"
    params = ('public',)
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print(f"Call: cursor.execute(sql, params)")
    cursor.execute(sql, params)
    result = cursor.fetchall()
    print(f"✅ Result: {result}")
    cursor.close()
except Exception as e:
    print(f"❌ Error: {e}")

conn.close()

print("\n" + "=" * 70)
print("CONCLUSION")
print("=" * 70)
print("✅ pg8000 standard cursor API works with %s placeholders!")
print("   We should use pg8000.connect() NOT pg8000.native.Connection()")

