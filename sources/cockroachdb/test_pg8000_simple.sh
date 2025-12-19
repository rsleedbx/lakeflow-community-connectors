#!/bin/bash
# Simple test script for pg8000 using existing $COCKROACHDB_URL

set -e

if [ -z "$COCKROACHDB_URL" ]; then
    echo "❌ COCKROACHDB_URL environment variable not set"
    echo ""
    echo "Please set it first, for example:"
    echo "export COCKROACHDB_URL='postgresql://user:pass@host:port/database?sslmode=require'"
    exit 1
fi

echo "🔍 Using COCKROACHDB_URL from environment"
echo ""

# Parse the URL into components
# Format: postgresql://user:pass@host:port/database?sslmode=require
URL="$COCKROACHDB_URL"

# Extract components using parameter expansion and sed
USER_PASS=$(echo "$URL" | sed -n 's|postgresql://\([^@]*\)@.*|\1|p')
export COCKROACH_USER=$(echo "$USER_PASS" | cut -d':' -f1)
export COCKROACH_PASSWORD=$(echo "$USER_PASS" | cut -d':' -f2-)

HOST_PORT=$(echo "$URL" | sed -n 's|postgresql://[^@]*@\([^/]*\).*|\1|p')
export COCKROACH_HOST=$(echo "$HOST_PORT" | cut -d':' -f1)
export COCKROACH_PORT=$(echo "$HOST_PORT" | cut -d':' -f2)

export COCKROACH_DATABASE=$(echo "$URL" | sed -n 's|postgresql://[^/]*/\([^?]*\).*|\1|p')

echo "Parsed connection details:"
echo "  Host: $COCKROACH_HOST"
echo "  Port: $COCKROACH_PORT"
echo "  Database: $COCKROACH_DATABASE"
echo "  User: $COCKROACH_USER"
echo ""

# Run the Python test script
python3 test_pg8000_local.py

