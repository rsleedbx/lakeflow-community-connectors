#!/bin/bash
set -e

# Usage: ./create_databricks_connection.sh <connection_url> [connection_name]
# Example: ./create_databricks_connection.sh "postgresql://user:pass@host:port/db?sslmode=require" "my_crdb_connection"
#
# To export CONNECTION_NAME for use in subsequent commands:
#   source ./create_databricks_connection.sh "$URL" && ./createpipeline.sh "$CONNECTION_NAME"
#
# Or use command substitution:
#   CONNECTION_NAME=$(./create_databricks_connection.sh "$URL" | grep "CONNECTION_NAME=" | cut -d'=' -f2)

if [ -z "$1" ]; then
  echo "Error: Connection URL required"
  echo "Usage: $0 <connection_url> [connection_name]"
  echo ""
  echo "Examples:"
  echo "  $0 'postgresql://user:pass@host:port/db?sslmode=require' 'my_connection'"
  echo ""
  echo "To use the connection name in next command:"
  echo "  source $0 \"\$URL\" && ./createpipeline.sh \"\$CONNECTION_NAME\""
  exit 1
fi

CONNECTION_URL="$1"

# Generate default connection name if not provided
if [ -z "$2" ]; then
  # Get Databricks username and remove @databricks.com
  DATABRICKS_USER=$(databricks current-user me --output json 2>/dev/null | grep -o '"userName":"[^"]*"' | cut -d'"' -f4 | sed 's/@databricks.com$//' | tr '.' '_' || echo "user")
  
  # Extract hostname from connection URL
  HOST_FULL=$(echo "$CONNECTION_URL" | sed -n 's|postgresql://[^@]*@\([^:]*\).*|\1|p')
  HOST_SHORT=$(echo "$HOST_FULL" | cut -d'.' -f1)
  
  CONNECTION_NAME="${DATABRICKS_USER}_${HOST_SHORT}"
else
  CONNECTION_NAME="$2"
fi

# Parse the connection URL
# Format: postgresql://user:password@host:port/database?params

# Extract user and password
USER_PASS=$(echo "$CONNECTION_URL" | sed -n 's|postgresql://\([^@]*\)@.*|\1|p')
USER=$(echo "$USER_PASS" | cut -d':' -f1)
PASSWORD=$(echo "$USER_PASS" | cut -d':' -f2-)

# Extract host and port
HOST_PORT=$(echo "$CONNECTION_URL" | sed -n 's|postgresql://[^@]*@\([^/]*\).*|\1|p')
HOST=$(echo "$HOST_PORT" | cut -d':' -f1)
PORT=$(echo "$HOST_PORT" | cut -d':' -f2)

# Extract database
DATABASE=$(echo "$CONNECTION_URL" | sed -n 's|postgresql://[^@]*@[^/]*/\([^?]*\).*|\1|p')

# Extract sslmode (default to require if not specified)
SSLMODE=$(echo "$CONNECTION_URL" | sed -n 's|.*sslmode=\([^&]*\).*|\1|p')
SSLMODE="${SSLMODE:-require}"

echo "Configuring Databricks connection with:"
echo "  Name: $CONNECTION_NAME"
echo "  Connection URL: $CONNECTION_URL"
echo ""
echo "Testing THREE modes to see which parameters Unity Catalog passes:"
echo "  Mode 1: GitHub-style (token + base_url)"
echo "  Mode 2: Single connection_url parameter"
echo "  Mode 3: Individual parameters (host, port, database, user, password)"
echo ""

# Check if connection already exists
if databricks connections get "$CONNECTION_NAME" &>/dev/null; then
  echo "⚠️  Connection '$CONNECTION_NAME' already exists"
  echo "Deleting and recreating to test all modes..."
  databricks connections delete "$CONNECTION_NAME"
  sleep 2
fi

# Extract base URL (without credentials) for Mode 1
BASE_URL="postgresql://${HOST}:${PORT}/${DATABASE}?sslmode=${SSLMODE}"

# Test Mode 1: GitHub-style parameters (token + base_url)
# This is THE KEY TEST: If this works but Mode 3 doesn't, we know Unity Catalog
# has parameter name restrictions (only passes 'token'/'base_url', not 'host'/'port'/etc.)
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧪 MODE 1: Using 'token' + 'base_url' (GitHub-style)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "This is the DIAGNOSTIC TEST:"
echo "  - 'token' will contain the FULL connection URL with credentials"
echo "  - 'base_url' will contain the URL without credentials (for reference)"
echo ""
echo "If Mode 1 works but Mode 3 fails, it proves Unity Catalog only passes"
echo "specific parameter names like 'token'/'base_url' (not 'host'/'port'/etc.)."
echo ""

databricks connections create --json '{
  "name": "'"$CONNECTION_NAME"'",
  "connection_type": "GENERIC_LAKEFLOW_CONNECT",
  "options": {
    "sourceName": "cockroachdb",
    "token": "'"$CONNECTION_URL"'",
    "base_url": "'"$BASE_URL"'",
    "externalOptionsAllowList": "cursor,include_diff,select_query,resolved_interval,batch_size,initial_scan,split_column_families"
  }
}'

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ SUCCESS: Mode 1 works! Unity Catalog passes 'token' and 'base_url'."
  echo "   🎯 KEY FINDING: Unity Catalog uses GitHub-style parameter names!"
  echo "   This means we should use 'token' (not 'connection_url' or 'host'/'port'/etc.)."
  echo ""
  echo "📝 Connection name exported as: CONNECTION_NAME=$CONNECTION_NAME"
  echo "   Use it in the next command: "
  echo "   ./createpipeline.sh"
  echo ""
  export CONNECTION_NAME
  exit 0
fi

echo ""
echo "⚠️  Mode 1 failed. Trying Mode 2..."
echo ""

# Test Mode 2: Single connection_url parameter
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧪 MODE 2: Using 'connection_url' (single parameter)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
databricks connections create --json '{
  "name": "'"$CONNECTION_NAME"'",
  "connection_type": "GENERIC_LAKEFLOW_CONNECT",
  "options": {
    "sourceName": "cockroachdb",
    "connection_url": "'"$CONNECTION_URL"'",
    "externalOptionsAllowList": "cursor,include_diff,select_query,resolved_interval,batch_size,initial_scan,split_column_families"
  }
}'

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ SUCCESS: Mode 2 works! Unity Catalog accepts 'connection_url' parameter."
  echo ""
  echo "📝 Connection name exported as: CONNECTION_NAME=$CONNECTION_NAME"
  echo "   Use it in the next command: "
  echo "   ./createpipeline.sh"
  echo ""
  export CONNECTION_NAME
  exit 0
fi

echo ""
echo "⚠️  Mode 2 failed. Trying Mode 3..."
echo ""

# Test Mode 3: Individual parameters
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧪 MODE 3: Using individual parameters (host, port, database, user, password)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
databricks connections create --json '{
  "name": "'"$CONNECTION_NAME"'",
  "connection_type": "GENERIC_LAKEFLOW_CONNECT",
  "options": {
    "sourceName": "cockroachdb",
    "host": "'"$HOST"'",
    "port": "'"$PORT"'",
    "database": "'"$DATABASE"'",
    "user": "'"$USER"'",
    "password": "'"$PASSWORD"'",
    "sslmode": "'"$SSLMODE"'",
    "schema": "public",
    "externalOptionsAllowList": "cursor,include_diff,select_query,resolved_interval,batch_size,initial_scan,split_column_families"
  }
}'

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ SUCCESS: Mode 3 works! Unity Catalog accepts individual parameters."
  echo ""
  echo "📝 Connection name exported as: CONNECTION_NAME=$CONNECTION_NAME"
  echo "   Use it in the next command: "
  echo "   ./createpipeline.sh"
  echo ""
  export CONNECTION_NAME
  exit 0
fi

echo ""
echo "❌ FAILED: All three modes failed!"
echo "Please check the error messages above for details."
exit 1

echo ""
echo "📝 Connection name exported as: CONNECTION_NAME=$CONNECTION_NAME"
echo "   Use it in the next command:"
echo "   ./createpipeline.sh \$CONNECTION_NAME"

# Export for use in parent shell (if sourced) or subshells
export CONNECTION_NAME

