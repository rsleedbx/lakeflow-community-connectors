#!/bin/bash
set -e

# Usage: ./create_databricks_connection.sh <connection_url> [connection_name]
# Example: ./create_databricks_connection.sh "postgresql://user:pass@host:port/db?sslmode=require" "my_crdb_connection"

if [ -z "$1" ]; then
  echo "Error: Connection URL required"
  echo "Usage: $0 <connection_url> [connection_name]"
  echo ""
  echo "Examples:"
  echo "  $0 'postgresql://user:pass@host:port/db?sslmode=require' 'my_connection'"
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

# Extract credentials (GitHub-style: token = username:password)
TOKEN="${USER}:${PASSWORD}"

# Base URL without credentials (will be combined with token in connector)
BASE_URL="postgresql://${HOST}:${PORT}/${DATABASE}?sslmode=${SSLMODE}"

echo "Creating Unity Catalog connection:"
echo "  Name: $CONNECTION_NAME"
echo "  Host: $HOST:$PORT"
echo "  Database: $DATABASE"
echo "  User: $USER"
echo "  SSL Mode: $SSLMODE"
echo ""
echo "GitHub-style parameters:"
echo "  token: ${USER}:*** (username:password)"
echo "  base_url: $BASE_URL"
echo ""

# Check if connection already exists
if databricks connections get "$CONNECTION_NAME" &>/dev/null; then
  echo "⚠️  Connection '$CONNECTION_NAME' already exists"
  echo "Deleting and recreating..."
  databricks connections delete "$CONNECTION_NAME"
  sleep 2
fi

# Create connection with GitHub-style parameters + fallback individual params
# token = username:password (like GitHub PAT)
# base_url = server URL without credentials
# Connector will reconstruct: postgresql://token@base_url
echo "Creating connection..."
databricks connections create --json '{
  "name": "'"$CONNECTION_NAME"'",
  "connection_type": "GENERIC_LAKEFLOW_CONNECT",
  "options": {
    "sourceName": "cockroachdb",
    "token": "'"$TOKEN"'",
    "base_url": "'"$BASE_URL"'",
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
  echo "✅ Connection created successfully!"
  echo ""
  echo "Connection includes BOTH formats:"
  echo "  • GitHub-style: token + base_url"
  echo "  • Individual params: host, port, database, user, password, sslmode"
  echo ""
  echo "The connector will use whichever format Unity Catalog passes at runtime."
  echo ""
  echo "📝 Connection name exported as: CONNECTION_NAME=$CONNECTION_NAME"
  echo "   Use it in the next command:"
  echo "   ./createpipeline.sh"
  echo ""
  export CONNECTION_NAME
  exit 0
else
  echo ""
  echo "❌ Failed to create connection!"
  echo "Please check the error messages above for details."
  exit 1
fi
