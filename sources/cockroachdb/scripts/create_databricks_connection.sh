#!/bin/bash
set -e

# Usage: ./create_databricks_connection.sh <connection_url> [connection_name]
# Example: ./create_databricks_connection.sh "postgresql://user:pass@host:port/db?sslmode=require" "my_crdb_connection"

if [ -z "$1" ]; then
  echo "Error: Connection URL required"
  echo "Usage: $0 <connection_url> [connection_name]"
  echo "Example: $0 'postgresql://user:pass@host:port/db?sslmode=require' 'my_connection'"
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
echo "  Host: $HOST"
echo "  Port: $PORT"
echo "  Database: $DATABASE"
echo "  User: $USER"
echo "  SSL Mode: $SSLMODE"
echo ""

# Check if connection already exists
if databricks connections get "$CONNECTION_NAME" &>/dev/null; then
  echo "⚠️  Connection '$CONNECTION_NAME' already exists"
  echo "Updating existing connection..."
  
  databricks connections update "$CONNECTION_NAME" --json '{
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
  
  echo ""
  echo "✅ Connection '$CONNECTION_NAME' updated successfully!"
else
  echo "Creating new connection..."
  
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
  
  echo ""
  echo "✅ Connection '$CONNECTION_NAME' created successfully!"
fi

