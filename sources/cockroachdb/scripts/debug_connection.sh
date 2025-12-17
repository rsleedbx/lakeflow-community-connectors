#!/bin/bash
# Debug script to diagnose connection issues
# Run this in your terminal: ./debug_connection.sh

set +e  # Don't exit on error

CONNECTION_NAME="robert_lee_battle-walrus-11108"

echo "════════════════════════════════════════════════════════════════"
echo "  CockroachDB Connection Diagnostic Tool"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Checking connection: $CONNECTION_NAME"
echo ""

# Step 1: Check if connection exists
echo "━━━ Step 1: Check if connection exists ━━━"
if databricks api get "/api/2.0/unity-catalog/connections/$CONNECTION_NAME" --output json > /tmp/conn_check.json 2>&1; then
  echo "✅ Connection exists"
  echo ""
  
  # Step 2: Check connection type
  echo "━━━ Step 2: Verify connection type ━━━"
  CONN_TYPE=$(jq -r '.connection_type' /tmp/conn_check.json)
  echo "Connection type: $CONN_TYPE"
  
  if [ "$CONN_TYPE" = "GENERIC_LAKEFLOW_CONNECT" ]; then
    echo "✅ Correct type: GENERIC_LAKEFLOW_CONNECT"
  else
    echo "❌ WRONG TYPE! Expected: GENERIC_LAKEFLOW_CONNECT, Got: $CONN_TYPE"
    echo ""
    echo "Fix: Delete and recreate the connection:"
    echo "  databricks connections delete $CONNECTION_NAME"
    echo "  ./create_databricks_connection.sh \"\$COCKROACHDB_URL\" \"$CONNECTION_NAME\""
  fi
  echo ""
  
  # Step 3: Check required options
  echo "━━━ Step 3: Verify required options ━━━"
  jq -r '.options' /tmp/conn_check.json > /tmp/conn_options.json
  
  SOURCE_NAME=$(jq -r '.sourceName // "MISSING"' /tmp/conn_options.json)
  HOST=$(jq -r '.host // "MISSING"' /tmp/conn_options.json)
  PORT=$(jq -r '.port // "MISSING"' /tmp/conn_options.json)
  DATABASE=$(jq -r '.database // "MISSING"' /tmp/conn_options.json)
  USER=$(jq -r '.user // "MISSING"' /tmp/conn_options.json)
  SSLMODE=$(jq -r '.sslmode // "MISSING"' /tmp/conn_options.json)
  
  echo "Required options:"
  echo "  sourceName: $SOURCE_NAME $([ "$SOURCE_NAME" = "cockroachdb" ] && echo "✅" || echo "❌ MUST BE 'cockroachdb'")"
  echo "  host:       $HOST $([ "$HOST" != "MISSING" ] && echo "✅" || echo "❌ MISSING")"
  echo "  port:       $PORT $([ "$PORT" != "MISSING" ] && echo "✅" || echo "❌ MISSING")"
  echo "  database:   $DATABASE $([ "$DATABASE" != "MISSING" ] && echo "✅" || echo "❌ MISSING")"
  echo "  user:       $USER $([ "$USER" != "MISSING" ] && echo "✅" || echo "❌ MISSING")"
  echo "  sslmode:    $SSLMODE $([ "$SSLMODE" != "MISSING" ] && echo "✅" || echo "❌ MISSING")"
  echo ""
  
  # Step 4: Check if all required fields are present
  if [ "$SOURCE_NAME" = "cockroachdb" ] && \
     [ "$HOST" != "MISSING" ] && \
     [ "$DATABASE" != "MISSING" ] && \
     [ "$USER" != "MISSING" ]; then
    echo "✅ ALL REQUIRED OPTIONS PRESENT"
    echo ""
    echo "━━━ Connection looks good! ━━━"
    echo ""
    echo "Full connection details:"
    jq '.' /tmp/conn_check.json
  else
    echo "❌ MISSING REQUIRED OPTIONS"
    echo ""
    echo "Fix: Update the connection with all required fields:"
    echo "  ./create_databricks_connection.sh \"\$COCKROACHDB_URL\" \"$CONNECTION_NAME\""
  fi
  
else
  echo "❌ Connection '$CONNECTION_NAME' NOT FOUND"
  cat /tmp/conn_check.json 2>/dev/null
  echo ""
  echo "━━━ Available connections ━━━"
  databricks api get "/api/2.0/unity-catalog/connections" --output json | jq -r '.connections[] | .name' 2>/dev/null || echo "Could not list connections"
  echo ""
  echo "Fix: Create the connection:"
  echo "  cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/scripts"
  echo "  ./create_databricks_connection.sh \"\$COCKROACHDB_URL\" \"$CONNECTION_NAME\""
fi

echo ""
echo "════════════════════════════════════════════════════════════════"

# Cleanup
rm -f /tmp/conn_check.json /tmp/conn_options.json

