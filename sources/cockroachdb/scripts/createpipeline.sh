#!/bin/bash
# Create DLT pipeline for CockroachDB connector
# Usage: ./createpipeline.sh <connection_name> <table_list>
#
# Examples:
#   ./createpipeline.sh cockroachdb_connection "usertable"              # YCSB workload
#   ./createpipeline.sh cockroachdb_connection "customers,orders"       # Multiple tables
#
# Note: table_list is REQUIRED (comma-separated, no spaces)

SOURCE_NAME="cockroachdb"
CONNECTION_NAME="${1:-}"
TABLE_LIST="${2:-}"  # Required: specific tables to ingest

# Validate required arguments
if [ -z "$CONNECTION_NAME" ]; then
  echo "❌ Error: connection_name is required"
  echo ""
  echo "Usage: $0 <connection_name> <table_list>"
  echo ""
  echo "Examples:"
  echo "  $0 cockroachdb_connection \"usertable\"          # YCSB workload"
  echo "  $0 cockroachdb_connection \"customers,orders\"   # Multiple tables"
  echo ""
  exit 1
fi

if [ -z "$TABLE_LIST" ]; then
  echo "❌ Error: table_list is required"
  echo ""
  echo "Usage: $0 <connection_name> <table_list>"
  echo ""
  echo "Please specify tables as a comma-separated list:"
  echo "  $0 $CONNECTION_NAME \"usertable\"                # For YCSB workload"
  echo "  $0 $CONNECTION_NAME \"customers,orders\"         # For multiple tables"
  echo ""
  echo "To discover available tables, connect to your CockroachDB cluster and run:"
  echo "  SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
  echo ""
  echo "Why is this required?"
  echo "  Community Connectors cannot dynamically discover tables because connection"
  echo "  credentials are only available during Spark execution, after the pipeline"
  echo "  spec has already been defined."
  echo ""
  exit 1
fi

# Get current username and workspace URL
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')

# Set paths
WORKSPACE_PATH="/Workspace/Users/$USER_NAME"
PROJECT_NAME="${SOURCE_NAME}"
PROJECT_PATH="$WORKSPACE_PATH/$PROJECT_NAME"

# Create pipeline name
PIPELINE_NAME="$(echo $USER_NAME | cut -d'@' -f1 | tr '.' '_')_${SOURCE_NAME}"

echo "Creating DLT pipeline for $SOURCE_NAME connector..."
echo "User: $USER_NAME"
echo "Connection: $CONNECTION_NAME"
echo "Pipeline name: $PIPELINE_NAME"
echo "Tables: $TABLE_LIST"
echo ""

# Check if pipeline exists and delete it
echo "Checking for existing pipeline..."
PIPELINE_ID=$(databricks pipelines list-pipelines \
  --filter "name LIKE '$PIPELINE_NAME'" \
  --output json | jq -r '.[0].pipeline_id // empty')

if [ -n "$PIPELINE_ID" ]; then
  echo "⚠️  Found existing pipeline: $PIPELINE_ID"
  echo "Deleting existing pipeline..."
  databricks pipelines delete "$PIPELINE_ID"
  echo "✅ Deleted"
  sleep 5
fi

# Create schema if it doesn't exist
echo ""
echo "Checking schema..."
if ! databricks schemas get "main.${PIPELINE_NAME}" &>/dev/null; then
  echo "Creating schema: main.${PIPELINE_NAME}"
  databricks schemas create "${PIPELINE_NAME}" main
  echo "✅ Schema created"
else
  echo "✅ Schema already exists"
fi

# Build pipeline JSON using jq for proper JSON construction
echo ""
echo "Creating DLT pipeline..."

# Build configuration object
CONFIG_JSON=$(jq -n \
  --arg source_name "$SOURCE_NAME" \
  --arg connection_name "$CONNECTION_NAME" \
  --arg table_list "$TABLE_LIST" \
  '{
    source_name: $source_name,
    connection_name: $connection_name,
    table_list: $table_list
  }')
echo "Configuration: source_name=$SOURCE_NAME, connection_name=$CONNECTION_NAME, table_list=$TABLE_LIST"

# Build full pipeline JSON
PIPELINE_JSON=$(jq -n \
  --arg name "$PIPELINE_NAME" \
  --arg schema "$PIPELINE_NAME" \
  --arg ingest_path "$PROJECT_PATH/ingest.py" \
  --argjson config "$CONFIG_JSON" \
  '{
    name: $name,
    catalog: "main",
    schema: $schema,
    configuration: $config,
    serverless: true,
    continuous: false,
    development: true,
    libraries: [
      {
        file: {
          path: $ingest_path
        }
      }
    ]
  }')

# Create pipeline
RESPONSE=$(databricks pipelines create --json "$PIPELINE_JSON" 2>&1)

# Check if command succeeded
if echo "$RESPONSE" | grep -q "Error:"; then
  echo "❌ Failed to create pipeline:"
  echo "$RESPONSE"
  exit 1
fi

# Extract pipeline ID from response
PIPELINE_ID=$(echo "$RESPONSE" | jq -r '.pipeline_id // empty')

if [ -z "$PIPELINE_ID" ]; then
  echo "❌ Failed to extract pipeline ID from response:"
  echo "$RESPONSE"
  exit 1
fi

echo ""
echo "✅ Pipeline created!"
echo ""
echo "Pipeline ID: $PIPELINE_ID"
echo "View pipeline at: $WORKSPACE_URL/pipelines/$PIPELINE_ID"
echo ""
echo "To start the pipeline:"
echo "  databricks pipelines start-update $PIPELINE_ID"
echo ""
echo "To monitor progress:"
echo "  databricks pipelines get $PIPELINE_ID --output json | jq -r '.state'"

