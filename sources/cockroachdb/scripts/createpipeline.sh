#!/bin/bash
# Create DLT pipeline for CockroachDB connector
# Usage: ./createpipeline.sh [connection_name] [table_list]
#
# Examples:
#   ./createpipeline.sh                                    # Use default connection "cockroachdb_connection" and all tables
#   ./createpipeline.sh my_crdb_connection                 # Use custom connection name
#   ./createpipeline.sh cockroachdb_connection customers   # Use specific table

SOURCE_NAME="cockroachdb"
CONNECTION_NAME="${1:-cockroachdb_connection}"  # Default to cockroachdb_connection
TABLE_LIST="${2:-}"  # Optional: specific tables to ingest

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
if [ -n "$TABLE_LIST" ]; then
  echo "Tables: $TABLE_LIST"
else
  echo "Tables: All available tables"
fi
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

# Build configuration JSON
CONFIG_JSON='{'
CONFIG_JSON+=' "source_name": "'$SOURCE_NAME'",'
CONFIG_JSON+=' "connection_name": "'$CONNECTION_NAME'"'

if [ -n "$TABLE_LIST" ]; then
  CONFIG_JSON+=', "table_list": "'$TABLE_LIST'"'
fi

CONFIG_JSON+=' }'

# Create new pipeline
echo ""
echo "Creating DLT pipeline..."
databricks pipelines create \
  --json '{
    "name": "'$PIPELINE_NAME'",
    "catalog": "main",
    "schema": "'${PIPELINE_NAME}'",
    "configuration": '$CONFIG_JSON',
    "serverless": true,
    "continuous": false,
    "development": true,
    "libraries": [
      {
        "file": {
          "path": "'$PROJECT_PATH'/ingest.py"
        }
      }
    ]
  }' | tee /tmp/$PIPELINE_NAME.$$

# Extract pipeline ID from saved response
PIPELINE_ID=$(cat /tmp/$PIPELINE_NAME.$$ | jq -r '.pipeline_id')
rm -f /tmp/$PIPELINE_NAME.$$

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

