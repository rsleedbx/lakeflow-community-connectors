#!/usr/bin/env bash

# Create CockroachDB Pipeline with Azure Parquet Mode
# This script creates a NEW pipeline with Azure credentials enabled

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"

echo "════════════════════════════════════════════════════════════════"
echo "🎯 Creating CockroachDB Pipeline - Azure Parquet Mode"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Load Azure credentials
AZURE_ENV="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.env"
if [ ! -f "$AZURE_ENV" ]; then
    echo "❌ Azure credentials not found: $AZURE_ENV"
    echo "   Run: sources/cockroachdb/scripts/setup_azure_blob_for_cdc.sh"
    exit 1
fi

source "$AZURE_ENV"

# Load CockroachDB credentials (optional for pipeline creation)
CRDB_ENV="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cockroachcloud.env"
if [ -f "$CRDB_ENV" ]; then
    source "$CRDB_ENV"
fi

echo "✅ Azure credentials loaded"
echo "   Account: $AZURE_STORAGE_ACCOUNT"
echo "   Container: $AZURE_STORAGE_CONTAINER"
echo ""

# Configuration
PIPELINE_NAME="robert_lee_cockroachdb_azure"
CONNECTION_NAME="robert_lee_battle-walrus-11108"
SOURCE_NAME="cockroachdb"
TABLE_LIST="usertable"
CATALOG="main"
SCHEMA_NAME="robert_lee_cockroachdb_azure"

echo "Pipeline Configuration:"
echo "  Name: $PIPELINE_NAME"
echo "  Connection: $CONNECTION_NAME"
echo "  Tables: $TABLE_LIST"
echo "  Mode: Azure Parquet ⭐"
echo ""

# Delete old pipeline if exists
echo "🗑️  Checking for existing pipeline..."
EXISTING_ID=$(databricks pipelines list-pipelines --output json 2>/dev/null | jq -r ".[] | select(.name == \"$PIPELINE_NAME\") | .pipeline_id")

if [ -n "$EXISTING_ID" ] && [ "$EXISTING_ID" != "null" ]; then
    echo "   Found existing pipeline: $EXISTING_ID"
    echo "   Deleting..."
    databricks pipelines delete --pipeline-id "$EXISTING_ID" 2>/dev/null || true
    sleep 2
fi

# Create pipeline with Azure Parquet configuration
echo "📝 Creating new pipeline..."
echo ""

PIPELINE_JSON=$(cat <<EOF
{
  "name": "$PIPELINE_NAME",
  "catalog": "$CATALOG",
  "target": "$SCHEMA_NAME",
  "serverless": true,
  "development": true,
  "continuous": false,
  "channel": "CURRENT",
  "configuration": {
    "connection_name": "$CONNECTION_NAME",
    "source_name": "$SOURCE_NAME",
    "table_list": "$TABLE_LIST",
    "azure_account_name": "$AZURE_STORAGE_ACCOUNT",
    "azure_account_key": "$AZURE_STORAGE_KEY",
    "azure_container": "$AZURE_STORAGE_CONTAINER",
    "azure_path_prefix": "cockroachdb-cdc"
  },
  "libraries": [
    {
      "file": {
        "path": "/Workspace/Users/robert.lee@databricks.com/cockroachdb/ingest.py"
      }
    }
  ]
}
EOF
)

# Create the pipeline
RESULT=$(echo "$PIPELINE_JSON" | databricks pipelines create --json-input 2>&1)

if echo "$RESULT" | grep -q "pipeline_id"; then
    NEW_PIPELINE_ID=$(echo "$RESULT" | jq -r '.pipeline_id')
    echo "✅ Pipeline created successfully!"
    echo "   Pipeline ID: $NEW_PIPELINE_ID"
    echo "   Name: $PIPELINE_NAME"
    echo ""
    
    echo "📋 Configuration:"
    databricks pipelines get "$NEW_PIPELINE_ID" --output json 2>/dev/null | jq '.spec.configuration'
    echo ""
    
    echo "════════════════════════════════════════════════════════════════"
    echo "✅ Azure Parquet Pipeline Ready!"
    echo "════════════════════════════════════════════════════════════════"
    echo ""
    echo "To run the pipeline:"
    echo "  cd sources/cockroachdb/scripts"
    echo "  ./monitor_pipeline.sh $PIPELINE_NAME $CATALOG $SCHEMA_NAME $NEW_PIPELINE_ID"
    echo ""
    echo "Expected behavior:"
    echo "  🎯 OPERATION MODE: Azure Parquet (Production)"
    echo "  📦 Installing Azure dependencies..."
    echo "  🔍 Creating changefeed to Azure..."
    echo "  📂 Reading Parquet files from Azure..."
    echo ""
else
    echo "❌ Failed to create pipeline"
    echo "$RESULT"
    exit 1
fi


