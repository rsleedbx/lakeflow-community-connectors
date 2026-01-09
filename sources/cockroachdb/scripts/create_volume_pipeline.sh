#!/usr/bin/env bash
set -e

#######################################
# Create CockroachDB Volume Mode Pipeline
#
# Similar to createpipeline.sh but for Volume mode
#######################################

echo "═══════════════════════════════════════════════════════════════"
echo "🚀 Creating CockroachDB Volume Mode Pipeline"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Find git root
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

COCKROACH_DIR="$GIT_ROOT/sources/cockroachdb"
PIPELINE_JSON="$COCKROACH_DIR/.env/cockroachdb_pipelines.json"

# Verify pipeline config exists
if [ ! -f "$PIPELINE_JSON" ]; then
    echo "❌ Missing $PIPELINE_JSON"
    echo "   This file should define catalog, schema, volume_name, blob_prefix"
    exit 1
fi

# Check if yq is available
if ! command -v yq &> /dev/null; then
    echo "❌ Error: yq is required to parse JSON configuration"
    echo "   Install with: brew install yq (macOS) or snap install yq (Linux)"
    exit 1
fi

# Load pipeline configuration into associative array
declare -A pipeline_config
while IFS='=' read -r key value; do
    value="${value%\'}"
    value="${value#\'}"
    pipeline_config["$key"]="$value"
done < <(yq -o=shell "$PIPELINE_JSON")

# Configuration
export TEST_MODE=volume
export PIPELINE_NAME="${pipeline_config[pipeline_name]}"
export CATALOG="${pipeline_config[catalog]}"
export SCHEMA="${pipeline_config[schema]}${TEST_MODE:+_${TEST_MODE}}"
export VOLUME_NAME="${pipeline_config[volume_name]}"
export VOLUME_PATH="/Volumes/${CATALOG}/${pipeline_config[schema]}/${VOLUME_NAME}"

# Get current user
USER_NAME=$(databricks current-user me --output json 2>/dev/null | jq -r '.userName' || echo "robert.lee@databricks.com")
NOTEBOOK_PATH="/Workspace/Repos/${USER_NAME}/lakeflow-community-connectors/sources/cockroachdb/ingest"

echo "📋 Configuration:"
echo "  Config File: $PIPELINE_JSON"
echo "  Pipeline Name: ${PIPELINE_NAME}"
echo "  Catalog: ${CATALOG}"
echo "  Schema: ${SCHEMA}"
echo "  Volume Path: ${VOLUME_PATH}"
echo "  Notebook: ${NOTEBOOK_PATH}"
echo ""

#######################################
# Check if pipeline exists
#######################################
echo "🔍 Checking for existing pipeline..."

EXISTING_PIPELINE=$(databricks pipelines list-pipelines --output json 2>/dev/null | \
  jq -r ".statuses[] | select(.name == \"${PIPELINE_NAME}\") | .pipeline_id" || echo "")

if [ -n "$EXISTING_PIPELINE" ]; then
  echo "⚠️  Pipeline '${PIPELINE_NAME}' already exists: ${EXISTING_PIPELINE}"
  echo ""
  read -p "   Delete and recreate? (y/N) " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Deleting old pipeline..."
    databricks pipelines delete "$EXISTING_PIPELINE" 2>/dev/null || true
    sleep 3
    echo "✅ Old pipeline deleted"
  else
    echo "ℹ️  Keeping existing pipeline: ${EXISTING_PIPELINE}"
    echo ""
    echo "▶️  Start pipeline:"
    echo "   databricks pipelines start-update ${EXISTING_PIPELINE} --full-refresh"
    echo ""
    echo "📊 Monitor pipeline:"
    echo "   ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA}"
    exit 0
  fi
fi
echo ""

#######################################
# Create new pipeline
#######################################
echo "🚀 Creating new pipeline..."

# Pipeline JSON configuration
PIPELINE_JSON=$(cat <<EOF
{
  "name": "${PIPELINE_NAME}",
  "catalog": "${CATALOG}",
  "target": "${SCHEMA}",
  "channel": "PREVIEW",
  "serverless": true,
  "development": true,
  "configuration": {
    "volume_path": "${VOLUME_PATH}",
    "source_name": "cockroachdb",
    "table_list": "usertable"
  },
  "libraries": [
    {
      "notebook": {
        "path": "${NOTEBOOK_PATH}"
      }
    }
  ]
}
EOF
)

echo "$PIPELINE_JSON" | jq '.'
echo ""

# Save to temp file
TEMP_FILE=$(mktemp)
echo "$PIPELINE_JSON" > "$TEMP_FILE"

# Create pipeline
RESULT=$(databricks pipelines create --json @"$TEMP_FILE" --output json 2>&1)

if [ $? -eq 0 ]; then
  export PIPELINE_ID=$(echo "$RESULT" | jq -r '.pipeline_id')
  WORKSPACE_URL=$(databricks auth env --output json 2>/dev/null | jq -r '.env.DATABRICKS_HOST' || echo "https://adb-984752964297111.11.azuredatabricks.net")
  
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  echo "✅ Pipeline Created Successfully!"
  echo "════════════════════════════════════════════════════════════════"
  echo ""
  echo "Pipeline ID: ${PIPELINE_ID}"
  echo "Pipeline Name: ${PIPELINE_NAME}"
  echo ""
  echo "🔗 View in UI:"
  echo "   ${WORKSPACE_URL}/pipelines/${PIPELINE_ID}"
  echo ""
  echo "▶️  Start pipeline (full refresh):"
  echo "   databricks pipelines start-update ${PIPELINE_ID} --full-refresh"
  echo ""
  echo "📈 Monitor pipeline:"
  echo "   ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA}"
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  
  # Save pipeline ID
  echo "$PIPELINE_ID" > ../pipeline_id_volume.txt
  echo "💾 Pipeline ID saved to: pipeline_id_volume.txt"
  
else
  echo ""
  echo "❌ Failed to create pipeline"
  echo ""
  echo "Error details:"
  echo "$RESULT" | jq '.' 2>/dev/null || echo "$RESULT"
  rm -f "$TEMP_FILE"
  exit 1
fi

# Cleanup
rm -f "$TEMP_FILE"

echo ""
echo "✅ Pipeline creation complete!"
echo ""
echo "🎯 Next steps:"
echo "   1. Start the pipeline with the command above"
echo "   2. Monitor: ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA}"
echo "   3. Check tables in catalog: ${CATALOG}.${SCHEMA}"
echo ""



