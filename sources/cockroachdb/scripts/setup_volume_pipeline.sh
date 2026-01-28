#!/usr/bin/env bash
set -e

#######################################
# Complete Volume Mode Pipeline Setup
#
# Workflow:
# 1. Verify volume has data files and _metadata/
# 2. Copy connector code to workspace (optional - copy_connector_to_workspace.sh or databricks bundle)
# 3. Create DLT pipeline (create_volume_pipeline.sh)
# 4. Start & monitor pipeline (monitor_pipeline.sh)
#
# Prerequisites:
# - Run sync_azure_to_volume_compact.py to populate volume
# - Volume structure: /Volumes/catalog/schema/volume/
#   ├── _metadata/schema.json
#   ├── timestamped_dir/snapshot_*.parquet
#   └── timestamped_dir/cdc_*.parquet
#######################################

echo "═══════════════════════════════════════════════════════════════"
echo "🚀 CockroachDB Volume Mode - Complete Setup"
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

# Set configuration from JSON
PIPELINE_NAME="${pipeline_config[pipeline_name]}"
CATALOG="${pipeline_config[catalog]}"
SCHEMA="${pipeline_config[schema]}"
VOLUME_NAME="${pipeline_config[volume_name]}"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME_NAME}"

echo "📋 Configuration:"
echo "  Config File: $PIPELINE_JSON"
echo "  Pipeline Name: ${PIPELINE_NAME}"
echo "  Catalog: ${CATALOG}"
echo "  Schema: ${SCHEMA}"
echo "  Volume Path: ${VOLUME_PATH}"
echo ""

# Change to scripts directory
cd "$(dirname "$0")"

#######################################
# Step 1: Verify Volume Has Files
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 1: Verify Volume Has Files"
echo "═══════════════════════════════════════════════════════════════"

# Check volume files using Databricks SDK (via list_volume_files.py)
# Note: 'databricks fs ls' CLI does NOT work with Unity Catalog Volumes
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🔍 Checking volume structure..."

# Test volume access
if ! python3 "$SCRIPTS_DIR/list_volume_files.py" "${VOLUME_PATH}/" > /dev/null 2>&1; then
    echo "❌ Cannot access volume!"
    echo "   Volume Path: ${VOLUME_PATH}"
    echo ""
    echo "🔧 Possible issues:"
    echo "   1. Volume doesn't exist - create it in Databricks first"
    echo "   2. No permissions to access volume"
    echo "   3. Databricks authentication not configured (run: databricks auth login)"
    exit 1
fi

# List all files in volume (non-recursive, top-level only)
ALL_FILES=$(python3 "$SCRIPTS_DIR/list_volume_files.py" "${VOLUME_PATH}/" 2>/dev/null || echo "")

# Check for _metadata directory
HAS_METADATA=$(echo "$ALL_FILES" | grep -c "_metadata" || echo "0")

# Check for timestamped directories (e.g., 2024-01-22_10-15-30/)
TIMESTAMP_DIRS=$(echo "$ALL_FILES" | grep -E "/[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}/$" || echo "")
TIMESTAMP_DIR_COUNT=$(echo "$TIMESTAMP_DIRS" | grep -c "/" || echo "0")

# If we have timestamped directories, check the first one for files
if [ "$TIMESTAMP_DIR_COUNT" -gt 0 ]; then
    FIRST_TIMESTAMP_DIR=$(echo "$TIMESTAMP_DIRS" | head -1 | tr -d '\n')
    SAMPLE_FILES=$(python3 "$SCRIPTS_DIR/list_volume_files.py" "${FIRST_TIMESTAMP_DIR}" 2>/dev/null | grep -E "\.(parquet|json|ndjson)$" || echo "")
    SAMPLE_FILE_COUNT=$(echo "$SAMPLE_FILES" | grep -c "/" || echo "0")
    
    if [ "$SAMPLE_FILE_COUNT" -eq 0 ]; then
        echo "❌ No data files found in volume directories!"
        echo "   Volume Path: ${VOLUME_PATH}"
        echo "   Found ${TIMESTAMP_DIR_COUNT} timestamped directory(ies) but they're empty"
        echo ""
        echo "🔧 Please run sync script to populate volume:"
        echo "   python3 sync_azure_to_volume_compact.py \\"
        echo "     --catalog ${CATALOG} \\"
        echo "     --schema ${SCHEMA} \\"
        echo "     --volume ${VOLUME_NAME} \\"
        echo "     --azure-config <azure_config.json>"
        exit 1
    fi
    
    echo "✅ Volume has ${TIMESTAMP_DIR_COUNT} timestamped directory(ies)"
    echo "   Sample: $(basename "$FIRST_TIMESTAMP_DIR" | tr -d '/')"
    echo "   Files in sample: ${SAMPLE_FILE_COUNT}"
else
    # No timestamped directories - check for files in root
    ROOT_FILES=$(echo "$ALL_FILES" | grep -E "\.(parquet|json|ndjson)$" || echo "")
    ROOT_FILE_COUNT=$(echo "$ROOT_FILES" | grep -c "/" || echo "0")
    
    if [ "$ROOT_FILE_COUNT" -eq 0 ]; then
        echo "❌ No data files found in Volume!"
        echo "   Volume Path: ${VOLUME_PATH}"
        echo ""
        echo "🔧 Please run sync script to populate volume:"
        echo "   python3 sync_azure_to_volume_compact.py \\"
        echo "     --catalog ${CATALOG} \\"
        echo "     --schema ${SCHEMA} \\"
        echo "     --volume ${VOLUME_NAME} \\"
        echo "     --azure-config <azure_config.json>"
        exit 1
    fi
    
    echo "✅ Volume has ${ROOT_FILE_COUNT} file(s) in root"
fi

# Check for _metadata/schema.json
if [ "$HAS_METADATA" -gt 0 ]; then
    SCHEMA_EXISTS=$(python3 "$SCRIPTS_DIR/list_volume_files.py" "${VOLUME_PATH}/_metadata/" 2>/dev/null | grep -c "schema.json" || echo "0")
    if [ "$SCHEMA_EXISTS" -gt 0 ]; then
        echo "✅ Volume has _metadata/schema.json"
    else
        echo "⚠️  _metadata/schema.json not found (optional - will be inferred if missing)"
    fi
else
    echo "⚠️  _metadata/ directory not found (optional - schema will be inferred)"
fi
echo ""

#######################################
# Step 2: Copy Connector Code (Optional)
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 2: Copy Connector Code to Workspace"
echo "═══════════════════════════════════════════════════════════════"

# Check copy/deployment method
if [ -f "$GIT_ROOT/databricks.yml" ]; then
    echo "ℹ️  Databricks Asset Bundle (databricks.yml) detected"
    echo ""
    read -p "   Deploy via bundle? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "📤 Deploying via databricks bundle..."
        cd "$GIT_ROOT"
        databricks bundle deploy
        cd "$SCRIPTS_DIR"
        echo "✅ Bundle deployed (code copied + pipeline created)"
        echo ""
    else
        echo "⚠️  Skipping - assuming connector code already in workspace"
        echo ""
    fi
elif [ -f "./copy_connector_to_workspace.sh" ]; then
    echo "📤 Running copy_connector_to_workspace.sh..."
    ./copy_connector_to_workspace.sh
    echo ""
else
    echo "⚠️  No copy method found, assuming connector code already in workspace"
    echo "   (Looking for: databricks.yml or copy_connector_to_workspace.sh)"
    echo ""
fi

#######################################
# Step 3: Create Pipeline
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 3: Create Volume Mode Pipeline"
echo "═══════════════════════════════════════════════════════════════"

if [ -f "./deploy_pipeline.sh" ]; then
    ./deploy_pipeline.sh --config "$PIPELINE_JSON"
else
    echo "❌ deploy_pipeline.sh not found!"
    exit 1
fi

# Get pipeline ID
PIPELINE_ID_FILE="../pipeline_id_volume.txt"
if [ -f "$PIPELINE_ID_FILE" ]; then
    PIPELINE_ID=$(cat "$PIPELINE_ID_FILE")
    echo ""
    echo "📊 Pipeline ID: ${PIPELINE_ID}"
fi

#######################################
# Step 4: Start Pipeline
#######################################
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Step 4: Start Pipeline"
echo "═══════════════════════════════════════════════════════════════"
echo ""

if [ -n "$PIPELINE_ID" ]; then
    read -p "Start pipeline now? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🚀 Starting pipeline with full refresh..."
        databricks pipelines start-update "${PIPELINE_ID}" --full-refresh
        echo ""
        echo "✅ Pipeline started!"
        echo ""
        
        # Monitor
        echo "📈 Monitoring pipeline (Ctrl+C to stop monitoring)..."
        echo ""
        ./monitor_pipeline.sh "${PIPELINE_NAME}" "${CATALOG}" "${SCHEMA}"
    else
        echo ""
        echo "ℹ️  Pipeline created but not started"
        echo ""
        echo "▶️  To start manually:"
        echo "   databricks pipelines start-update ${PIPELINE_ID} --full-refresh"
        echo ""
        echo "📈 To monitor:"
        echo "   ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA}"
    fi
else
    echo "⚠️  Could not get pipeline ID, start manually from UI"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "✅ Setup Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📊 Pipeline: ${PIPELINE_NAME}"
echo "📍 Tables will be in: ${CATALOG}.${SCHEMA}"
echo ""
echo "🎯 Next Steps:"
echo "   1. Monitor pipeline: ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA}"
echo "   2. Query results: SELECT * FROM ${CATALOG}.${SCHEMA}.usertable"
echo ""



