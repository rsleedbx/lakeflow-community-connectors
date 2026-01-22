#!/usr/bin/env bash
set -e

#######################################
# Complete Volume Mode Pipeline Setup
#
# Modeled after Direct Mode setup:
# 1. Deploy connector (copydir.sh)
# 2. Create pipeline (create_volume_pipeline.sh)
# 3. Monitor pipeline (monitor_pipeline.sh)
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

# Count Parquet files in volume (handle errors gracefully)
# Note: 'databricks fs ls' does NOT work with Unity Catalog Volumes - use SDK instead
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FILE_COUNT=$(python3 "$SCRIPTS_DIR/list_volume_files.py" --count --pattern "*.parquet" "${VOLUME_PATH}/" 2>/dev/null || echo "0")

# Ensure FILE_COUNT is a clean integer
FILE_COUNT=$(echo "$FILE_COUNT" | tr -d '\n' | xargs)

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "❌ No Parquet files found in Volume!"
    echo "   Volume Path: ${VOLUME_PATH}"
    echo ""
    echo "🔧 Please run sync script first:"
    echo "   ./sync_azure_to_volume.sh"
    exit 1
fi

echo "✅ Volume contains ${FILE_COUNT} Parquet files"
echo ""

#######################################
# Step 2: Deploy Connector
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 2: Deploy Connector to Databricks"
echo "═══════════════════════════════════════════════════════════════"

if [ -f "./copydir.sh" ]; then
    echo "📤 Running copydir.sh..."
    ./copydir.sh
    echo ""
else
    echo "⚠️  copydir.sh not found, assuming connector already deployed"
    echo ""
fi

#######################################
# Step 3: Create Pipeline
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 3: Create Volume Mode Pipeline"
echo "═══════════════════════════════════════════════════════════════"

if [ -f "./create_volume_pipeline.sh" ]; then
    ./create_volume_pipeline.sh
else
    echo "❌ create_volume_pipeline.sh not found!"
    exit 1
fi

# Get pipeline ID
if [ -f "../pipeline_id_volume.txt" ]; then
    PIPELINE_ID=$(cat ../pipeline_id_volume.txt)
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



