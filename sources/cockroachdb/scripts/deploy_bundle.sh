#!/usr/bin/env bash
# Deploy CockroachDB connector using Databricks Asset Bundles

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(dirname "$SCRIPT_DIR")"

echo "🚀 Deploying CockroachDB Connector via Databricks Bundle"
echo "════════════════════════════════════════════════════════"
echo ""

# Navigate to source directory
cd "$SOURCE_DIR"

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ databricks CLI not found"
    echo "   Install: https://docs.databricks.com/dev-tools/cli/databricks-cli.html"
    exit 1
fi

echo "✅ Databricks CLI found: $(databricks --version)"
echo ""

# Validate bundle
echo "📋 Step 1: Validating bundle..."
if databricks bundle validate; then
    echo "   ✅ Bundle configuration is valid"
else
    echo "   ❌ Bundle validation failed"
    exit 1
fi
echo ""

# Deploy bundle
echo "🚀 Step 2: Deploying bundle..."
TARGET="${1:-development}"
echo "   Target: $TARGET"
echo ""

if databricks bundle deploy --target "$TARGET"; then
    echo "   ✅ Bundle deployed successfully"
else
    echo "   ❌ Bundle deployment failed"
    exit 1
fi
echo ""

# Get pipeline info
echo "📊 Step 3: Getting pipeline info..."
PIPELINE_NAME=$(databricks bundle validate --target "$TARGET" 2>/dev/null | grep -A 5 "pipelines:" | grep "name:" | awk '{print $2}' || echo "")

if [ -z "$PIPELINE_NAME" ]; then
    # Fallback: construct expected name
    USER_SHORT=$(databricks current-user me --output json 2>/dev/null | jq -r '.userName' | cut -d'@' -f1 | tr '.' '_')
    PIPELINE_NAME="${USER_SHORT}_cockroachdb"
fi

echo "   Pipeline name: $PIPELINE_NAME"

# Get pipeline ID
PIPELINE_ID=$(databricks pipelines list-pipelines --filter "name LIKE '$PIPELINE_NAME'" --output json 2>/dev/null | jq -r ".[] | select(.name == \"$PIPELINE_NAME\") | .pipeline_id")

if [ ! -z "$PIPELINE_ID" ] && [ "$PIPELINE_ID" != "null" ]; then
    echo "   Pipeline ID: $PIPELINE_ID"
else
    echo "   ⚠️  Pipeline ID not found yet (might take a moment)"
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "✅ Deployment Complete!"
echo "════════════════════════════════════════════════════════"
echo ""
echo "📋 What was deployed:"
echo "   - Pipeline: $PIPELINE_NAME"
echo "   - Source code: ingest.py + cockroachdb.py"
echo "   - Dependencies: pg8000 (managed by Databricks)"
echo "   - Target: $TARGET"
echo ""
echo "🎯 Next steps:"
echo ""
echo "  1. Run the pipeline:"
if [ ! -z "$PIPELINE_ID" ]; then
    echo "     databricks pipelines start-update $PIPELINE_ID --full-refresh"
    echo ""
    echo "  2. Monitor execution:"
    echo "     $SCRIPT_DIR/monitor_pipeline.sh $PIPELINE_NAME"
else
    echo "     databricks bundle run cockroachdb_pipeline --target $TARGET"
    echo ""
    echo "  2. Or find pipeline ID and monitor:"
    echo "     databricks pipelines list-pipelines | grep $PIPELINE_NAME"
fi
echo ""
echo "  3. View in UI:"
echo "     ${DATABRICKS_HOST}#joblist/pipelines/"
echo ""
echo "💡 To update after code changes:"
echo "   databricks bundle deploy --target $TARGET"
echo ""




