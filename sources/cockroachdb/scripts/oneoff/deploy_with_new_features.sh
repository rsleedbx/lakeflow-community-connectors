#!/usr/bin/env bash
set -e

echo "🚀 DEPLOYING COCKROACHDB CONNECTOR WITH EVENT COALESCING"
echo "=========================================================="
echo ""

# Check if connection URL is provided
if [ -z "$1" ]; then
  echo "❌ Error: Connection URL required"
  echo ""
  echo "Usage:"
  echo "  $0 '<cockroachdb_connection_url>'"
  echo ""
  echo "Example:"
  echo "  $0 'postgresql://rslee:PASSWORD@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require'"
  echo ""
  echo "Or set environment variable:"
  echo "  export COCKROACHDB_URL='postgresql://...'"
  echo "  $0 \"\$COCKROACHDB_URL\""
  exit 1
fi

CONNECTION_URL="$1"
CONNECTION_NAME="${2:-robert_lee_battle-walrus-11108}"
PIPELINE_ID="${3:-7d02d189-2d1b-4331-aee7-acf24bcd9225}"

echo "Configuration:"
echo "  Connection Name: $CONNECTION_NAME"
echo "  Pipeline ID: $PIPELINE_ID"
echo ""

# Step 1: Create connection with updated options
echo "Step 1: Creating Unity Catalog connection with updated allow list..."
echo "  New options: target_rows, coalesce_split_families"
echo ""
./create_databricks_connection.sh "$CONNECTION_URL" "$CONNECTION_NAME"

if [ $? -ne 0 ]; then
  echo ""
  echo "❌ Failed to create connection"
  exit 1
fi

echo ""
echo "✅ Connection created successfully!"
echo ""

# Step 2: Deploy connector code
echo "Step 2: Deploying connector with event coalescing fix..."
echo ""
./copydir.sh

if [ $? -ne 0 ]; then
  echo ""
  echo "❌ Failed to deploy connector"
  exit 1
fi

echo ""
echo "✅ Connector deployed successfully!"
echo ""

# Step 3: Start pipeline
echo "Step 3: Starting pipeline update..."
echo ""
databricks pipelines start-update "$PIPELINE_ID"

if [ $? -ne 0 ]; then
  echo ""
  echo "❌ Failed to start pipeline"
  exit 1
fi

echo ""
echo "=========================================================="
echo "✅ DEPLOYMENT COMPLETE!"
echo "=========================================================="
echo ""
echo "🎉 New Features Enabled:"
echo "  ✅ Dynamic Batch Sizing"
echo "     - Auto-calculates batch_size based on column families"
echo "     - Configure via 'target_rows' (default: 15,000)"
echo ""
echo "  ✅ Event Coalescing"
echo "     - Merges fragmented split_column_families events"
echo "     - 100,000+ events → ~10,000 complete rows"
echo "     - No more NULL fields!"
echo ""
echo "📊 Expected Results:"
echo "  - Logs will show: 'Coalesced to X complete rows'"
echo "  - Delta table will have ~10,000 rows (not 11 fragmented ones)"
echo "  - All fields will be populated (no NULLs from fragmentation)"
echo ""
echo "🔗 View Pipeline:"
echo "  https://e2-dogfood.staging.cloud.databricks.com/pipelines/$PIPELINE_ID"
echo ""
echo "📝 Verify Success:"
echo "  SELECT COUNT(*) FROM main.robert_lee_cockroachdb.usertable;"
echo "  -- Expected: ~10,000 rows"
echo ""




