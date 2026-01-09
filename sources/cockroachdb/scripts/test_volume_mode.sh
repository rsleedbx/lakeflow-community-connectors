#!/usr/bin/env bash
set -e

#######################################
# Test CockroachDB Connector - Volume Mode
#
# Tests the cockroachdb.py connector reading from Unity Catalog Volume
#######################################

echo "═══════════════════════════════════════════════════════════════"
echo "Test CockroachDB Connector - Volume Mode"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Configuration
USER_NAME="robert.lee@databricks.com"
WORKSPACE_DIR="/Workspace/Users/${USER_NAME}/cockroachdb_tests"
NOTEBOOK_PATH="${WORKSPACE_DIR}/test_volume_mode"
VOLUME_PATH="dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files"

# Local files
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="$(dirname "$SCRIPT_DIR")"
NOTEBOOK_LOCAL="${SOURCE_DIR}/test_volume_mode.py"
CONNECTOR_LOCAL="${SOURCE_DIR}/cockroachdb.py"

echo "📋 Configuration:"
echo "  Test: CockroachDB Connector Volume Mode"
echo "  Volume: ${VOLUME_PATH}"
echo "  Notebook: ${NOTEBOOK_PATH}"
echo ""

#######################################
# Step 1: Verify Volume Has Files
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 1: Verify Volume Has Files"
echo "═══════════════════════════════════════════════════════════════"

FILE_COUNT=$(databricks fs ls "${VOLUME_PATH}/" 2>/dev/null | grep -c '\.parquet$' || echo "0")

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "❌ No Parquet files found in Volume!"
    echo "   Volume Path: ${VOLUME_PATH}"
    echo ""
    echo "🔧 Please run sync_azure_to_volume.sh first"
    exit 1
fi

echo "✅ Volume contains ${FILE_COUNT} Parquet files"
echo ""

#######################################
# Step 2: Upload Connector
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 2: Upload CockroachDB Connector"
echo "═══════════════════════════════════════════════════════════════"

# Create workspace directory
echo "Creating workspace directory: ${WORKSPACE_DIR}"
databricks workspace mkdirs "${WORKSPACE_DIR}"

# Upload connector as notebook (for easy import)
CONNECTOR_NOTEBOOK="${WORKSPACE_DIR}/cockroachdb"
echo "Uploading connector: ${CONNECTOR_LOCAL}"
echo "                 to: ${CONNECTOR_NOTEBOOK}"

databricks workspace import \
  "${CONNECTOR_NOTEBOOK}" \
  --file "${CONNECTOR_LOCAL}" \
  --language PYTHON \
  --format SOURCE \
  --overwrite

echo "✅ Connector uploaded"
echo ""

#######################################
# Step 3: Upload Test Notebook
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 3: Upload Test Notebook"
echo "═══════════════════════════════════════════════════════════════"

echo "Uploading: ${NOTEBOOK_LOCAL}"
echo "       to: ${NOTEBOOK_PATH}"

databricks workspace import \
  "${NOTEBOOK_PATH}" \
  --file "${NOTEBOOK_LOCAL}" \
  --language PYTHON \
  --format SOURCE \
  --overwrite

echo "✅ Test notebook uploaded"
echo ""

#######################################
# Step 4: Run Test
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 4: Run Test Notebook"
echo "═══════════════════════════════════════════════════════════════"

echo "🚀 Running test notebook..."
echo "   (This may take a few minutes)"
echo ""

# Create a one-time job to run the test
JOB_JSON=$(cat <<EOF
{
  "run_name": "CockroachDB Volume Mode Test - $(date +%Y%m%d_%H%M%S)",
  "tasks": [
    {
      "task_key": "test_task",
      "notebook_task": {
        "notebook_path": "${NOTEBOOK_PATH}",
        "source": "WORKSPACE"
      },
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 1
      },
      "libraries": [],
      "timeout_seconds": 3600
    }
  ]
}
EOF
)

echo "$JOB_JSON" > /tmp/test_job.json
RUN_RESPONSE=$(databricks jobs submit --json @/tmp/test_job.json)
RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
rm /tmp/test_job.json

if [ -z "$RUN_ID" ] || [ "$RUN_ID" == "null" ]; then
    echo "❌ Failed to submit test job"
    echo "$RUN_RESPONSE"
    exit 1
fi

echo "📊 Test job submitted: Run ID = ${RUN_ID}"
echo "   View progress: https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111#job/${RUN_ID}"
echo ""

echo "⏳ Waiting for test to complete..."

# Poll for completion
for i in {1..120}; do
    RUN_STATE=$(databricks runs get --run-id "${RUN_ID}" | jq -r '.state.life_cycle_state // "UNKNOWN"')
    
    if [ "$RUN_STATE" == "TERMINATED" ]; then
        RESULT_STATE=$(databricks runs get --run-id "${RUN_ID}" | jq -r '.state.result_state // "UNKNOWN"')
        if [ "$RESULT_STATE" == "SUCCESS" ]; then
            echo ""
            echo "✅ Test completed successfully!"
            break
        else
            echo ""
            echo "❌ Test failed with state: ${RESULT_STATE}"
            echo ""
            echo "📋 Getting test output..."
            databricks runs get-output --run-id "${RUN_ID}"
            exit 1
        fi
    elif [ "$RUN_STATE" == "INTERNAL_ERROR" ] || [ "$RUN_STATE" == "SKIPPED" ]; then
        echo ""
        echo "❌ Test job failed: ${RUN_STATE}"
        exit 1
    fi
    
    echo "   Status: ${RUN_STATE}... (${i}/120)"
    sleep 5
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "✅ Test Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📊 Results:"
echo "   Notebook: ${NOTEBOOK_PATH}"
echo "   Run: https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111#job/${RUN_ID}"
echo "   Output Table: main.robert_lee_cockroachdb.usertable_volume_test"
echo ""
echo "🔍 Query results:"
echo "   SELECT * FROM main.robert_lee_cockroachdb.usertable_volume_test;"
echo ""
echo "📋 Check in notebook output:"
echo "   - Row counts"
echo "   - Sample data"  
echo "   - CDC metadata (_cdc_key, _cdc_updated, _cdc_operation)"
echo "   - Cursor tracking (_source_file)"
echo ""

