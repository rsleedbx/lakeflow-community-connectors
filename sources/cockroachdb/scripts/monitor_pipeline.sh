#!/usr/bin/env bash
# Monitor CockroachDB Pipeline Execution
# Usage: ./monitor_pipeline.sh [pipeline_name] [catalog] [schema] [pipeline_id] [--no-start]

set -e

PIPELINE_NAME="${1:-robert_lee_cockroachdb}"
CATALOG="${2:-main}"
SCHEMA="${3:-robert_lee_cockroachdb_cdc}"
PIPELINE_ID="${4-''}"
NO_START=false

# Check for --no-start flag in any remaining arguments
shift 4 2>/dev/null || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-start)
            NO_START=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo "🚀 CockroachDB Pipeline Monitor"
echo "════════════════════════════════════════════════════════"
echo "   Pipeline: $PIPELINE_NAME"
echo "   Catalog: $CATALOG"
echo "   Schema: $SCHEMA"
echo "════════════════════════════════════════════════════════"
echo ""

# Get pipeline ID
if [[ -z $PIPELINE_ID ]]; then 
    echo "📋 Step 1: Getting pipeline ID..."
    PIPELINE_ID=$(databricks pipelines list-pipelines --filter "name LIKE '$PIPELINE_NAME'" --output json 2>/dev/null | jq -r ".[] | select(.name == \"$PIPELINE_NAME\") | .pipeline_id")
fi

if [ -z "$PIPELINE_ID" ] || [ "$PIPELINE_ID" == "null" ]; then
    echo "❌ Pipeline not found: $PIPELINE_NAME"
    echo ""
    echo "Available pipelines:"
    databricks pipelines list-pipelines --output json 2>/dev/null | jq -r '.[] | "  - \(.name)"' | head -20
    exit 1
fi

echo "   ✅ Pipeline ID: $PIPELINE_ID"
echo ""

# Start full refresh or monitor existing
if [ "$NO_START" = false ]; then
    echo "🔄 Step 2: Starting full refresh..."
    UPDATE_OUTPUT=$(databricks pipelines start-update "$PIPELINE_ID" \
      --full-refresh \
      --output json 2>&1)

    UPDATE_ID=$(echo "$UPDATE_OUTPUT" | jq -r '.update_id' 2>/dev/null)

    if [ -z "$UPDATE_ID" ] || [ "$UPDATE_ID" == "null" ]; then
        echo "❌ Failed to start update"
        echo "$UPDATE_OUTPUT"
        exit 1
    fi

    echo "   ✅ Update ID: $UPDATE_ID"
    echo "   ✅ Started at: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
else
    echo "🔍 Step 2: Getting latest update..."
    # Get the most recent update for this pipeline
    PIPELINE_INFO=$(databricks pipelines get "$PIPELINE_ID" --output json 2>/dev/null)
    UPDATE_ID=$(echo "$PIPELINE_INFO" | jq -r '.latest_updates[0].update_id // empty')
    
    if [ -z "$UPDATE_ID" ]; then
        echo "❌ No active or recent updates found for this pipeline"
        echo ""
        echo "💡 To start a new run, use:"
        echo "   ./monitor_pipeline.sh $PIPELINE_NAME $CATALOG $SCHEMA $PIPELINE_ID"
        exit 1
    fi
    
    UPDATE_STATE=$(echo "$PIPELINE_INFO" | jq -r '.latest_updates[0].state // empty')
    echo "   ✅ Update ID: $UPDATE_ID"
    echo "   ℹ️  Current State: $UPDATE_STATE"
    echo "   ℹ️  Monitoring mode: Will not start new update"
    echo ""
fi

# Monitor progress
echo "⏳ Step 3: Monitoring progress..."
echo "────────────────────────────────────────────────────────"
START_TIME=$(date +%s)
ITERATION=0
LAST_STATE=""

while true; do
  ITERATION=$((ITERATION + 1))
  
  # Get update status
  UPDATE_JSON=$(databricks pipelines get-update "$PIPELINE_ID" "$UPDATE_ID" \
    --output json 2>/dev/null)
  
  STATE=$(echo "$UPDATE_JSON" | jq -r '.update.state' 2>/dev/null)
  CURRENT_TIME=$(date +%s)
  ELAPSED=$((CURRENT_TIME - START_TIME))
  
  # Show status if changed or every 30 seconds
  if [ "$STATE" != "$LAST_STATE" ] || [ $((ITERATION % 3)) -eq 0 ]; then
    TIMESTAMP=$(date '+%H:%M:%S')
    printf "   [%s] [%3ds] State: %-15s\n" "$TIMESTAMP" "$ELAPSED" "$STATE"
    LAST_STATE="$STATE"
  fi
  
  # Check if complete
  if [[ "$STATE" == "COMPLETED" ]]; then
    echo ""
    echo "✅ Pipeline completed successfully!"
    break
  elif [[ "$STATE" == "FAILED" ]]; then
    echo ""
    echo "❌ Pipeline failed!"
    echo ""
    echo "Error details:"
    echo "$UPDATE_JSON" | jq -r '.update.events[] | select(.level == "ERROR") | "   [\(.timestamp)] \(.message)"'
    echo ""
    echo "💡 Debug with:"
    echo "   databricks pipelines get-update --pipeline-id $PIPELINE_ID --update-id $UPDATE_ID --output json | jq '.update.events'"
    exit 1
  elif [[ "$STATE" == "CANCELED" ]]; then
    echo ""
    echo "⚠️  Pipeline was canceled"
    exit 1
  fi
  
  sleep 10
done

# Get final metrics
echo ""
echo "📊 Step 4: Final Metrics"
echo "════════════════════════════════════════════════════════"

UPDATE_JSON=$(databricks pipelines get-update "$PIPELINE_ID" "$UPDATE_ID" \
  --output json 2>/dev/null)

# Extract and display metrics
DURATION=$(echo "$UPDATE_JSON" | jq -r 'if .update.completion_time and .update.start_time then ((.update.completion_time - .update.start_time) / 1000 | floor) else "N/A" end')
INPUT_ROWS=$(echo "$UPDATE_JSON" | jq -r '.update.metrics.num_input_rows // "N/A"')
OUTPUT_ROWS=$(echo "$UPDATE_JSON" | jq -r '.update.metrics.num_output_rows // "N/A"')
INPUT_BYTES=$(echo "$UPDATE_JSON" | jq -r '.update.metrics.num_input_bytes // "N/A"')

printf "   %-20s %s\n" "Duration:" "${DURATION}s"
printf "   %-20s %s\n" "Input Rows:" "$INPUT_ROWS"
printf "   %-20s %s\n" "Output Rows:" "$OUTPUT_ROWS"
printf "   %-20s %s\n" "Input Bytes:" "$INPUT_BYTES"

echo ""

# Get table row counts
echo "📋 Step 5: Table Row Counts"
echo "════════════════════════════════════════════════════════"

# Get warehouse ID
WAREHOUSE_ID=$(databricks warehouses list --output json 2>/dev/null | jq -r '.[0].id')

if [ -z "$WAREHOUSE_ID" ] || [ "$WAREHOUSE_ID" == "null" ]; then
    echo "   ⚠️  No warehouse available for queries"
else
    # List tables
    TABLES=$(databricks sql query \
      --warehouse-id "$WAREHOUSE_ID" \
      --statement "SHOW TABLES IN ${CATALOG}.${SCHEMA}" \
      --output json 2>/dev/null | jq -r '.result.data_array[][1]' 2>/dev/null || echo "")
    
    if [ -z "$TABLES" ]; then
        echo "   ⚠️  Could not retrieve table list"
    else
        for TABLE in $TABLES; do
            COUNT=$(databricks sql query \
              --warehouse-id "$WAREHOUSE_ID" \
              --statement "SELECT COUNT(*) FROM ${CATALOG}.${SCHEMA}.${TABLE}" \
              --output json 2>/dev/null | jq -r '.result.data_array[0][0]' 2>/dev/null || echo "Error")
            
            printf "   %-35s %10s rows\n" "$TABLE:" "$COUNT"
        done
    fi
fi

echo ""

# Check for warnings
WARN_COUNT=$(echo "$UPDATE_JSON" | jq '[.update.events[] | select(.level == "WARN")] | length')
ERROR_COUNT=$(echo "$UPDATE_JSON" | jq '[.update.events[] | select(.level == "ERROR")] | length')

if [ "$WARN_COUNT" -gt 0 ]; then
    echo "⚠️  Warnings: $WARN_COUNT"
    echo "$UPDATE_JSON" | jq -r '.update.events[] | select(.level == "WARN") | "   \(.message)"' | head -5
    echo ""
fi

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "❌ Errors: $ERROR_COUNT"
    echo "$UPDATE_JSON" | jq -r '.update.events[] | select(.level == "ERROR") | "   \(.message)"' | head -5
    echo ""
fi

# Summary
echo "════════════════════════════════════════════════════════"
echo "✅ Monitoring Complete!"
echo ""
echo "📊 Summary:"
echo "   Pipeline: $PIPELINE_NAME"
echo "   Update ID: $UPDATE_ID"
echo "   Duration: ${DURATION}s"
echo "   Output Rows: $OUTPUT_ROWS"
echo "   Status: $STATE"
echo ""
echo "💡 View in UI:"
echo "   ${DATABRICKS_HOST}#joblist/pipelines/$PIPELINE_ID/updates/$UPDATE_ID"
echo ""

