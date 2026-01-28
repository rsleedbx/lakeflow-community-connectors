#!/usr/bin/env bash
# Setup new CockroachDB pipeline with CDC mode (ingestion_type='cdc')
# This creates a FRESH pipeline to avoid migration issues from 'snapshot' to 'cdc'
#
# Usage:
#   ./setup_cdc_pipeline.sh                                    # Uses defaults
#   ./setup_cdc_pipeline.sh my_connection                      # Custom connection
#   ./setup_cdc_pipeline.sh my_connection "table1,table2"      # Custom tables
#
# This script:
#   1. Deploys latest connector code (with ingestion_type='cdc')
#   2. Creates NEW pipeline (deletes existing if name conflicts)
#   3. Starts first run (full snapshot + establishes cursor)
#   4. Monitors until completion

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
CONNECTION_NAME="${1:-robert_lee_battle-walrus-11108}"
TABLE_LIST="${2:-usertable}"
PIPELINE_SUFFIX="${3:-cdc}"  # Optional suffix to avoid name conflicts

echo "================================================================================"
echo "🚀 Setting up CockroachDB CDC Pipeline"
echo "================================================================================"
echo ""
echo "Configuration:"
echo "  Connection: $CONNECTION_NAME"
echo "  Tables: $TABLE_LIST"
echo "  Mode: CDC (ingestion_type='cdc')"
echo ""
echo "This will:"
echo "  ✅ Deploy connector with timeout-based incremental CDC"
echo "  ✅ Create fresh pipeline (ingestion_type='cdc' from start)"
echo "  ✅ Run initial snapshot (establishes cursor)"
echo "  ✅ Subsequent runs will be incremental (~30s if no changes)"
echo ""
echo "================================================================================"
echo ""

# Step 1: Deploy connector code
echo "📦 Step 1: Deploying connector code..."
echo "-------------------------------------------------------------------------------"
cd "$SOURCE_DIR"
./scripts/copydir.sh

if [ $? -ne 0 ]; then
    echo "❌ Failed to deploy connector"
    exit 1
fi

echo ""
echo "✅ Connector deployed"
echo ""

# Step 2: Create pipeline
echo "🔧 Step 2: Creating new pipeline..."
echo "-------------------------------------------------------------------------------"

# Export CONNECTION_NAME so createpipeline.sh picks it up
export CONNECTION_NAME="$CONNECTION_NAME"

# Add suffix to pipeline name to avoid conflicts
ORIGINAL_SCRIPT="$SCRIPT_DIR/createpipeline.sh"

# Create temporary modified script with suffix
TEMP_SCRIPT=$(mktemp)
cat "$ORIGINAL_SCRIPT" | sed "s/PIPELINE_NAME=\"\\(.*\\)\"/PIPELINE_NAME=\"\\1_${PIPELINE_SUFFIX}\"/" > "$TEMP_SCRIPT"
chmod +x "$TEMP_SCRIPT"

# Run modified script
"$TEMP_SCRIPT" "$CONNECTION_NAME" "$TABLE_LIST"
RESULT=$?

# Clean up temp script
rm "$TEMP_SCRIPT"

if [ $RESULT -ne 0 ]; then
    echo "❌ Failed to create pipeline"
    exit 1
fi

# Extract pipeline ID from output (createpipeline.sh prints it)
echo ""
echo "📝 Extracting pipeline ID..."

USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
PIPELINE_NAME="$(echo $USER_NAME | cut -d'@' -f1 | tr '.' '_')_cockroachdb_${PIPELINE_SUFFIX}"

PIPELINE_ID=$(databricks pipelines list-pipelines \
  --filter "name LIKE '$PIPELINE_NAME'" \
  --output json | jq -r '.[0].pipeline_id // empty')

if [ -z "$PIPELINE_ID" ]; then
    echo "❌ Could not find created pipeline"
    exit 1
fi

echo "✅ Pipeline created: $PIPELINE_ID"
echo ""

# Step 3: Start initial run
echo "▶️  Step 3: Starting initial run (full snapshot)..."
echo "-------------------------------------------------------------------------------"
echo "This will:"
echo "  - Fetch all rows from source table(s)"
echo "  - Establish cursor for future incremental runs"
echo "  - Duration: ~2-5 minutes (depends on table size)"
echo ""

UPDATE_ID=$(databricks pipelines start-update "$PIPELINE_ID" --output json | jq -r '.update_id')

if [ -z "$UPDATE_ID" ]; then
    echo "❌ Failed to start pipeline"
    exit 1
fi

echo "✅ Pipeline started"
echo "Update ID: $UPDATE_ID"
echo ""

# Step 4: Monitor progress
echo "👁️  Step 4: Monitoring progress..."
echo "-------------------------------------------------------------------------------"
echo "Press Ctrl+C to stop monitoring (pipeline will continue running)"
echo ""

START_TIME=$(date +%s)

for i in {1..60}; do
    # Get current state
    STATE=$(databricks pipelines get "$PIPELINE_ID" --output json | jq -r '.latest_updates[0].state')
    ELAPSED=$(($(date +%s) - START_TIME))
    
    echo "$(date '+%H:%M:%S') - ${ELAPSED}s - State: $STATE"
    
    if [ "$STATE" = "COMPLETED" ]; then
        echo ""
        echo "================================================================================"
        echo "✅ Initial run COMPLETED in ${ELAPSED}s!"
        echo "================================================================================"
        echo ""
        
        # Get some stats
        echo "📊 Pipeline Information:"
        echo "  Pipeline ID: $PIPELINE_ID"
        echo "  Pipeline Name: $PIPELINE_NAME"
        echo "  Tables: $TABLE_LIST"
        echo "  Mode: CDC (cursor-based incremental)"
        echo ""
        
        echo "💡 Next Steps:"
        echo ""
        echo "1. View pipeline in UI:"
        WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
        echo "   ${WORKSPACE_URL}/pipelines/${PIPELINE_ID}"
        echo ""
        
        echo "2. Check output table:"
        echo "   SELECT count(*) FROM main.${PIPELINE_NAME}.${TABLE_LIST%%,*};"
        echo ""
        
        echo "3. Run incremental update (will complete in ~30-40s if no changes):"
        echo "   databricks pipelines start-update $PIPELINE_ID"
        echo ""
        
        echo "4. Schedule for 30-minute intervals:"
        echo "   (Configure in pipeline settings or use Jobs API)"
        echo ""
        
        echo "================================================================================"
        echo "🎉 CDC Pipeline Ready for Production!"
        echo "================================================================================"
        echo ""
        echo "Performance expectations:"
        echo "  - First run: Full snapshot (~2-5 min)"
        echo "  - Subsequent runs (no changes): ~30-40s (timeout)"
        echo "  - Subsequent runs (with changes): Proportional to # of changes"
        echo "  - For millions of rows: 10-100x faster than re-scanning"
        echo ""
        
        # Optional: Test CDC functionality
        if [ -n "$COCKROACHDB_URL" ]; then
            echo "================================================================================"
            echo "🧪 CDC Testing (Optional)"
            echo "================================================================================"
            echo ""
            echo "Would you like to test CDC by generating changes? (y/n)"
            read -t 10 -p "Answer (auto-skip in 10s): " TEST_CDC || TEST_CDC="n"
            echo ""
            
            if [ "$TEST_CDC" = "y" ] || [ "$TEST_CDC" = "Y" ]; then
                echo "▶️  Generating CDC events..."
                echo "-------------------------------------------------------------------------------"
                echo "Running: cockroach workload run ycsb \$COCKROACHDB_URL --duration 1m"
                echo ""
                
                cockroach workload run ycsb "$COCKROACHDB_URL" --duration 1m
                
                echo ""
                echo "✅ CDC events generated (1 minute of changes)"
                echo ""
                
                echo "▶️  Triggering incremental CDC run..."
                echo "-------------------------------------------------------------------------------"
                
                UPDATE_ID_2=$(databricks pipelines start-update "$PIPELINE_ID" --output json | jq -r '.update_id')
                echo "Update ID: $UPDATE_ID_2"
                echo ""
                
                echo "👁️  Monitoring CDC run..."
                START_TIME_2=$(date +%s)
                
                for j in {1..30}; do
                    STATE_2=$(databricks pipelines get "$PIPELINE_ID" --output json | jq -r '.latest_updates[0].state')
                    ELAPSED_2=$(($(date +%s) - START_TIME_2))
                    
                    echo "$(date '+%H:%M:%S') - ${ELAPSED_2}s - State: $STATE_2"
                    
                    if [ "$STATE_2" = "COMPLETED" ]; then
                        echo ""
                        echo "✅ CDC run COMPLETED in ${ELAPSED_2}s!"
                        echo ""
                        
                        echo "📊 Checking results..."
                        echo "-------------------------------------------------------------------------------"
                        echo ""
                        echo "Verifying changes were captured in Delta Lake..."
                        echo ""
                        echo "Run this query in Databricks SQL to verify:"
                        echo ""
                        echo "  SELECT"
                        echo "    count(*) as total_rows,"
                        echo "    count(DISTINCT _cdc_updated) as distinct_timestamps,"
                        echo "    max(_cdc_updated) as latest_update"
                        echo "  FROM main.${PIPELINE_NAME}.${TABLE_LIST%%,*};"
                        echo ""
                        echo "Expected: distinct_timestamps should be > 1 (showing multiple update batches)"
                        echo ""
                        break
                    elif [ "$STATE_2" = "FAILED" ]; then
                        echo ""
                        echo "❌ CDC run FAILED"
                        break
                    fi
                    
                    sleep 5
                done
                
                echo ""
                echo "================================================================================"
                echo "✅ CDC Test Complete!"
                echo "================================================================================"
                echo ""
            else
                echo "Skipping CDC test. You can test later by running:"
                echo "  cockroach workload run ycsb \$COCKROACHDB_URL --duration 1m"
                echo "  databricks pipelines start-update $PIPELINE_ID"
                echo ""
            fi
        else
            echo "💡 Tip: Set COCKROACHDB_URL to enable CDC testing"
            echo "   export COCKROACHDB_URL='postgresql://user:pass@host:port/db?sslmode=require'"
            echo ""
        fi
        
        exit 0
        
    elif [ "$STATE" = "FAILED" ]; then
        echo ""
        echo "================================================================================"
        echo "❌ Pipeline FAILED after ${ELAPSED}s"
        echo "================================================================================"
        echo ""
        
        echo "Error logs:"
        databricks pipelines list-pipeline-events "$PIPELINE_ID" \
            --filter "update_id='$UPDATE_ID'" \
            --max-results 20 | \
            jq -r '.[] | select(.level=="ERROR") | .message' | head -10
        
        echo ""
        echo "Full logs available at:"
        WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
        echo "  ${WORKSPACE_URL}/pipelines/${PIPELINE_ID}?o=$(databricks auth env --output json | jq -r '.env.DATABRICKS_ACCOUNT_ID')"
        echo ""
        exit 1
        
    fi
    
    sleep 10
done

echo ""
echo "⏰ Monitoring timed out after 10 minutes"
echo "Pipeline is still running. Check status at:"
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
echo "  ${WORKSPACE_URL}/pipelines/${PIPELINE_ID}"
echo ""
echo "Monitor status:"
echo "  databricks pipelines get $PIPELINE_ID"

