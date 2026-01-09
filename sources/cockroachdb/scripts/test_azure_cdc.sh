#!/usr/bin/env bash
# Test CockroachDB → Azure Blob CDC with correct URI format
# 
# Comprehensive end-to-end test including:
#   - Changefeed creation/reuse (smart detection)
#   - Heavy YCSB workload (60s, 10 workers) to generate CDC events
#   - Extended wait (120s) for CDC batch flush
#   - Automatic statistics analysis
#
# Usage: ./test_azure_cdc.sh [FORMAT] [WORKLOAD] [--force-new]
#   FORMAT: parquet (default) or json
#   WORKLOAD: manual (default), ycsb, tpcc, or stats (analyze only)
#   --force-new: Cancel existing changefeeds and create a fresh one
#
# Note: If using a virtual environment, activate it before running this script.

set -e

# Use python from PATH (respects virtual environment)
# Fall back to python3 if python is not found
if command -v python &> /dev/null; then
    PYTHON_CMD="python"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
else
    echo "❌ Error: Python not found. Please install Python 3.7+"
    exit 1
fi

# Parse arguments
CHANGEFEED_FORMAT="${1:-parquet}"  # Default to parquet
WORKLOAD_TYPE="${2:-manual}"        # Default to manual SQL updates
FORCE_NEW=false

# Check for --force-new flag in any position
for arg in "$@"; do
    if [ "$arg" = "--force-new" ]; then
        FORCE_NEW=true
    fi
done

export CHANGEFEED_FORMAT  # Export immediately for Python code

if [ "$CHANGEFEED_FORMAT" != "parquet" ] && [ "$CHANGEFEED_FORMAT" != "json" ]; then
    echo "❌ Invalid format: $CHANGEFEED_FORMAT"
    echo "   Usage: $0 [FORMAT] [WORKLOAD]"
    echo "   FORMAT: parquet (default) | json"
    echo "   WORKLOAD: manual (default) | ycsb | tpcc | stats"
    echo ""
    echo "   stats: Skip workload and analyze existing files only"
    exit 1
fi

if [ "$WORKLOAD_TYPE" != "manual" ] && [ "$WORKLOAD_TYPE" != "ycsb" ] && [ "$WORKLOAD_TYPE" != "tpcc" ] && [ "$WORKLOAD_TYPE" != "stats" ]; then
    echo "❌ Invalid workload: $WORKLOAD_TYPE"
    echo "   Usage: $0 [FORMAT] [WORKLOAD]"
    echo "   FORMAT: parquet (default) | json"
    echo "   WORKLOAD: manual (default) | ycsb | tpcc | stats"
    echo ""
    echo "   stats: Skip workload and analyze existing files only"
    exit 1
fi

# Find git root and load credentials from there
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

# Global associative array for credentials (Python-style dictionary in bash)
declare -A azure_creds
declare -A crdb_creds

# Load Azure credentials from JSON
AZURE_JSON="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.json"
if [ -f "$AZURE_JSON" ]; then
    # Check if yq is available
    if ! command -v yq &> /dev/null; then
        echo "❌ Error: yq is required to parse JSON credentials"
        echo "   Install with: brew install yq (macOS) or snap install yq (Linux)"
        exit 1
    fi
    
    # Load Azure credentials into associative array
    while IFS='=' read -r key value; do
        value="${value%\'}"
        value="${value#\'}"
        azure_creds["$key"]="$value"
    done < <(yq -o=shell "$AZURE_JSON")
else
    echo "❌ Missing $AZURE_JSON"
    echo "   Run: sources/cockroachdb/scripts/setup_azure_blob_for_cdc.sh"
    exit 1
fi

# Load CockroachDB credentials from JSON
CRDB_JSON="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_credentials.json"
if [ -f "$CRDB_JSON" ]; then
    # Load CockroachDB credentials into associative array
    while IFS='=' read -r key value; do
        value="${value%\'}"
        value="${value#\'}"
        crdb_creds["$key"]="$value"
    done < <(yq -o=shell "$CRDB_JSON")
else
    echo "❌ Missing $CRDB_JSON"
    echo "   Create it with your CockroachDB credentials"
    echo "   Example: {\"cockroachdb_url\": \"postgresql://user:pass@host:port/database?sslmode=require\"}"
    exit 1
fi

# Verify cockroachdb_url is set
if [ -z "${crdb_creds[cockroachdb_url]}" ]; then
    echo "❌ cockroachdb_url not set in $CRDB_JSON"
    echo "   Add: \"cockroachdb_url\": \"postgresql://user:pass@host:port/database?sslmode=require\""
    exit 1
fi

echo "🧪 Testing CockroachDB → Azure Blob CDC"
echo "========================================"
echo ""

# URL-encode the Azure storage key using jq
ENCODED_KEY=$(echo -n "${azure_creds[azure_storage_key]}" | jq -sRr @uri)

# Construct proper Azure URI with format-specific path
if [ "$CHANGEFEED_FORMAT" = "parquet" ]; then
    PATH_SUFFIX="parquet-cdc"
    FILE_EXTENSION=".parquet"
    FILE_QUERY="[?contains(name, '.parquet')]"
else
    PATH_SUFFIX="json-cdc"
    FILE_EXTENSION=".ndjson"
    FILE_QUERY="[?contains(name, '.ndjson')]"
fi

AZURE_URI="azure://${azure_creds[azure_storage_container]}/${PATH_SUFFIX}?AZURE_ACCOUNT_NAME=${azure_creds[azure_storage_account]}&AZURE_ACCOUNT_KEY=${ENCODED_KEY}"

echo "Configuration:"
echo "  Format: $CHANGEFEED_FORMAT"
echo "  Workload: $WORKLOAD_TYPE"
echo "  Account: ${azure_creds[azure_storage_account]}"
echo "  Container: ${azure_creds[azure_storage_container]}"
echo "  Path: $PATH_SUFFIX"
echo "  URI: azure://${azure_creds[azure_storage_container]}/${PATH_SUFFIX}?..."
echo ""

# Skip changefeed management if stats-only mode
if [ "$WORKLOAD_TYPE" = "stats" ]; then
    echo "ℹ️  Stats-Only Mode: Skipping changefeed management"
    echo "   Analyzing existing files only"
    echo ""
    
    # Jump directly to Step 7
else
    # Step 1: Check and manage existing changefeeds
    if [ "$FORCE_NEW" = true ]; then
        echo "Step 1: Cancelling ALL existing changefeeds (--force-new flag)..."
    else
        echo "Step 1: Checking for existing changefeeds..."
    fi

# Get script directory for helper scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Temporarily disable exit-on-error for changefeed detection
set +e

# Find/manage existing changefeeds
FORCE_NEW_FLAG=""
if [ "$FORCE_NEW" = true ]; then
    FORCE_NEW_FLAG="--force-new"
fi

$PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" find-changefeeds \
    --table usertable \
    --json "$CRDB_JSON" \
    $FORCE_NEW_FLAG

# Capture exit code and re-enable exit-on-error
EXIT_CODE=$?
set -e
SKIP_CREATION=false

if [ $EXIT_CODE -eq 42 ]; then
    # Healthy changefeed exists - get its job ID
    CHANGEFEED_JOB_ID=$($PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" get-latest-job \
        --table usertable \
        --json "$CRDB_JSON")
    
    SKIP_CREATION=true
    echo ""
    echo "  ℹ️  Will use existing changefeed for testing: Job $CHANGEFEED_JOB_ID"
fi

echo ""

# Step 2: Create changefeed with correct Azure URI (or skip if exists)
if [ "$SKIP_CREATION" = true ]; then
    echo "Step 2: Skipping changefeed creation (using existing Job $CHANGEFEED_JOB_ID)..."
else
    echo "Step 2: Creating changefeed with correct Azure URI..."
fi

# Only create if we don't have a healthy one
if [ "$SKIP_CREATION" = false ]; then
CHANGEFEED_JOB_ID=$($PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" create-changefeed \
    --table usertable \
    --azure-uri "$AZURE_URI" \
    --format "$CHANGEFEED_FORMAT" \
    --json "$CRDB_JSON")

    if [ $? -eq 0 ] && [ ! -z "$CHANGEFEED_JOB_ID" ]; then
        echo "  ✅ Changefeed created: Job $CHANGEFEED_JOB_ID"
    else
        echo "  ❌ Failed to create changefeed"
        exit 1
    fi
fi

echo ""

# Step 3: Wait for changefeed initialization
if [ "$SKIP_CREATION" = false ]; then
    echo "Step 3: Waiting 10 seconds for changefeed to initialize..."
    sleep 10
else
    echo "Step 3: Skipping initialization wait (using existing changefeed)..."
fi
echo ""

# Step 4: Check job status
echo "Step 4: Checking changefeed status..."

# Store exit code separately since we check it later
set +e
$PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" check-status \
    --job-id "$CHANGEFEED_JOB_ID" \
    --json "$CRDB_JSON"
STATUS_CHECK_EXIT=$?
set -e

export CHANGEFEED_JOB_ID
export CHANGEFEED_FORMAT

if [ $STATUS_CHECK_EXIT -ne 0 ]; then
    echo ""
    echo "❌ Changefeed has errors. Exiting."
    exit 1
fi

echo ""

# Step 4.5: Wait for initial scan to complete (only if changefeed was just created)
if [ "$SKIP_CREATION" = false ]; then
    echo "Step 4.5: Waiting for initial scan to complete..."
    echo "  ℹ️  This establishes timestamp cutoff for UPDATE detection in Parquet format"
    echo "  📝 Parquet: Events after this point will be marked as UPDATE (not SNAPSHOT)"
    echo "  📝 JSON: Has explicit before/after fields for UPDATE detection"
    echo ""
    echo "  📊 Table: 9,995 rows × 11 column families = ~110,000 events to scan"
    echo "  ⏱️  Expected time: 60-120 seconds"
    echo ""
    
    # Poll for initial snapshot files with extended timeout
    MAX_WAIT=180  # 3 minutes max
    POLL_INTERVAL=15
    ELAPSED=0
    SNAPSHOT_FILES=0
    PREV_COUNT=0
    STABLE_COUNT=0
    
    while [ $ELAPSED -lt $MAX_WAIT ]; do
        sleep $POLL_INTERVAL
        ELAPSED=$((ELAPSED + POLL_INTERVAL))
        
        SNAPSHOT_FILES=$(az storage blob list \
            --account-name ${azure_creds[azure_storage_account]} \
            --account-key "${azure_creds[azure_storage_key]}" \
            --container-name ${azure_creds[azure_storage_container]} \
            --prefix "${PATH_SUFFIX}/" \
            --query "$FILE_QUERY" \
            --output tsv 2>/dev/null | wc -l | tr -d ' ')
        
        if [ "$SNAPSHOT_FILES" -gt 0 ]; then
            if [ "$SNAPSHOT_FILES" -eq "$PREV_COUNT" ]; then
                STABLE_COUNT=$((STABLE_COUNT + 1))
                echo "  ⏳ [${ELAPSED}s] Files stable: $SNAPSHOT_FILES (stability: ${STABLE_COUNT}/2)"
                
                if [ "$STABLE_COUNT" -ge 2 ]; then
                    echo ""
                    echo "  ✅ Initial scan complete! ($SNAPSHOT_FILES files, stable for $((STABLE_COUNT * POLL_INTERVAL))s)"
                    echo "  ⏳ Waiting 20s for changefeed to reach steady state..."
                    sleep 20
                    echo "  ✅ Changefeed now in steady state (ready for CDC)"
                    break
                fi
            else
                STABLE_COUNT=0
                echo "  📈 [${ELAPSED}s] Files appearing: $SNAPSHOT_FILES (+$((SNAPSHOT_FILES - PREV_COUNT)))"
            fi
            PREV_COUNT=$SNAPSHOT_FILES
        else
            echo "  ⏳ [${ELAPSED}s] Waiting for initial scan files to appear..."
        fi
    done
    
    if [ "$SNAPSHOT_FILES" -eq 0 ]; then
        echo ""
        echo "  ❌ ERROR: No snapshot files appeared after ${MAX_WAIT}s!"
        echo "  ℹ️  Possible causes:"
        echo "     - Changefeed configuration error"
        echo "     - Azure storage credentials issue"
        echo "     - CockroachDB cluster connectivity problem"
        echo ""
        exit 1
    fi
    
    echo ""
fi

fi  # End of changefeed management section (skipped in stats-only mode)

# Skip workload if stats-only mode
if [ "$WORKLOAD_TYPE" != "stats" ]; then
    # Step 5: Run workload
    echo "Step 5: Running $WORKLOAD_TYPE workload for CDC testing..."

if [ "$WORKLOAD_TYPE" = "manual" ]; then
    echo "  Method: Direct SQL UPDATEs (proven to trigger CDC flush)"
    echo "  Target: 10,000 UPDATE operations with 100-byte padding"
    echo ""
    
    # Get row count first
    ROW_COUNT=$($PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" get-row-count \
        --table usertable \
        --json "$CRDB_JSON")
    
    echo "  Table has $ROW_COUNT rows"
    echo ""
    
    if [ "$ROW_COUNT" -eq 0 ]; then
        echo "  ⚠️  Table is empty! Initializing with YCSB data..."
        cockroach workload init ycsb "${crdb_creds[cockroachdb_url]}" 2>&1 | tail -3
        echo "  ✅ Table initialized"
        echo ""
    fi
    
    # Run multiple UPDATE batches to generate enough CDC data
    # Each batch updates 500 rows with a longer string to ensure we hit the 1MB threshold
    echo "  Running 20 UPDATE batches (500 rows each)..."
    echo ""
    
    for i in {1..20}; do
        OFFSET=$(( (i-1) * 500 ))
        PADDING=$(printf '_%.0s' {1..100})  # 100 character padding
        BATCH_MARKER="_batch${i}${PADDING}"
        
        $PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" execute-sql \
            --sql "UPDATE usertable SET field0 = field0 || '${BATCH_MARKER}' WHERE ycsb_key IN (SELECT ycsb_key FROM usertable ORDER BY ycsb_key LIMIT 500 OFFSET ${OFFSET})" \
            --json "$CRDB_JSON" \
            --commit >/dev/null
        
        echo -n "."
        if [ $((i % 5)) -eq 0 ]; then
            echo " [$i/20 complete]"
        fi
    done
    
    echo ""
    echo ""
    echo "  ✅ Workload complete:"
    echo "     - 10,000 UPDATE operations"
    echo "     - ~100 bytes padding per update"  
    echo "     - Total data: ~1MB+ (guaranteed to exceed CDC flush threshold)"
    echo ""

elif [ "$WORKLOAD_TYPE" = "ycsb" ]; then
    echo "  Method: YCSB benchmark workload"
    echo "  Target: Mixed read/write operations (60s duration)"
    echo ""
    
    # Initialize if needed
    TABLE_EXISTS=$($PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" get-row-count \
        --table usertable \
        --json "$CRDB_JSON" 2>/dev/null || echo "0")
    
    if [ "$TABLE_EXISTS" -eq 0 ]; then
        echo "  Initializing YCSB workload (10,000 rows)..."
        cockroach workload init ycsb "${crdb_creds[cockroachdb_url]}" --rows 10000 2>&1 | grep -E "(creating|initialized)" || true
        echo "  ✅ Initialization complete"
        echo ""
    fi
    
    echo "  Running YCSB workload (90s, 20 workers)..."
    cockroach workload run ycsb "${crdb_creds[cockroachdb_url]}" --duration 90s --concurrency 20 2>&1 | tail -5
    echo ""
    echo "  ✅ YCSB workload complete"
    echo ""

elif [ "$WORKLOAD_TYPE" = "tpcc" ]; then
    echo "  Method: TPC-C benchmark workload"
    echo "  Target: OLTP transactions (60s duration, 10 warehouses)"
    echo ""
    
    # Check if TPCC is initialized
    TPCC_EXISTS=$($PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" get-row-count \
        --table warehouse \
        --json "$CRDB_JSON" 2>/dev/null || echo "0")
    
    if [ "$TPCC_EXISTS" -eq 0 ]; then
        echo "  Initializing TPC-C workload (10 warehouses, ~100MB data)..."
        cockroach workload init tpcc "${crdb_creds[cockroachdb_url]}" --warehouses 10 2>&1 | grep -E "(creating|initialized)" || true
        echo "  ✅ Initialization complete (this is a one-time operation)"
        echo ""
    fi
    
    echo "  Running TPC-C workload (60s, 10 warehouses)..."
    cockroach workload run tpcc "${crdb_creds[cockroachdb_url]}" --duration 60s --warehouses 10 2>&1 | tail -5
    echo ""
    echo "  ✅ TPC-C workload complete"
    echo ""
fi

# Step 6: Smart wait with dynamic file detection
echo "Step 6: Waiting for CDC events to flush to storage..."
echo ""
echo "  ℹ️  Cloud Storage Changefeed Flush Behavior:"
echo "     - Snapshot events: Flush within 30 seconds"
echo "     - CDC events: Batch until ~1MB threshold is met"
echo "     - After threshold: Files appear in 30-90 seconds"
echo ""
echo "  🔍 Smart wait: Checking for new files every 10 seconds (max 120s)..."
echo ""

# Count snapshot files (should be 11 or 22 for split_column_families)
INITIAL_FILE_COUNT=$(az storage blob list \
    --account-name ${azure_creds[azure_storage_account]} \
    --account-key "${azure_creds[azure_storage_key]}" \
    --container-name ${azure_creds[azure_storage_container]} \
    --prefix "${PATH_SUFFIX}/" \
    --query "$FILE_QUERY" \
    --output tsv 2>/dev/null | wc -l | tr -d ' ')

echo "  📊 Initial files: $INITIAL_FILE_COUNT (snapshot)"
echo ""

# Smart wait: Check for new files every 10 seconds
CDC_FILES_DETECTED=false
for i in {1..12}; do
    sleep 10
    
    CURRENT_FILE_COUNT=$(az storage blob list \
        --account-name ${azure_creds[azure_storage_account]} \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name ${azure_creds[azure_storage_container]} \
        --prefix "${PATH_SUFFIX}/" \
        --query "$FILE_QUERY" \
        --output tsv 2>/dev/null | wc -l | tr -d ' ')
    
    NEW_FILES=$((CURRENT_FILE_COUNT - INITIAL_FILE_COUNT))
    
    if [ "$NEW_FILES" -gt 0 ]; then
        echo "  ✅ CDC files detected! (+$NEW_FILES files after $((i*10))s)"
        CDC_FILES_DETECTED=true
        # Wait 10 more seconds for batch to complete
        echo "  ⏳ Waiting 10 more seconds for batch completion..."
        sleep 10
        break
    else
        echo "  ⏳ [$((i*10))s] No new files yet... (files: $CURRENT_FILE_COUNT)"
    fi
done

echo ""
if [ "$CDC_FILES_DETECTED" = true ]; then
    echo "  ✅ CDC flush complete (early detection)"
else
    echo "  ⏰ Max wait time reached (120s)"
    echo "  ℹ️  If no CDC files appeared, workload may need more data volume"
fi
echo ""
fi  # End of workload/wait section (skipped in stats-only mode)

# Step 7: Check Azure Blob
if [ "$WORKLOAD_TYPE" = "stats" ]; then
    echo "Step 7: Analyzing existing files in Azure Blob Storage..."
else
    echo "Step 7: Checking Azure Blob Storage..."
fi

BLOB_COUNT=$(az storage blob list \
    --account-name ${azure_creds[azure_storage_account]} \
    --account-key "${azure_creds[azure_storage_key]}" \
    --container-name ${azure_creds[azure_storage_container]} \
    --prefix "${PATH_SUFFIX}/" \
    --query "$FILE_QUERY" \
    --output tsv 2>/dev/null | wc -l | tr -d ' ')

if [ "$BLOB_COUNT" -gt 0 ]; then
    echo "  ✅ Found $BLOB_COUNT $CHANGEFEED_FORMAT file(s)!"
    echo ""
    
    # List files
    echo "  Event files:"
    az storage blob list \
        --account-name ${azure_creds[azure_storage_account]} \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name ${azure_creds[azure_storage_container]} \
        --prefix "${PATH_SUFFIX}/" \
        --output table
    echo ""
    
    # Download first file
    FIRST_BLOB=$(az storage blob list \
        --account-name ${azure_creds[azure_storage_account]} \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name ${azure_creds[azure_storage_container]} \
        --prefix "${PATH_SUFFIX}/" \
        --query "$FILE_QUERY.name" \
        --output tsv 2>/dev/null | head -1)
    
    if [ ! -z "$FIRST_BLOB" ]; then
        echo "  📥 Downloading sample: $FIRST_BLOB"
        
        if [ "$CHANGEFEED_FORMAT" = "parquet" ]; then
            SAMPLE_FILE="/tmp/azure_sample_events.parquet"
        else
            SAMPLE_FILE="/tmp/azure_sample_events.ndjson"
        fi
        
        az storage blob download \
            --account-name ${azure_creds[azure_storage_account]} \
            --account-key "${azure_creds[azure_storage_key]}" \
            --container-name ${azure_creds[azure_storage_container]} \
            --name "$FIRST_BLOB" \
            --file "$SAMPLE_FILE" \
            --no-progress 2>/dev/null
        
        if [ "$CHANGEFEED_FORMAT" = "json" ]; then
            EVENT_COUNT=$(wc -l < "$SAMPLE_FILE" | tr -d ' ')
            echo "  📊 Events in file: $EVENT_COUNT"
            echo ""
            echo "  🔍 First event:"
            head -1 "$SAMPLE_FILE" | jq '.' 2>/dev/null
        else
            echo "  ✅ Parquet file downloaded"
            echo "  📊 File size: $(ls -lh "$SAMPLE_FILE" | awk '{print $5}')"
            echo ""
            echo "  💡 To inspect Parquet file:"
            echo "     python3 -c 'import pandas as pd; df = pd.read_parquet(\"$SAMPLE_FILE\"); print(df.head())'"
        fi
    fi
else
    echo "  ⚠️  No event files found yet"
    echo "  All blobs:"
    az storage blob list \
        --account-name ${azure_creds[azure_storage_account]} \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name ${azure_creds[azure_storage_container]} \
        --output table
fi

echo ""
if [ "$WORKLOAD_TYPE" = "stats" ]; then
    echo "✅ Stats-Only Mode Complete!"
else
    echo "✅ Azure CDC Test Complete!"
fi
echo ""

# Step 8: Analyze changefeed statistics
if [ "$WORKLOAD_TYPE" = "stats" ]; then
    echo "📊 CHANGEFEED STATISTICS"
    echo "════════════════════════════════════════════════════════════════"
else
    echo "Step 8: Analyzing changefeed statistics..."
fi
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Call analyzer using unified CLI
$PYTHON_CMD "$SCRIPT_DIR/changefeed_helper.py" analyze-files \
    --format "$CHANGEFEED_FORMAT" \
    --account "${azure_creds[azure_storage_account]}" \
    --key "${azure_creds[azure_storage_key]}" \
    --container "${azure_creds[azure_storage_container]}"

echo ""

# Display changefeed details
echo "📊 Changefeed Details:"
echo "  Job ID: $CHANGEFEED_JOB_ID"
echo "  Azure URI: azure://${azure_creds[azure_storage_container]}/$PATH_SUFFIX/..."
echo "  Created: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "  Storage Account: ${azure_creds[azure_storage_account]}"
echo "  Container: ${azure_creds[azure_storage_container]}"
echo ""

