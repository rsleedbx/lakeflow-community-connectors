#!/usr/bin/env bash
set -euo pipefail

#######################################
# Unified CockroachDB Pipeline Deployment
#
# Supports all connector modes:
# - DIRECT: Sinkless changefeed (testing)
# - VOLUME: Unity Catalog Volumes (file-based)
# - AZURE_PARQUET: Azure Blob Storage (Parquet)
# - AZURE_JSON: Azure Blob Storage (JSON)
# - AZURE_DUAL: Azure Blob Storage (both formats)
#
# Configuration:
# - Single source: .env/cockroachdb_pipelines.json
# - Supports multiple pre-configured deployments
# - Pythonic bash style (JSON + associative arrays)
#
# Usage:
#   ./deploy_pipeline.sh [--force] [--no-start] [--config CONFIG_FILE]
#
# Options:
#   --force        Delete existing pipeline without prompting
#   --no-start     Create pipeline but don't start it
#   --config FILE  Use alternate config file (default: .env/cockroachdb_pipelines.json)
#######################################

echo "═══════════════════════════════════════════════════════════════"
echo "🚀 CockroachDB Unified Pipeline Deployment"
echo "═══════════════════════════════════════════════════════════════"
echo ""

#######################################
# Parse arguments
#######################################
FORCE_DELETE=false
NO_START=false
CONFIG_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE_DELETE=true
            shift
            ;;
        --no-start)
            NO_START=true
            shift
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        *)
            echo "❌ Unknown option: $1"
            echo ""
            echo "Usage: $0 [--force] [--no-start] [--config FILE]"
            exit 1
            ;;
    esac
done

#######################################
# Find git root and set paths
#######################################
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

COCKROACH_DIR="$GIT_ROOT/sources/cockroachdb"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set default config file if not specified
if [ -z "$CONFIG_FILE" ]; then
    CONFIG_FILE="$COCKROACH_DIR/.env/cockroachdb_pipelines.json"
else
    # If config file is not an absolute path, make it relative to COCKROACH_DIR
    if [[ "$CONFIG_FILE" != /* ]]; then
        CONFIG_FILE="$COCKROACH_DIR/$CONFIG_FILE"
    fi
fi

# Verify config exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Configuration file not found: $CONFIG_FILE"
    echo ""
    echo "Expected format:"
    echo "{"
    echo "  \"pipeline_name\": \"my_cockroachdb_pipeline\","
    echo "  \"mode\": \"volume|direct|azure_parquet|azure_json|azure_dual\","
    echo "  \"catalog\": \"main\","
    echo "  \"schema\": \"my_schema\","
    echo "  \"volume_name\": \"my_volume\",  // For VOLUME mode"
    echo "  \"connection_name\": \"cockroachdb_connection\",  // For DIRECT mode"
    echo "  \"table_list\": \"usertable,orders\"  // For DIRECT mode"
    echo "}"
    exit 1
fi

#######################################
# Check dependencies
#######################################
if ! command -v yq &> /dev/null; then
    echo "❌ Error: yq is required to parse JSON configuration"
    echo "   Install: brew install yq (macOS) or snap install yq (Linux)"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "❌ Error: jq is required for JSON manipulation"
    echo "   Install: brew install jq (macOS) or apt install jq (Linux)"
    exit 1
fi

#######################################
# Load configuration
#######################################
echo "📋 Loading configuration from: $CONFIG_FILE"

declare -A config
while IFS='=' read -r key value; do
    # Remove surrounding quotes
    value="${value%\'}"
    value="${value#\'}"
    config["$key"]="$value"
done < <(yq -o=shell "$CONFIG_FILE")

# Extract required fields
PIPELINE_NAME="${config[pipeline_name]}"
MODE="${config[mode]}"
CATALOG="${config[catalog]}"
SCHEMA="${config[schema]}"

# Validate required fields
if [ -z "$PIPELINE_NAME" ] || [ -z "$MODE" ] || [ -z "$CATALOG" ] || [ -z "$SCHEMA" ]; then
    echo "❌ Missing required fields in config:"
    echo "   pipeline_name: ${PIPELINE_NAME:-MISSING}"
    echo "   mode: ${MODE:-MISSING}"
    echo "   catalog: ${CATALOG:-MISSING}"
    echo "   schema: ${SCHEMA:-MISSING}"
    exit 1
fi

# Validate mode
case "$MODE" in
    direct|volume|azure_parquet|azure_json|azure_dual)
        ;;
    *)
        echo "❌ Invalid mode: $MODE"
        echo "   Valid modes: direct, volume, azure_parquet, azure_json, azure_dual"
        exit 1
        ;;
esac

# Strip any existing mode suffixes from pipeline name and schema, then append current mode
# This ensures clean naming when reusing base configs across modes
for suffix in "_direct" "_volume" "_azure_parquet" "_azure_json" "_azure_dual"; do
    PIPELINE_NAME="${PIPELINE_NAME%$suffix}"
    SCHEMA="${SCHEMA%$suffix}"
done

# Append current mode suffix
MODE_SUFFIX="_${MODE}"
PIPELINE_NAME="${PIPELINE_NAME}${MODE_SUFFIX}"
SCHEMA="${SCHEMA}${MODE_SUFFIX}"

echo "✅ Configuration loaded:"
echo "   Pipeline: $PIPELINE_NAME"
echo "   Mode: $MODE"
echo "   Catalog: $CATALOG"
echo "   Schema: $SCHEMA"
echo ""

#######################################
# Get current user and workspace info
#######################################
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
WORKSPACE_PATH="/Workspace/Users/$USER_NAME/cockroachdb${MODE_SUFFIX}"

echo "👤 User: $USER_NAME"
echo "🌐 Workspace: $WORKSPACE_URL"
echo "📁 Workspace Path: $WORKSPACE_PATH"
echo ""

#######################################
# Mode-specific validation
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 1: Validate Prerequisites for Mode: $MODE"
echo "═══════════════════════════════════════════════════════════════"
echo ""

case "$MODE" in
    direct)
        echo "📡 DIRECT mode: Validating CockroachDB configuration..."
        
        CRDB_CONFIG="${config[crdb_config]}"
        TABLE_LIST="${config[table_list]:-usertable}"
        
        if [ -z "$CRDB_CONFIG" ]; then
            echo "❌ DIRECT mode requires 'crdb_config' in config"
            echo "   This should be a path to a JSON file with CockroachDB connection details"
            echo "   Required fields: cockroachdb_url, token, base_url"
            exit 1
        fi
        
        # Make relative paths relative to COCKROACH_DIR
        if [[ "$CRDB_CONFIG" != /* ]]; then
            CRDB_CONFIG="$COCKROACH_DIR/$CRDB_CONFIG"
        fi
        
        if [ ! -f "$CRDB_CONFIG" ]; then
            echo "❌ CockroachDB config file not found: $CRDB_CONFIG"
            exit 1
        fi
        
        # Validate config has required fields
        CRDB_URL=$(jq -r '.cockroachdb_url // .cockrodb_url // empty' "$CRDB_CONFIG")
        if [ -z "$CRDB_URL" ]; then
            echo "❌ CockroachDB config missing 'cockroachdb_url' field"
            exit 1
        fi
        
        echo "✅ CockroachDB config: $CRDB_CONFIG"
        echo "✅ CockroachDB URL: $(echo "$CRDB_URL" | sed 's/:[^:@]*@/:***@/')"  # Hide password
        echo "✅ Table list: $TABLE_LIST"
        ;;
        
    volume)
        echo "📂 VOLUME mode: Validating Unity Catalog Volume..."
        
        VOLUME_NAME="${config[volume_name]}"
        
        if [ -z "$VOLUME_NAME" ]; then
            echo "❌ VOLUME mode requires 'volume_name' in config"
            exit 1
        fi
        
        VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME_NAME}"
        
        # Validate volume structure using Python helper
        echo "🔍 Validating volume structure..."
        VALIDATION_JSON=$(python3 "$SCRIPTS_DIR/list_volume_files.py" --validate "${VOLUME_PATH}/" 2>&1)
        VALIDATION_EXIT=$?
        
        if [ $VALIDATION_EXIT -ne 0 ]; then
            echo "❌ Cannot access volume: ${VOLUME_PATH}"
            ERROR_MSG=$(echo "$VALIDATION_JSON" | jq -r '.error // "Unknown error"' 2>/dev/null || echo "Unknown error")
            echo "   Error: $ERROR_MSG"
            echo ""
            echo "🔧 Possible issues:"
            echo "   1. Volume doesn't exist - create it in Databricks first"
            echo "   2. No permissions to access volume"
            echo "   3. Run: databricks auth login"
            exit 1
        fi
        
        # Parse validation results
        HAS_TIMESTAMP_DIRS=$(echo "$VALIDATION_JSON" | jq -r '.has_timestamped_dirs')
        TIMESTAMP_DIR_COUNT=$(echo "$VALIDATION_JSON" | jq -r '.timestamp_dir_count')
        SAMPLE_TIMESTAMP=$(echo "$VALIDATION_JSON" | jq -r '.sample_timestamp_dir')
        SAMPLE_FILE_COUNT=$(echo "$VALIDATION_JSON" | jq -r '.sample_dir_file_count')
        ROOT_FILE_COUNT=$(echo "$VALIDATION_JSON" | jq -r '.root_file_count')
        HAS_SCHEMA=$(echo "$VALIDATION_JSON" | jq -r '.has_schema_file')
        
        # Check if we have data files (either in timestamped dirs or root)
        if [ "$HAS_TIMESTAMP_DIRS" = "true" ]; then
            if [ "$SAMPLE_FILE_COUNT" -eq 0 ]; then
                echo "❌ No data files found in volume directories"
                echo "   Found ${TIMESTAMP_DIR_COUNT} timestamped directory(ies) but they're empty"
                echo ""
                echo "🔧 Run sync script to populate volume:"
                echo "   python3 sync_azure_to_volume_compact.py --catalog ${CATALOG} --schema ${SCHEMA} --volume ${VOLUME_NAME}"
                exit 1
            fi
            
            echo "✅ Volume has ${TIMESTAMP_DIR_COUNT} timestamped directory(ies)"
            echo "   Sample: ${SAMPLE_TIMESTAMP}"
            echo "   Files in sample: ${SAMPLE_FILE_COUNT}"
        else
            if [ "$ROOT_FILE_COUNT" -eq 0 ]; then
                echo "❌ No data files found in volume"
                echo ""
                echo "🔧 Run sync script to populate volume:"
                echo "   python3 sync_azure_to_volume_compact.py --catalog ${CATALOG} --schema ${SCHEMA} --volume ${VOLUME_NAME}"
                exit 1
            fi
            
            echo "✅ Volume has ${ROOT_FILE_COUNT} data file(s) in root"
        fi
        
        # Check for _metadata/schema.json
        if [ "$HAS_SCHEMA" = "true" ]; then
            echo "✅ Volume has _metadata/schema.json"
        else
            echo "⚠️  _metadata/schema.json not found (will be inferred)"
        fi
        ;;
        
    azure_parquet|azure_json|azure_dual)
        echo "☁️  AZURE mode: Validating Azure configuration..."
        
        AZURE_CONFIG="${config[azure_config]}"
        BLOB_PREFIX="${config[blob_prefix]}"
        
        if [ -z "$AZURE_CONFIG" ] || [ -z "$BLOB_PREFIX" ]; then
            echo "❌ AZURE modes require 'azure_config' and 'blob_prefix' in config"
            exit 1
        fi
        
        # Make relative paths relative to COCKROACH_DIR
        if [[ "$AZURE_CONFIG" != /* ]]; then
            AZURE_CONFIG="$COCKROACH_DIR/$AZURE_CONFIG"
        fi
        
        if [ ! -f "$AZURE_CONFIG" ]; then
            echo "❌ Azure config file not found: $AZURE_CONFIG"
            exit 1
        fi
        
        echo "✅ Azure config: $AZURE_CONFIG"
        echo "✅ Blob prefix: $BLOB_PREFIX"
        ;;
esac

echo ""

#######################################
# Step 2: Check/Create Schema
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 2: Check/Create Schema"
echo "═══════════════════════════════════════════════════════════════"
echo ""

FULL_SCHEMA="${CATALOG}.${SCHEMA}"
echo "🔍 Checking schema: $FULL_SCHEMA"

if ! databricks schemas get "$FULL_SCHEMA" &>/dev/null; then
    echo "📦 Creating schema: $FULL_SCHEMA"
    databricks schemas create "${SCHEMA}" "${CATALOG}"
    echo "✅ Schema created"
else
    echo "✅ Schema exists"
fi

echo ""

#######################################
# Step 3: Copy Connector Code
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 3: Copy Connector Code to Workspace"
echo "═══════════════════════════════════════════════════════════════"
echo ""

if [ -f "$SCRIPTS_DIR/copy_connector_to_workspace.sh" ]; then
    echo "📤 Copying connector code..."
    "$SCRIPTS_DIR/copy_connector_to_workspace.sh" "$MODE_SUFFIX"
    echo "✅ Connector code copied"
else
    echo "⚠️  copy_connector_to_workspace.sh not found"
    echo "   Assuming connector code already in workspace"
fi

echo ""

#######################################
# Step 4: Check/Delete Existing Pipeline
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 4: Check Existing Pipeline"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Use server-side filter for fast lookup (avoids fetching all pipelines)
# Note: --filter returns array directly, not wrapped in {statuses: [...]}
EXISTING_PIPELINE=$(databricks pipelines list-pipelines \
  --filter "name LIKE '${PIPELINE_NAME}'" \
  --output json 2>/dev/null | \
  jq -r ".[]? | select(.name == \"${PIPELINE_NAME}\") | .pipeline_id" || echo "")

if [ -n "$EXISTING_PIPELINE" ]; then
    echo "⚠️  Pipeline '${PIPELINE_NAME}' already exists: ${EXISTING_PIPELINE}"
    
    if [ "$FORCE_DELETE" = true ]; then
        echo "🗑️  Force deleting existing pipeline..."
        databricks pipelines delete "$EXISTING_PIPELINE" 2>/dev/null || true
        sleep 3
        echo "✅ Pipeline deleted"
    else
        echo ""
        read -p "   Delete and recreate? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🗑️  Deleting old pipeline..."
            databricks pipelines delete "$EXISTING_PIPELINE" 2>/dev/null || true
            sleep 3
            echo "✅ Pipeline deleted"
        else
            echo "ℹ️  Keeping existing pipeline: ${EXISTING_PIPELINE}"
            echo ""
            
            # Ask if user wants to start the existing pipeline
            read -p "   Start existing pipeline? (y/N) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                # Use the existing pipeline ID for monitoring
                PIPELINE_ID="$EXISTING_PIPELINE"
                
                # Start and monitor
                if [ -f "$SCRIPTS_DIR/monitor_pipeline.sh" ]; then
                    echo "📈 Starting and monitoring pipeline (Ctrl+C to stop)..."
                    echo ""
                    "$SCRIPTS_DIR/monitor_pipeline.sh" "${PIPELINE_NAME}" "${CATALOG}" "${SCHEMA}" "${PIPELINE_ID}"
                else
                    # Fallback if monitor script not found
                    echo "🚀 Starting pipeline with full refresh..."
                    databricks pipelines start-update "${PIPELINE_ID}" --full-refresh
                    echo ""
                    echo "✅ Pipeline started!"
                fi
                
                echo ""
                echo "═══════════════════════════════════════════════════════════════"
                echo "✅ Pipeline Started!"
                echo "═══════════════════════════════════════════════════════════════"
                echo ""
                echo "📊 Pipeline: ${PIPELINE_NAME}"
                echo "🔗 Pipeline ID: ${PIPELINE_ID}"
                echo "📍 Target: ${CATALOG}.${SCHEMA}"
                echo ""
            else
                echo ""
                echo "ℹ️  Pipeline not started"
                echo ""
                echo "▶️  To start manually:"
                echo "   databricks pipelines start-update ${EXISTING_PIPELINE} --full-refresh"
                echo ""
                echo "📊 To monitor existing run:"
                echo "   ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA} ${EXISTING_PIPELINE} --no-start"
                echo ""
                echo "📊 To start and monitor:"
                echo "   ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA} ${EXISTING_PIPELINE}"
            fi
            
            exit 0
        fi
    fi
fi

echo ""

#######################################
# Step 5: Create Pipeline
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 5: Create DLT Pipeline"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Build pipeline configuration based on mode
case "$MODE" in
    direct)
        # DIRECT mode requires CockroachDB URL and credentials, not Databricks connection
        CRDB_CONFIG="${config[crdb_config]}"
        
        if [ -z "$CRDB_CONFIG" ]; then
            echo "❌ DIRECT mode requires 'crdb_config' in config (path to CockroachDB config JSON)"
            echo "   The config should contain: cockroachdb_url, token, base_url"
            exit 1
        fi
        
        # Make relative paths relative to COCKROACH_DIR
        if [[ "$CRDB_CONFIG" != /* ]]; then
            CRDB_CONFIG="$COCKROACH_DIR/$CRDB_CONFIG"
        fi
        
        if [ ! -f "$CRDB_CONFIG" ]; then
            echo "❌ CockroachDB config file not found: $CRDB_CONFIG"
            exit 1
        fi
        
        # Read CockroachDB connection details
        CRDB_URL=$(jq -r '.cockroachdb_url // .cockrodb_url' "$CRDB_CONFIG")
        CRDB_TOKEN=$(jq -r '.token // empty' "$CRDB_CONFIG")
        CRDB_BASE_URL=$(jq -r '.base_url // empty' "$CRDB_CONFIG")
        
        PIPELINE_CONFIG=$(jq -n \
            --arg source_name "cockroachdb" \
            --arg cockroachdb_url "$CRDB_URL" \
            --arg token "$CRDB_TOKEN" \
            --arg base_url "$CRDB_BASE_URL" \
            --arg table_list "$TABLE_LIST" \
            '{
                source_name: $source_name,
                cockroachdb_url: $cockroachdb_url,
                token: $token,
                base_url: $base_url,
                table_list: $table_list
            }')
        ;;
        
    volume)
        TABLE_LIST="${config[table_list]:-usertable}"
        PIPELINE_CONFIG=$(jq -n \
            --arg source_name "cockroachdb" \
            --arg volume_path "$VOLUME_PATH" \
            --arg table_list "$TABLE_LIST" \
            '{
                source_name: $source_name,
                volume_path: $volume_path,
                table_list: $table_list
            }')
        ;;
        
    azure_parquet|azure_json|azure_dual)
        # Get Azure config path (should already be set from Step 1, but handle if not)
        if [ -z "$AZURE_CONFIG" ]; then
            AZURE_CONFIG="${config[azure_config]}"
            # Make relative paths relative to COCKROACH_DIR
            if [[ "$AZURE_CONFIG" != /* ]]; then
                AZURE_CONFIG="$COCKROACH_DIR/$AZURE_CONFIG"
            fi
        fi
        
        # Read Azure config
        AZURE_ACCOUNT=$(jq -r '.account_name' "$AZURE_CONFIG")
        AZURE_CONTAINER=$(jq -r '.container_name' "$AZURE_CONFIG")
        AZURE_KEY=$(jq -r '.account_key' "$AZURE_CONFIG")
        
        PIPELINE_CONFIG=$(jq -n \
            --arg account_name "$AZURE_ACCOUNT" \
            --arg account_key "$AZURE_KEY" \
            --arg container_name "$AZURE_CONTAINER" \
            --arg blob_prefix "$BLOB_PREFIX" \
            --arg format "$MODE" \
            '{
                azure_account_name: $account_name,
                azure_account_key: $account_key,
                azure_container_name: $container_name,
                azure_blob_prefix: $blob_prefix,
                changefeed_format: $format
            }')
        ;;
esac

# Build full pipeline JSON
PIPELINE_JSON=$(jq -n \
    --arg name "$PIPELINE_NAME" \
    --arg catalog "$CATALOG" \
    --arg schema "$SCHEMA" \
    --arg ingest_path "${WORKSPACE_PATH}/ingest.py" \
    --argjson config "$PIPELINE_CONFIG" \
    '{
        name: $name,
        catalog: $catalog,
        target: $schema,
        channel: "PREVIEW",
        serverless: true,
        development: true,
        continuous: false,
        configuration: $config,
        libraries: [
            {
                file: {
                    path: $ingest_path
                }
            }
        ]
    }')

echo "🚀 Creating pipeline..."
echo "   Configuration keys: $(echo "$PIPELINE_CONFIG" | jq -r 'keys | join(", ")')"

# Create pipeline
RESPONSE=$(databricks pipelines create --json "$PIPELINE_JSON" 2>&1)

# Check if command succeeded
if echo "$RESPONSE" | grep -q "Error:"; then
    echo "❌ Failed to create pipeline:"
    echo "$RESPONSE"
    exit 1
fi

# Extract pipeline ID
PIPELINE_ID=$(echo "$RESPONSE" | jq -r '.pipeline_id // empty')

if [ -z "$PIPELINE_ID" ]; then
    echo "❌ Failed to extract pipeline ID from response:"
    echo "$RESPONSE"
    exit 1
fi

echo "✅ Pipeline created!"
echo "   Pipeline ID: $PIPELINE_ID"
echo "   View at: $WORKSPACE_URL/pipelines/$PIPELINE_ID"

# Save pipeline ID to mode-specific file
PIPELINE_ID_FILE="$COCKROACH_DIR/pipeline_id_${MODE}.txt"
echo "$PIPELINE_ID" > "$PIPELINE_ID_FILE"
echo "   Saved to: pipeline_id_${MODE}.txt"
echo ""

#######################################
# Step 6: Start Pipeline (Optional)
#######################################
if [ "$NO_START" = false ]; then
    echo "═══════════════════════════════════════════════════════════════"
    echo "Step 6: Start Pipeline"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    
    read -p "Start pipeline now? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Monitor script will start the pipeline, just pass the ID
        if [ -f "$SCRIPTS_DIR/monitor_pipeline.sh" ]; then
            echo "📈 Starting and monitoring pipeline (Ctrl+C to stop)..."
            echo ""
            "$SCRIPTS_DIR/monitor_pipeline.sh" "${PIPELINE_NAME}" "${CATALOG}" "${SCHEMA}" "${PIPELINE_ID}"
        else
            # Fallback if monitor script not found
            echo "🚀 Starting pipeline with full refresh..."
            databricks pipelines start-update "${PIPELINE_ID}" --full-refresh
            echo ""
            echo "✅ Pipeline started!"
        fi
    else
        echo "ℹ️  Pipeline created but not started"
    fi
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "✅ Deployment Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📊 Pipeline: ${PIPELINE_NAME}"
echo "🔗 Pipeline ID: ${PIPELINE_ID}"
echo "📍 Target: ${CATALOG}.${SCHEMA}"
echo "🎯 Mode: ${MODE}"
echo ""
echo "🔧 Next Steps:"
echo ""
echo "   ▶️  Start pipeline:"
echo "      databricks pipelines start-update ${PIPELINE_ID} --full-refresh"
echo ""
echo "   📈 Monitor pipeline:"
echo "      ./monitor_pipeline.sh ${PIPELINE_NAME} ${CATALOG} ${SCHEMA} ${PIPELINE_ID}"
echo ""
echo "   📊 Query results:"
case "$MODE" in
    direct)
        echo "      SELECT * FROM ${CATALOG}.${SCHEMA}.$(echo $TABLE_LIST | cut -d',' -f1)"
        ;;
    *)
        echo "      SELECT * FROM ${CATALOG}.${SCHEMA}.<table_name>"
        ;;
esac
echo ""
