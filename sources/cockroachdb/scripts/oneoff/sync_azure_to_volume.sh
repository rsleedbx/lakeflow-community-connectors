#!/usr/bin/env bash
set -e

#######################################
# Sync Azure Blob Storage to Databricks Volume
#
# This script:
# 1. Downloads Parquet files from Azure to local temp directory
# 2. Uploads them to Databricks Volume using databricks fs cp
# 3. No Unity Catalog External Location needed!
#
# Credentials: Loaded from .env/cockroachdb_cdc_azure.json
#######################################

echo "═══════════════════════════════════════════════════════════════"
echo "Azure Blob Storage → Databricks Volume Sync"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Find git root
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

COCKROACH_DIR="$GIT_ROOT/sources/cockroachdb"
AZURE_JSON="$COCKROACH_DIR/.env/cockroachdb_cdc_azure.json"
PIPELINE_JSON="$COCKROACH_DIR/.env/cockroachdb_pipelines.json"

# Verify credential files exist
if [ ! -f "$AZURE_JSON" ]; then
    echo "❌ Missing $AZURE_JSON"
    echo "   Run: sources/cockroachdb/scripts/01_azure_storage.sh"
    exit 1
fi

if [ ! -f "$PIPELINE_JSON" ]; then
    echo "❌ Missing $PIPELINE_JSON"
    echo "   This file should define catalog, schema, volume_name, blob_prefix"
    exit 1
fi

# Check if yq is available
if ! command -v yq &> /dev/null; then
    echo "❌ Error: yq is required to parse JSON credentials"
    echo "   Install with: brew install yq (macOS) or snap install yq (Linux)"
    exit 1
fi

# Load Azure credentials into associative array
declare -A azure_creds
while IFS='=' read -r key value; do
    value="${value%\'}"
    value="${value#\'}"
    azure_creds["$key"]="$value"
done < <(yq -o=shell "$AZURE_JSON")

# Load pipeline configuration into associative array
declare -A pipeline_config
while IFS='=' read -r key value; do
    value="${value%\'}"
    value="${value#\'}"
    pipeline_config["$key"]="$value"
done < <(yq -o=shell "$PIPELINE_JSON")

# Verify required credentials
if [ -z "${azure_creds[azure_storage_account]}" ]; then
    echo "❌ azure_storage_account not set in $AZURE_JSON"
    exit 1
fi

if [ -z "${azure_creds[azure_storage_key]}" ]; then
    echo "❌ azure_storage_key not set in $AZURE_JSON"
    exit 1
fi

# Verify required pipeline configuration
if [ -z "${pipeline_config[catalog]}" ]; then
    echo "❌ catalog not set in $PIPELINE_JSON"
    exit 1
fi

if [ -z "${pipeline_config[schema]}" ]; then
    echo "❌ schema not set in $PIPELINE_JSON"
    exit 1
fi

# Local temp directory (with unique PID suffix)
TEMP_DIR="${pipeline_config[temp_dir_prefix]}_$$"
mkdir -p "$TEMP_DIR"

echo "📋 Configuration:"
echo "  Azure Credentials: $AZURE_JSON"
echo "  Pipeline Config: $PIPELINE_JSON"
echo "  Azure Account: ${azure_creds[azure_storage_account]}"
echo "  Azure Storage Key: ${azure_creds[azure_storage_key]:0:10}... (loaded from JSON)"
echo "  Container: ${azure_creds[azure_storage_container]}"
echo "  Blob Prefix: ${pipeline_config[blob_prefix]}"
echo "  Catalog: ${pipeline_config[catalog]}"
echo "  Schema: ${pipeline_config[schema]}"
echo "  Volume: ${pipeline_config[volume_name]}"
echo "  Volume Path: dbfs:/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}"
echo "  Local Temp: ${TEMP_DIR}"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "🧹 Cleaning up temp directory..."
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

#######################################
# Step 1: Create Schema and Volume via Databricks CLI
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 1: Create Schema and Volume"
echo "═══════════════════════════════════════════════════════════════"

# Check if databricks CLI is available
if ! command -v databricks &> /dev/null; then
    echo "❌ databricks CLI not found. Please install it first."
    exit 1
fi

echo "Creating schema: ${pipeline_config[catalog]}.${pipeline_config[schema]}"
# Create schema via Databricks CLI
cat > /tmp/create_schema.json <<EOF
{
  "name": "${pipeline_config[schema]}",
  "catalog_name": "${pipeline_config[catalog]}",
  "comment": "CockroachDB CDC data"
}
EOF

databricks api post /api/2.1/unity-catalog/schemas --json @/tmp/create_schema.json 2>&1 | grep -q "ALREADY_EXISTS" && echo "   (Schema already exists)" || echo "   (Schema created or already exists)"
rm -f /tmp/create_schema.json

echo "✅ Schema ready: ${pipeline_config[catalog]}.${pipeline_config[schema]}"

echo ""
echo "Creating volume: ${pipeline_config[catalog]}.${pipeline_config[schema]}.${pipeline_config[volume_name]}"
# Create volume via Databricks CLI
cat > /tmp/create_volume.json <<EOF
{
  "catalog_name": "${pipeline_config[catalog]}",
  "schema_name": "${pipeline_config[schema]}",
  "name": "${pipeline_config[volume_name]}",
  "volume_type": "${pipeline_config[volume_type]}",
  "comment": "CockroachDB CDC Parquet files synced from Azure"
}
EOF

databricks api post /api/2.1/unity-catalog/volumes --json @/tmp/create_volume.json 2>&1 | grep -q "ALREADY_EXISTS" && echo "   (Volume already exists)" || echo "   (Volume created or already exists)"
rm -f /tmp/create_volume.json

echo "✅ Volume ready: ${pipeline_config[catalog]}.${pipeline_config[schema]}.${pipeline_config[volume_name]}"
echo "   Path: dbfs:/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}"
echo ""

#######################################
# Step 2: List Files in Azure
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 2: List Files in Azure Blob Storage"
echo "═══════════════════════════════════════════════════════════════"

echo "📂 Listing files in Azure..."
echo ""

# Use Azure CLI to list blobs
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI (az) not found. Please install it:"
    echo "   brew install azure-cli"
    exit 1
fi

# List blobs and filter for Parquet files
AZURE_FILES=$(az storage blob list \
  --account-name "${azure_creds[azure_storage_account]}" \
  --account-key "${azure_creds[azure_storage_key]}" \
  --container-name "${azure_creds[azure_storage_container]}" \
  --prefix "${pipeline_config[blob_prefix]}/" \
  --query "[?ends_with(name, '.parquet')].name" \
  --output tsv)

if [ -z "$AZURE_FILES" ]; then
    echo "❌ No Parquet files found in Azure"
    echo "   Path: ${azure_creds[azure_storage_account]}/${azure_creds[azure_storage_container]}/${pipeline_config[blob_prefix]}/"
    exit 1
fi

FILE_COUNT=$(echo "$AZURE_FILES" | wc -l | xargs)
echo "✅ Found ${FILE_COUNT} Parquet files in Azure:"
echo ""
echo "$AZURE_FILES" | head -10 | while read -r blob_name; do
    filename=$(basename "$blob_name")
    echo "  • ${filename}"
done

if [ "$FILE_COUNT" -gt 10 ]; then
    echo "  ... and $((FILE_COUNT - 10)) more files"
fi

echo ""

#######################################
# Step 3: List Existing Files in Volume
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 3: Check Existing Files in Volume"
echo "═══════════════════════════════════════════════════════════════"

echo "📂 Checking volume: /Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}"
echo ""

# List files in Volume
# Note: 'databricks fs ls' does NOT work with Unity Catalog Volumes - use SDK instead
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXISTING_FILES=$(python3 "$SCRIPTS_DIR/list_volume_files.py" --pattern "*.parquet" "/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}/" 2>/dev/null | awk -F'/' '{print $NF}' || echo "")

if [ -z "$EXISTING_FILES" ]; then
    echo "  (Volume is empty or doesn't exist yet)"
    EXISTING_COUNT=0
else
    EXISTING_COUNT=$(echo "$EXISTING_FILES" | wc -l | xargs)
    echo "📊 Found ${EXISTING_COUNT} existing files in Volume"
fi

echo ""

#######################################
# Step 4: Download and Upload Files
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 4: Copy Files from Azure to Volume"
echo "═══════════════════════════════════════════════════════════════"
echo ""

COPIED=0
SKIPPED=0
FAILED=0

while IFS= read -r blob_name; do
    filename=$(basename "$blob_name")
    
    # Check if file already exists in Volume
    if echo "$EXISTING_FILES" | grep -q "$filename"; then
        echo "⏭️  Skip: ${filename} (already exists)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi
    
    echo "📥 Processing: ${filename}"
    
    # Download from Azure to temp directory
    local_file="${TEMP_DIR}/${filename}"
    
    if az storage blob download \
        --account-name "${azure_creds[azure_storage_account]}" \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name "${azure_creds[azure_storage_container]}" \
        --name "$blob_name" \
        --file "$local_file" \
        --output none; then
        
        echo "   ✓ Downloaded to local temp"
        
        # Upload to Databricks Volume
        volume_file="dbfs:/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}/${filename}"
        
        if databricks fs cp "$local_file" "$volume_file"; then
            echo "   ✓ Uploaded to Volume"
            COPIED=$((COPIED + 1))
            
            # Clean up local file
            rm "$local_file"
        else
            echo "   ✗ Failed to upload to Volume"
            FAILED=$((FAILED + 1))
        fi
    else
        echo "   ✗ Failed to download from Azure"
        FAILED=$((FAILED + 1))
    fi
    
    echo ""
    
done <<< "$AZURE_FILES"

echo "═══════════════════════════════════════════════════════════════"
echo "📊 Sync Summary"
echo "═══════════════════════════════════════════════════════════════"
echo "  Copied:  ${COPIED} files"
echo "  Skipped: ${SKIPPED} files (already exist)"
echo "  Failed:  ${FAILED} files"
echo "  Total in Volume: $((EXISTING_COUNT + COPIED)) files"
echo ""

if [ "$FAILED" -gt 0 ]; then
    echo "⚠️  Some files failed to copy"
    exit 1
fi

#######################################
# Step 5: Verify Files in Volume
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "Step 5: Verify Files in Volume"
echo "═══════════════════════════════════════════════════════════════"
echo ""

echo "📂 Listing files in Volume:"
# Note: 'databricks fs ls' does NOT work with Unity Catalog Volumes - use SDK instead
python3 "$SCRIPTS_DIR/list_volume_files.py" --pattern "*.parquet" "/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}/" 2>/dev/null | head -10

FINAL_COUNT=$(python3 "$SCRIPTS_DIR/list_volume_files.py" --count --pattern "*.parquet" "/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}/" 2>/dev/null || echo "0")
echo ""
echo "✅ Volume contains ${FINAL_COUNT} Parquet files"
echo ""

#######################################
# Success!
#######################################
echo "═══════════════════════════════════════════════════════════════"
echo "✅ Sync Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📍 Volume: ${pipeline_config[catalog]}.${pipeline_config[schema]}.${pipeline_config[volume_name]}"
echo "📂 Path: dbfs:/Volumes/${pipeline_config[catalog]}/${pipeline_config[schema]}/${pipeline_config[volume_name]}"
echo "📊 Files Ready: ${FINAL_COUNT} Parquet files"
echo ""
echo "🎯 Next Steps:"
echo "   1. Deploy DLT pipeline that reads from Volume"
echo "   2. Run: ./setup_volume_pipeline.sh"
echo ""
echo "💡 To sync more files later, just run this script again!"
echo ""
echo "📝 Customization:"
echo "   Edit configuration: .env/cockroachdb_pipelines.json"
echo "   - catalog, schema, volume_name, blob_prefix, volume_type"
echo ""

