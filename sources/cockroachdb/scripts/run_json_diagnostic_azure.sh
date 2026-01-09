#!/bin/bash
################################################################################
# Run JSON Struct Diagnostic on Azure Blob Files
#
# This script analyzes JSON CDC files stored in Azure blob storage to understand
# the structure of 'before' and 'after' fields.
#
# Prerequisites:
#   - Azure storage credentials set in environment
#   - Python azure-storage-blob package installed
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source Azure configuration from 01_azure_storage.sh
if [ -f "$SCRIPT_DIR/01_azure_storage.sh" ]; then
    source "$SCRIPT_DIR/01_azure_storage.sh"
else
    echo "❌ Error: 01_azure_storage.sh not found"
    echo "   This file contains Azure storage credentials"
    exit 1
fi

# Default values
SCENARIO="${1:-test-json_usertable_no_split}"
TIMESTAMP="${2:-1767823340}"
FORMAT="json"
CATALOG="defaultdb"
SCHEMA="public"

echo "================================================================================"
echo "JSON STRUCT DIAGNOSTIC (Azure Blob Storage)"
echo "================================================================================"
echo "Scenario: $SCENARIO"
echo "Timestamp: $TIMESTAMP"
echo "Storage Account: $AZURE_STORAGE_ACCOUNT"
echo "Container: $AZURE_CONTAINER_NAME"
echo ""

# Construct blob path
BLOB_PREFIX="$FORMAT/$CATALOG/$SCHEMA/$SCENARIO/$TIMESTAMP"

echo "📁 Blob prefix: $BLOB_PREFIX"
echo ""

# Check if Azure credentials are set
if [ -z "${AZURE_STORAGE_KEY:-}" ]; then
    echo "❌ Error: AZURE_STORAGE_KEY not set"
    echo "   Please set Azure storage credentials in 01_azure_storage.sh"
    exit 1
fi

# Find a CDC file (prefer sequence 00000001)
echo "🔍 Listing blobs in Azure..."
BLOB_FILE=$(az storage blob list \
    --account-name "$AZURE_STORAGE_ACCOUNT" \
    --account-key "$AZURE_STORAGE_KEY" \
    --container-name "$AZURE_CONTAINER_NAME" \
    --prefix "$BLOB_PREFIX" \
    --query "[?contains(name, '00000001')].name" \
    --output tsv \
    | head -1)

if [ -z "$BLOB_FILE" ]; then
    # Fallback: any JSON file
    BLOB_FILE=$(az storage blob list \
        --account-name "$AZURE_STORAGE_ACCOUNT" \
        --account-key "$AZURE_STORAGE_KEY" \
        --container-name "$AZURE_CONTAINER_NAME" \
        --prefix "$BLOB_PREFIX" \
        --query "[?ends_with(name, '.ndjson') || ends_with(name, '.json')].name" \
        --output tsv \
        | head -1)
fi

if [ -z "$BLOB_FILE" ]; then
    echo "❌ Error: No JSON files found in blob prefix: $BLOB_PREFIX"
    exit 1
fi

echo "✅ Found blob: $BLOB_FILE"
echo ""

# Construct wasbs:// URL
WASBS_URL="wasbs://$AZURE_CONTAINER_NAME@$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$BLOB_FILE"

echo "📍 Blob URL: $WASBS_URL"
echo ""

# Run diagnostic
echo "================================================================================"
echo "RUNNING DIAGNOSTIC"
echo "================================================================================"
echo ""

export AZURE_STORAGE_KEY
cd "$SCRIPT_DIR"
python3 diagnose_json_struct.py "$WASBS_URL"

echo ""
echo "================================================================================"
echo "DIAGNOSTIC COMPLETE"
echo "================================================================================"

