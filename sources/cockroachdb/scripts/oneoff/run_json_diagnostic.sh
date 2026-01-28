#!/bin/bash
#
# Wrapper script to run JSON double-count diagnostic with proper credential loading
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Try multiple possible locations for credentials
CREDS_FILE=""
for possible_location in \
    "$SCRIPT_DIR/../.env/cockroachdb_cdc_azure.json" \
    "$REPO_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.json" \
    "$REPO_ROOT/sources/cockroachdb/cockroachdb_cdc_azure.json" \
    "$REPO_ROOT/.env/cockroachdb_cdc_azure.json" \
    "$SCRIPT_DIR/../cockroachdb_cdc_azure.json"; do
    if [ -f "$possible_location" ]; then
        CREDS_FILE="$possible_location"
        break
    fi
done

echo "═══════════════════════════════════════════════════════════════════════"
echo " JSON Changefeed Duplicate Event Diagnostic"
echo "═══════════════════════════════════════════════════════════════════════"
echo

# Check if credentials file exists
if [ -z "$CREDS_FILE" ] || [ ! -f "$CREDS_FILE" ]; then
    echo "❌ ERROR: Credentials file not found"
    echo
    echo "Tried these locations:"
    echo "  • $SCRIPT_DIR/../.env/cockroachdb_cdc_azure.json"
    echo "  • $REPO_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.json"
    echo "  • $REPO_ROOT/sources/cockroachdb/cockroachdb_cdc_azure.json"
    echo "  • $REPO_ROOT/.env/cockroachdb_cdc_azure.json"
    echo
    echo "Please create one of these files with your Azure credentials:"
    echo "{"
    echo "  \"azure_storage_account\": \"your_account_name\","
    echo "  \"azure_storage_key\": \"your_account_key=\","
    echo "  \"azure_storage_container\": \"changefeed-events\""
    echo "}"
    exit 1
fi

echo "✅ Found credentials file: $CREDS_FILE"
echo

# Load Azure credentials
echo "Loading Azure credentials..."
while IFS='=' read -r key value; do
    export "$key=$value"
done < <(jq -r 'to_entries | map(select(.key | startswith("_") | not)) | map("azure_\(.key)=\(.value|tostring)") | .[]' "$CREDS_FILE")

# Verify credentials loaded
if [ -z "${azure_azure_storage_account:-}" ]; then
    echo "❌ ERROR: Failed to load azure_storage_account from credentials"
    exit 1
fi

echo "✅ Loaded account: $azure_azure_storage_account"
echo

# Get test scenario to diagnose (default: json_usertable_no_split)
TEST_SCENARIO="${1:-json_usertable_no_split}"
PATH_PREFIX="json/defaultdb/public/test-${TEST_SCENARIO}"
SAMPLE_SIZE="${2:-50}"

echo "Diagnostic parameters:"
echo "  Container: changefeed-events"
echo "  Path prefix: $PATH_PREFIX"
echo "  Sample size: $SAMPLE_SIZE events"
echo

# Run diagnostic
python3 "$SCRIPT_DIR/diagnose_json_double_count.py" \
    "$azure_azure_storage_account" \
    "$azure_azure_storage_key" \
    "changefeed-events" \
    "$PATH_PREFIX" \
    "$SAMPLE_SIZE"

