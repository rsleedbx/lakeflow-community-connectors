#!/usr/bin/env bash
#
# Load credentials from JSON file into bash associative array using yq
# Based on user's Untitled-1 example
#
# Usage:
#   source load_credentials.sh path/to/credentials.json
#   echo "Catalog: ${credentials[catalog]}"
#   echo "Schema: ${credentials[schema]}"
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if yq is installed
if ! command -v yq &> /dev/null; then
    echo "❌ ERROR: yq is not installed"
    echo "   Install with: brew install yq (macOS) or snap install yq (Linux)"
    exit 1
fi

# Get credentials file path
CREDENTIALS_FILE="${1:-}"

if [ -z "$CREDENTIALS_FILE" ]; then
    # Try default locations
    DEFAULT_LOCATIONS=(
        "$SCRIPT_DIR/../examples/credentials_parquet.json"
        "$SCRIPT_DIR/../examples/credentials_both.json"
        "$SCRIPT_DIR/../examples/credentials_json.json"
        "$SCRIPT_DIR/../.env/credentials.json"
    )
    
    for location in "${DEFAULT_LOCATIONS[@]}"; do
        if [ -f "$location" ]; then
            CREDENTIALS_FILE="$location"
            break
        fi
    done
    
    if [ -z "$CREDENTIALS_FILE" ]; then
        echo "❌ ERROR: No credentials file found"
        echo "   Checked locations:"
        for location in "${DEFAULT_LOCATIONS[@]}"; do
            echo "     - $location"
        done
        echo ""
        echo "   Usage: source $0 path/to/credentials.json"
        return 1 2>/dev/null || exit 1
    fi
fi

if [ ! -f "$CREDENTIALS_FILE" ]; then
    echo "❌ ERROR: Credentials file not found: $CREDENTIALS_FILE"
    return 1 2>/dev/null || exit 1
fi

# Declare associative array for credentials
declare -gA credentials

# Use yq to output shell assignments and read them into the array
# This follows the user's Untitled-1 example pattern
while IFS='=' read -r key value; do
    # Remove surrounding single quotes from the value provided by -o=shell
    value="${value%\'}"
    value="${value#\'}"
    credentials["$key"]="$value"
done < <(yq -o=shell "$CREDENTIALS_FILE")

echo "✅ Loaded credentials from: $(basename "$CREDENTIALS_FILE")"
echo "   📁 Catalog: ${credentials[catalog]:-N/A}"
echo "   📂 Schema: ${credentials[schema]:-N/A}"
echo "   📊 Format: ${credentials[format]:-parquet}"

# Export commonly used variables for convenience
export CATALOG="${credentials[catalog]:-}"
export SCHEMA="${credentials[schema]:-}"
export FORMAT="${credentials[format]:-parquet}"
export AZURE_STORAGE_ACCOUNT="${credentials[azure_storage_account]:-}"
export AZURE_STORAGE_KEY="${credentials[azure_storage_key]:-}"
export AZURE_STORAGE_CONTAINER="${credentials[azure_storage_container]:-changefeed-events}"
export COCKROACHDB_TOKEN="${credentials[token]:-}"
export COCKROACHDB_BASE_URL="${credentials[base_url]:-}"

# Construct auto-generated paths
export AUTO_PATH_PREFIX="${FORMAT}/${CATALOG}/${SCHEMA}"

echo "   🔗 Auto path: ${AUTO_PATH_PREFIX}"



