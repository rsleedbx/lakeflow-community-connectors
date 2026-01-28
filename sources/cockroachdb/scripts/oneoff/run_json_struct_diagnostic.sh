#!/bin/bash
################################################################################
# Run JSON Struct Diagnostic on Test Files
#
# This script finds JSON CDC files in the volume and runs diagnostic analysis
# to understand the structure of 'before' and 'after' fields.
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Default test scenario
SCENARIO="${1:-test-json_usertable_no_split}"
TIMESTAMP="${2:-1767823340}"

# Parse scenario to get format, catalog, schema
FORMAT="json"
CATALOG="defaultdb"
SCHEMA="public"

echo "================================================================================"
echo "JSON STRUCT DIAGNOSTIC"
echo "================================================================================"
echo "Scenario: $SCENARIO"
echo "Timestamp: $TIMESTAMP"
echo ""

# Construct volume path
VOLUME_BASE="/Volumes/main/robert_lee_cockroachdb/parquet_files"
VOLUME_PATH="$VOLUME_BASE/$FORMAT/$CATALOG/$SCHEMA/$SCENARIO/$TIMESTAMP"

echo "📁 Volume path: $VOLUME_PATH"
echo ""

# Check if path exists
if [ ! -d "$VOLUME_PATH" ]; then
    echo "❌ Error: Volume path does not exist: $VOLUME_PATH"
    echo ""
    echo "Available scenarios:"
    ls -1 "$VOLUME_BASE/$FORMAT/$CATALOG/$SCHEMA/" 2>/dev/null | grep "^test-" || echo "  (none found)"
    exit 1
fi

# Find JSON files (prefer CDC file with sequence 00000001)
echo "🔍 Looking for JSON files..."
CDC_FILE=$(find "$VOLUME_PATH" -name "*.ndjson" -o -name "*.json" | grep "00000001" | head -1)

if [ -z "$CDC_FILE" ]; then
    # Fallback: any JSON file
    CDC_FILE=$(find "$VOLUME_PATH" -name "*.ndjson" -o -name "*.json" | head -1)
fi

if [ -z "$CDC_FILE" ]; then
    echo "❌ Error: No JSON files found in $VOLUME_PATH"
    exit 1
fi

echo "✅ Found file: $(basename "$CDC_FILE")"
echo ""

# Run diagnostic
echo "================================================================================"
echo "RUNNING DIAGNOSTIC"
echo "================================================================================"
echo ""

cd "$SCRIPT_DIR"
python3 diagnose_json_struct.py "$CDC_FILE"

echo ""
echo "================================================================================"
echo "DIAGNOSTIC COMPLETE"
echo "================================================================================"
echo ""
echo "💡 Key Questions to Answer:"
echo "   1. Are DELETE events 'after' field null or {}?"
echo "   2. Are SNAPSHOT events 'before' field null or {}?"
echo "   3. Does json.dumps(None) produce 'null' string?"
echo "   4. Does json.dumps({}) produce '{}' string?"
echo ""
echo "📋 Compare with Spark behavior:"
echo "   - F.to_json(null_struct) → what string?"
echo "   - F.to_json(empty_struct) → what string?"
echo "   - Our fix logic should match Python behavior"
echo ""

