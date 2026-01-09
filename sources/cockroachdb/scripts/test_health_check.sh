#!/bin/bash

# Quick diagnostic script to test health check
set -x  # Enable debug mode

JOB_ID="1139285157549768705"
CRDB_JSON="../.env/cockroachdb_credentials.json"
SCRIPTS_DIR="."

echo "Testing health check command..."
echo ""

# Run the command with timeout
if command -v timeout >/dev/null 2>&1; then
    echo "Using 'timeout' command"
    health_output=$(timeout 10 python3 "$SCRIPTS_DIR/changefeed_helper.py" check-status \
        --job-id "$JOB_ID" \
        --json "$CRDB_JSON" 2>&1)
    health_exit_code=$?
else
    echo "No timeout command available"
    health_output=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" check-status \
        --job-id "$JOB_ID" \
        --json "$CRDB_JSON" 2>&1)
    health_exit_code=$?
fi

echo "Exit code: $health_exit_code"
echo ""
echo "Raw output:"
echo "$health_output"
echo ""

echo "Parsing status..."
job_status=$(echo "$health_output" | grep -E "^\s*Status:" | sed 's/^[[:space:]]*Status:[[:space:]]*//')
echo "Parsed status: '$job_status'"
echo ""

echo "Parsing error..."
error_msg=$(echo "$health_output" | grep -E "^\s*Error:" | sed 's/^[[:space:]]*Error:[[:space:]]*//')
echo "Parsed error: '$error_msg'"
echo ""

echo "Parsing category..."
error_category=$(echo "$health_output" | grep -E "^\s*Category:" | sed 's/^[[:space:]]*Category:[[:space:]]*//')
echo "Parsed category: '$error_category'"
echo ""

echo "Parsing suggestion..."
error_suggestion=$(echo "$health_output" | grep -E "^\s*💡 Suggestion:" | sed 's/^[[:space:]]*💡 Suggestion:[[:space:]]*//')
echo "Parsed suggestion: '$error_suggestion'"
echo ""

echo "Done!"


