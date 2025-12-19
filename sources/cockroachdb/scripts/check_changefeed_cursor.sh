#!/bin/bash
# Check if there should be changes since a given cursor
# Usage: ./check_changefeed_cursor.sh <cursor_timestamp>

export COCKROACHDB_URL="postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

CURSOR="${1}"

if [ -z "$CURSOR" ]; then
    echo "Usage: $0 <cursor_timestamp>"
    echo ""
    echo "Example:"
    echo "  $0 '1766105060072019839.0000000000'"
    echo ""
    echo "This script checks if there should be any changes since the given cursor"
    exit 1
fi

echo "================================================================================"
echo "Checking for Changes Since Cursor"
echo "================================================================================"
echo ""
echo "Cursor: $CURSOR"
echo ""

# Get current timestamp
echo "1. Getting current CockroachDB timestamp..."
CURRENT_TS=$(psql "$COCKROACHDB_URL" -t -A -c "SELECT cluster_logical_timestamp()::string;")
CURRENT_TIME=$(psql "$COCKROACHDB_URL" -t -A -c "SELECT now()::string;")
echo "   Current timestamp: $CURRENT_TS"
echo "   Current wall time: $CURRENT_TIME"
echo ""

# Extract numeric parts for comparison
CURSOR_NUM=$(echo "$CURSOR" | cut -d'.' -f1)
CURRENT_NUM=$(echo "$CURRENT_TS" | cut -d'.' -f1)

# Calculate age
AGE_NS=$((CURRENT_NUM - CURSOR_NUM))
AGE_SEC=$((AGE_NS / 1000000000))

echo "2. Cursor age:"
echo "   Cursor timestamp: $CURSOR"
echo "   Age: $AGE_SEC seconds (~$((AGE_SEC / 60)) minutes)"
echo ""

# Test if changefeed would return data
echo "3. Testing changefeed with cursor..."
echo "   Running: EXPERIMENTAL CHANGEFEED FOR usertable"
echo "            WITH initial_scan='no', updated, resolved='1s',"
echo "                 split_column_families, cursor='$CURSOR'"
echo ""
echo "   Timing (will timeout in 10s if no changes)..."
echo ""

START_TIME=$(date +%s)

# Run changefeed with timeout
timeout 10s psql "$COCKROACHDB_URL" -t << SQL 2>&1 | head -10
EXPERIMENTAL CHANGEFEED FOR usertable 
WITH 
    initial_scan='no', 
    updated, 
    resolved='1s', 
    split_column_families,
    cursor='$CURSOR';
SQL

EXIT_CODE=$?
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "================================================================================"
echo "Results:"
echo "================================================================================"
echo "  Time elapsed: ${ELAPSED}s"
echo "  Exit code: $EXIT_CODE"
echo ""

if [ $EXIT_CODE -eq 124 ]; then
    echo "❌ TIMEOUT: No changes detected (changefeed hung)"
    echo "   This is expected when there are no changes since cursor"
    echo ""
    echo "Interpretation:"
    echo "  - Pipeline will timeout after 5s (incremental mode)"
    echo "  - This is normal when database is idle"
    echo "  - No action needed"
elif [ $ELAPSED -lt 3 ]; then
    echo "✅ FAST RESPONSE: Likely received resolved timestamp"
    echo "   This means CockroachDB is responsive but no changes exist"
else
    echo "⚠️  CHANGES DETECTED: Changefeed returned data"
    echo "   This means there ARE changes since the cursor"
    echo ""
    echo "Debugging steps:"
    echo "  1. Check pipeline logs - did it process these changes?"
    echo "  2. Check cursor advancement - did end_offset match this cursor?"
    echo "  3. Run pipeline again to pick up missed changes"
fi

echo ""
echo "================================================================================"
echo "Manual Verification:"
echo "================================================================================"
echo ""
echo "To check for changes in a specific time range:"
echo ""
echo "  # Check total row count"
echo "  psql \$COCKROACHDB_URL -c 'SELECT count(*) FROM usertable;'"
echo ""
echo "  # Check recent changes (requires updated_at column)"
echo "  # Note: usertable may not have timestamps, use changefeeds instead"
echo ""
echo "  # Generate test changes:"
echo "  cockroach workload run ycsb \$COCKROACHDB_URL --duration 30s"
echo ""

