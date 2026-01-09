#!/usr/bin/env bash
# Test changefeed timeout behavior with cursor (no changes)

export COCKROACHDB_URL="postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

echo "================================================================================"
echo "Testing CockroachDB Changefeed Timeout Behavior"
echo "================================================================================"
echo ""

# Get current timestamp for cursor
echo "1. Getting current cluster timestamp..."
CURSOR=$(psql "$COCKROACHDB_URL" -t -A -c "SELECT cluster_logical_timestamp()::string;")
echo "   Cursor: $CURSOR"
echo ""

# Test changefeed with cursor (no changes since cursor)
echo "2. Testing changefeed with cursor (expecting resolved timestamp within 1-2s)..."
echo "   Query: EXPERIMENTAL CHANGEFEED FOR usertable"
echo "          WITH initial_scan='no', updated, resolved='1s', split_column_families, cursor='$CURSOR'"
echo ""
echo "   Expected: Resolved timestamp within 1-2 seconds (no data events)"
echo "   Timing..."
echo ""

START_TIME=$(date +%s)

timeout 10s psql "$COCKROACHDB_URL" -t << SQL 2>&1 | head -20
EXPERIMENTAL CHANGEFEED FOR usertable 
WITH 
    initial_scan='no', 
    updated, 
    resolved='1s', 
    split_column_families,
    cursor='$CURSOR';
SQL

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "================================================================================"
echo "Results:"
echo "================================================================================"
echo "  Time elapsed: ${ELAPSED}s"
echo ""

if [ $ELAPSED -lt 3 ]; then
    echo "✅ GOOD: Changefeed responded quickly (< 3s)"
    echo "   This means resolved timestamps are being sent properly"
else
    echo "❌ SLOW: Changefeed took $ELAPSED seconds"
    echo "   This suggests:"
    echo "   - Resolved timestamps not being sent"
    echo "   - Network latency"
    echo "   - CockroachDB server issue"
fi

echo ""
echo "================================================================================"
echo "Diagnosis:"
echo "================================================================================"
echo ""
echo "If changefeed is slow (>3s), possible causes:"
echo ""
echo "1. Check resolved_interval setting (should be small, like '1s')"
echo "2. Check if cursor is too old (CockroachDB might need to scan)"
echo "3. Check CockroachDB server load"
echo "4. Check network latency to CockroachDB"
echo ""
echo "If fast (<3s), the issue is in our connector code:"
echo "- Socket timeout interfering"
echo "- Statement timeout too aggressive"
echo "- Connection pooling issue"
echo ""




