#!/usr/bin/env bash
# Verify changes exist since cursor

set -e

CURSOR="1766105060072019839.0000000000"

echo "========================================="
echo "🔍 Verifying Changes Since Cursor"
echo "========================================="
echo ""
echo "Cursor: $CURSOR"
echo ""

# 1. Check current timestamp
echo "1️⃣ Current CockroachDB timestamp:"
psql $COCKROACHDB_URL -c "SELECT cluster_logical_timestamp()::string as current_ts, now()::timestamptz as wall_time;"
echo ""

# 2. Convert cursor to wall time for comparison
echo "2️⃣ Cursor wall time:"
psql $COCKROACHDB_URL << EOF
SELECT 
  '$CURSOR'::decimal as cursor_hlc,
  to_timestamp(('$CURSOR'::decimal / 1000000000)::bigint) as cursor_wall_time;
EOF
echo ""

# 3. Check recent table modifications
echo "3️⃣ Recent table modifications:"
psql $COCKROACHDB_URL << EOF
SELECT 
  count(*) as total_rows,
  min(crdb_internal_mvcc_timestamp) as oldest_mvcc_ts,
  max(crdb_internal_mvcc_timestamp) as newest_mvcc_ts
FROM usertable
LIMIT 10;
EOF
echo ""

# 4. Try changefeed with 5-second timeout
echo "4️⃣ Testing changefeed (5-second timeout):"
echo "   Command: EXPERIMENTAL CHANGEFEED FOR usertable WITH initial_scan='no', updated, resolved='1s', split_column_families, cursor='$CURSOR';"
echo ""
timeout 5 psql $COCKROACHDB_URL << EOF || echo "   (Timed out after 5s - this is expected)"
EXPERIMENTAL CHANGEFEED FOR usertable
  WITH initial_scan='no', updated, resolved='1s',
       split_column_families, cursor='$CURSOR';
EOF
echo ""

# 5. Count events manually
echo "5️⃣ Manual event count check:"
echo "   Checking if changes exist after cursor..."
psql $COCKROACHDB_URL << EOF
-- Check if any rows have MVCC timestamp > cursor
SELECT 
  CASE 
    WHEN COUNT(*) > 0 THEN 'YES - Changes exist!'
    ELSE 'NO - No changes after cursor'
  END as changes_exist,
  COUNT(*) as rows_with_newer_mvcc
FROM usertable
WHERE crdb_internal_mvcc_timestamp::string::decimal > '$CURSOR'::decimal;
EOF
echo ""

echo "========================================="
echo "✅ Verification Complete"
echo "========================================="




