#!/usr/bin/env bash
set -e

# Two-Phase CDC Test: Snapshot first, then CDC only
# This guarantees clean separation between snapshot and CDC events

GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
AZURE_ENV="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.env"
CRDB_ENV="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cockroachcloud.env"

source "$AZURE_ENV"
source "$CRDB_ENV"

ENCODED_KEY=$(echo -n "$AZURE_STORAGE_KEY" | jq -sRr @uri)
AZURE_URI="azure://${AZURE_STORAGE_CONTAINER}/twophase-test?AZURE_ACCOUNT_NAME=${AZURE_STORAGE_ACCOUNT}&AZURE_ACCOUNT_KEY=${ENCODED_KEY}"

echo "🧪 Two-Phase CDC Test"
echo "===================="
echo ""

# Phase 1: Snapshot only
echo "Phase 1: Creating SNAPSHOT-ONLY changefeed..."
SNAPSHOT_JOB=$(python3 << PYTHON_EOF
import pg8000, ssl, os

conn_url = os.environ['COCKROACHDB_URL']
parts = conn_url.replace('postgresql://', '').split('@')
user, password = parts[0].split(':')
host_port_db = parts[1].split('/')
host, port = host_port_db[0].split(':')
database = host_port_db[1].split('?')[0]

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

conn = pg8000.connect(user=user, password=password, host=host, port=int(port), database=database, ssl_context=ssl_context)
cursor = conn.cursor()

sql = f"""
CREATE CHANGEFEED FOR TABLE usertable
INTO '$AZURE_URI'
WITH 
  format = 'parquet',
  compression = 'gzip',
  split_column_families,
  initial_scan = 'only'
"""

cursor.execute(sql)
result = cursor.fetchone()
print(result[0])

cursor.close()
conn.close()
PYTHON_EOF
)

echo "✅ Snapshot changefeed created: $SNAPSHOT_JOB"
echo "⏳ Waiting 120s for snapshot to complete..."
sleep 120

echo ""
echo "Phase 2: Creating CDC-ONLY changefeed..."
CDC_JOB=$(python3 << PYTHON_EOF
import pg8000, ssl, os

conn_url = os.environ['COCKROACHDB_URL']
parts = conn_url.replace('postgresql://', '').split('@')
user, password = parts[0].split(':')
host_port_db = parts[1].split('/')
host, port = host_port_db[0].split(':')
database = host_port_db[1].split('?')[0]

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

conn = pg8000.connect(user=user, password=password, host=host, port=int(port), database=database, ssl_context=ssl_context)
cursor = conn.cursor()

sql = f"""
CREATE CHANGEFEED FOR TABLE usertable
INTO '$AZURE_URI'
WITH 
  updated,
  resolved = '1s',
  format = 'parquet',
  compression = 'gzip',
  split_column_families,
  initial_scan = 'no'
"""

cursor.execute(sql)
result = cursor.fetchone()
print(result[0])

cursor.close()
conn.close()
PYTHON_EOF
)

echo "✅ CDC changefeed created: $CDC_JOB"
echo "⏳ Waiting 30s for changefeed to initialize..."
sleep 30

echo ""
echo "Phase 3: Running UPDATE workload..."
for i in {1..20}; do
  psql "$COCKROACHDB_URL" -c "UPDATE usertable SET field0 = field0 || '_batch${i}_' || REPEAT('_', 100) WHERE ycsb_key IN (SELECT ycsb_key FROM usertable LIMIT 500 OFFSET $((i*500)));" 2>&1 | grep "UPDATE" || true
  echo -n "."
done
echo ""
echo "✅ 10,000 UPDATEs complete"

echo ""
echo "⏳ Waiting 90s for CDC flush..."
sleep 90

echo ""
echo "Phase 4: Analyzing results..."
python3 ../scripts/changefeed_helper.py analyze-files \
    --format parquet \
    --account "$AZURE_STORAGE_ACCOUNT" \
    --key "$AZURE_STORAGE_KEY" \
    --container "$AZURE_STORAGE_CONTAINER" \
    --prefix twophase-test | tail -30

echo ""
echo "✅ Two-Phase Test Complete!"

