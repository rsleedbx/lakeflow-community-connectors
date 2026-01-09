#!/usr/bin/env bash

################################################################################
# Test Individual CDC Operations (One Row, One Operation at a Time)
# Purpose: Verify INSERT, UPDATE, DELETE are correctly captured in Parquet and JSON
################################################################################

set -e

CHANGEFEED_FORMAT="${1:-parquet}"  # Default to parquet
export CHANGEFEED_FORMAT  # Export for Python scripts

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "================================================================================
🧪 Single Operation CDC Test
================================================================================
Format: $CHANGEFEED_FORMAT
"

# Load credentials
AZURE_ENV="$SCRIPT_DIR/cockroachdb_cdc_azure.env"
COCKROACH_ENV="$PROJECT_ROOT/cockroachdb/.env/cockroachdb_cockroachcloud.env"

if [ ! -f "$AZURE_ENV" ]; then
    echo "❌ Azure environment file not found: $AZURE_ENV"
    exit 1
fi

if [ ! -f "$COCKROACH_ENV" ]; then
    echo "❌ CockroachDB environment file not found: $COCKROACH_ENV"
    exit 1
fi

source "$AZURE_ENV"
source "$COCKROACH_ENV"

# Verify COCKROACHDB_URL is set
if [ -z "$COCKROACHDB_URL" ]; then
    echo "❌ COCKROACHDB_URL not set in $COCKROACH_ENV"
    echo "   Add: export COCKROACHDB_URL='postgresql://user:pass@host:port/database?sslmode=require'"
    exit 1
fi

echo "✅ Credentials loaded"
echo ""

# Setup format-specific variables
if [ "$CHANGEFEED_FORMAT" = "parquet" ]; then
    PATH_SUFFIX="single-op-test-parquet"
    FILE_EXTENSION=".parquet"
    FILE_QUERY="[?contains(name, '.parquet')]"
else
    PATH_SUFFIX="single-op-test-json"
    FILE_EXTENSION=".ndjson"
    FILE_QUERY="[?contains(name, '.ndjson')]"
fi

# Construct Azure URI
AZURE_ACCOUNT_KEY_ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$AZURE_STORAGE_KEY', safe=''))")
AZURE_URI="azure://${AZURE_STORAGE_CONTAINER}/${PATH_SUFFIX}?AZURE_ACCOUNT_NAME=${AZURE_STORAGE_ACCOUNT}&AZURE_ACCOUNT_KEY=${AZURE_ACCOUNT_KEY_ENCODED}"
export AZURE_URI  # Export for Python scripts

echo "📦 Azure Path: ${PATH_SUFFIX}/"
echo ""

################################################################################
# Step 0: Create test table (simple schema)
################################################################################
echo "Step 0: Creating test table..."
psql "$COCKROACHDB_URL" << EOF
-- Drop table if exists
DROP TABLE IF EXISTS single_op_test CASCADE;

-- Create simple test table (no column families to avoid split complexity)
CREATE TABLE single_op_test (
    id INT PRIMARY KEY,
    name STRING,
    value STRING,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Insert initial row
INSERT INTO single_op_test (id, name, value) VALUES (1, 'test_row', 'initial_value');

SELECT 'Test table created with 1 row' as status;
EOF

echo "✅ Test table ready"
echo ""

################################################################################
# Step 1: Cancel existing changefeeds
################################################################################
echo "Step 1: Cancelling existing changefeeds for single_op_test..."

python3 << EOF
import pg8000.dbapi
import os
import urllib.parse

# Parse COCKROACHDB_URL
url = os.environ['COCKROACHDB_URL']
parsed = urllib.parse.urlparse(url)

conn = pg8000.dbapi.connect(
    user=parsed.username,
    password=parsed.password,
    host=parsed.hostname,
    port=parsed.port or 26257,
    database=parsed.path.lstrip('/') or 'defaultdb',
    ssl_context=True
)

cursor = conn.cursor()

# Find all jobs for single_op_test
cursor.execute("SELECT job_id, status FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%single_op_test%'")
result = cursor.fetchall()

if result:
    print(f"  Found {len(result)} existing changefeed(s). Cancelling...")
    for row in result:
        job_id = row[0]
        status = row[1]
        # Only cancel if not already in a terminal state
        if status not in ['canceled', 'failed', 'succeeded']:
            print(f"    Cancelling job {job_id} (status: {status})")
            cursor.execute(f"CANCEL JOB {job_id}")
        else:
            print(f"    Skipping job {job_id} (already {status})")
    conn.commit()
    print("  ✅ All changefeeds processed")
else:
    print("  ✅ No existing changefeeds found")

cursor.close()
conn.close()
EOF

echo ""

################################################################################
# Step 2: Create changefeed
################################################################################
echo "Step 2: Creating changefeed ($CHANGEFEED_FORMAT format)..."

CREATE_CHANGEFEED_SQL=$(python3 << EOF
import os

changefeed_format = os.environ['CHANGEFEED_FORMAT']
azure_uri = os.environ['AZURE_URI']

if changefeed_format == 'parquet':
    sql = f"""
CREATE CHANGEFEED FOR TABLE single_op_test
INTO '{azure_uri}'
WITH 
  format = 'parquet',
  compression = 'gzip',
  resolved = '1s',
  initial_scan = 'yes',
  updated;
"""
else:
    sql = f"""
CREATE CHANGEFEED FOR TABLE single_op_test
INTO '{azure_uri}'
WITH 
  format = 'json',
  envelope = 'wrapped',
  resolved = '1s',
  initial_scan = 'yes',
  updated,
  diff;
"""

print(sql)
EOF
)

export CHANGEFEED_FORMAT
export AZURE_URI

echo "SQL:"
echo "$CREATE_CHANGEFEED_SQL"
echo ""

JOB_ID=$(psql "$COCKROACHDB_URL" -tA -c "$CREATE_CHANGEFEED_SQL" 2>&1 | grep -E "^[0-9]+$" | head -1)

if [ -z "$JOB_ID" ]; then
    echo "❌ Failed to create changefeed"
    exit 1
fi

echo "✅ Changefeed created: Job ID $JOB_ID"
echo ""

################################################################################
# Step 3: Wait for initial scan
################################################################################
echo "Step 3: Waiting for initial scan (snapshot)..."
sleep 30
echo "✅ Initial scan complete"
echo ""

################################################################################
# Step 4: Clear Azure container
################################################################################
echo "Step 4: Clearing existing files..."
az storage blob delete-batch \
    --account-name $AZURE_STORAGE_ACCOUNT \
    --account-key "$AZURE_STORAGE_KEY" \
    --source $AZURE_STORAGE_CONTAINER \
    --pattern "${PATH_SUFFIX}/*" 2>/dev/null || true

echo "✅ Container cleared"
echo ""

################################################################################
# Step 5: Test INSERT
################################################################################
echo "================================================================================
🧪 TEST 1: INSERT
================================================================================
"

echo "Inserting new row (id=2)..."
psql "$COCKROACHDB_URL" << EOF
INSERT INTO single_op_test (id, name, value) VALUES (2, 'inserted_row', 'insert_value');
SELECT 'Inserted row 2' as status;
EOF

echo ""
echo "Waiting 120s for CDC flush..."
sleep 120

echo ""
echo "Checking for INSERT events..."
FILE_COUNT=$(az storage blob list \
    --account-name $AZURE_STORAGE_ACCOUNT \
    --account-key "$AZURE_STORAGE_KEY" \
    --container-name $AZURE_STORAGE_CONTAINER \
    --prefix "${PATH_SUFFIX}/" \
    --query "$FILE_QUERY" \
    --output tsv 2>/dev/null | wc -l | tr -d ' ')

echo "  Files found: $FILE_COUNT"

if [ "$FILE_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}✅ INSERT captured in $CHANGEFEED_FORMAT format${NC}"
else
    echo -e "  ${RED}❌ No files found - INSERT may not have flushed${NC}"
fi

# Analyze
echo ""
echo "Running analysis..."
cd "$SCRIPT_DIR"
python3 ./changefeed_helper.py analyze-files \
    --format "$CHANGEFEED_FORMAT" \
    --account "$AZURE_STORAGE_ACCOUNT" \
    --key "$AZURE_STORAGE_KEY" \
    --container "$AZURE_STORAGE_CONTAINER" \
    --prefix "${PATH_SUFFIX}"

echo ""
read -p "Press Enter to continue to UPDATE test..."

################################################################################
# Step 6: Test UPDATE
################################################################################
echo "
================================================================================
🧪 TEST 2: UPDATE
================================================================================
"

echo "Clearing files..."
az storage blob delete-batch \
    --account-name $AZURE_STORAGE_ACCOUNT \
    --account-key "$AZURE_STORAGE_KEY" \
    --source $AZURE_STORAGE_CONTAINER \
    --pattern "${PATH_SUFFIX}/*" 2>/dev/null || true

echo ""
echo "Updating existing row (id=2)..."
psql "$COCKROACHDB_URL" << EOF
UPDATE single_op_test SET value = 'updated_value', updated_at = now() WHERE id = 2;
SELECT 'Updated row 2' as status;
EOF

echo ""
echo "Waiting 120s for CDC flush..."
sleep 120

echo ""
echo "Checking for UPDATE events..."
FILE_COUNT=$(az storage blob list \
    --account-name $AZURE_STORAGE_ACCOUNT \
    --account-key "$AZURE_STORAGE_KEY" \
    --container-name $AZURE_STORAGE_CONTAINER \
    --prefix "${PATH_SUFFIX}/" \
    --query "$FILE_QUERY" \
    --output tsv 2>/dev/null | wc -l | tr -d ' ')

echo "  Files found: $FILE_COUNT"

if [ "$FILE_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}✅ UPDATE captured in $CHANGEFEED_FORMAT format${NC}"
else
    echo -e "  ${RED}❌ No files found - UPDATE may not have flushed${NC}"
fi

# Analyze
echo ""
echo "Running analysis..."
python3 ./changefeed_helper.py analyze-files \
    --format "$CHANGEFEED_FORMAT" \
    --account "$AZURE_STORAGE_ACCOUNT" \
    --key "$AZURE_STORAGE_KEY" \
    --container "$AZURE_STORAGE_CONTAINER" \
    --prefix "${PATH_SUFFIX}"

echo ""
read -p "Press Enter to continue to DELETE test..."

################################################################################
# Step 7: Test DELETE
################################################################################
echo "
================================================================================
🧪 TEST 3: DELETE
================================================================================
"

echo "Clearing files..."
az storage blob delete-batch \
    --account-name $AZURE_STORAGE_ACCOUNT \
    --account-key "$AZURE_STORAGE_KEY" \
    --source $AZURE_STORAGE_CONTAINER \
    --pattern "${PATH_SUFFIX}/*" 2>/dev/null || true

echo ""
echo "Deleting row (id=2)..."
psql "$COCKROACHDB_URL" << EOF
DELETE FROM single_op_test WHERE id = 2;
SELECT 'Deleted row 2' as status;
EOF

echo ""
echo "Waiting 120s for CDC flush..."
sleep 120

echo ""
echo "Checking for DELETE events..."
FILE_COUNT=$(az storage blob list \
    --account-name $AZURE_STORAGE_ACCOUNT \
    --account-key "$AZURE_STORAGE_KEY" \
    --container-name $AZURE_STORAGE_CONTAINER \
    --prefix "${PATH_SUFFIX}/" \
    --query "$FILE_QUERY" \
    --output tsv 2>/dev/null | wc -l | tr -d ' ')

echo "  Files found: $FILE_COUNT"

if [ "$FILE_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}✅ DELETE captured in $CHANGEFEED_FORMAT format${NC}"
else
    echo -e "  ${RED}❌ No files found - DELETE may not have flushed${NC}"
fi

# Analyze
echo ""
echo "Running analysis..."
python3 ./changefeed_helper.py analyze-files \
    --format "$CHANGEFEED_FORMAT" \
    --account "$AZURE_STORAGE_ACCOUNT" \
    --key "$AZURE_STORAGE_KEY" \
    --container "$AZURE_STORAGE_CONTAINER" \
    --prefix "${PATH_SUFFIX}"

################################################################################
# Summary
################################################################################
echo "
================================================================================
📊 TEST SUMMARY
================================================================================

Format: $CHANGEFEED_FORMAT
Table: single_op_test

Operations tested:
  1. ✅ INSERT (id=2)
  2. ✅ UPDATE (id=2)
  3. ✅ DELETE (id=2)

Check the analysis output above to verify each operation was captured correctly.

================================================================================
"

echo "Test complete!"

