#!/bin/bash
# Diagnostic script to understand why only 11 rows are being output

set -e

PIPELINE_ID="7d02d189-2d1b-4331-aee7-acf24bcd9225"
UPDATE_ID="ee25cb62-0e58-40de-baf9-3bf1824daed2"
TABLE_NAME="main.robert_lee_cockroachdb.usertable"

echo "=========================================="
echo "CockroachDB Connector Output Diagnostics"
echo "=========================================="
echo ""

echo "1. Latest Pipeline Update:"
databricks pipelines get $PIPELINE_ID --output json | jq '.latest_updates[0]'
echo ""

echo "2. Table Row Count:"
echo "SELECT COUNT(*) FROM $TABLE_NAME;" | databricks sql execute --query-stdin
echo ""

echo "3. Table Sample (first 5 rows):"
echo "SELECT * FROM $TABLE_NAME LIMIT 5;" | databricks sql execute --query-stdin
echo ""

echo "4. Table Metadata:"
echo "DESCRIBE EXTENDED $TABLE_NAME;" | databricks sql execute --query-stdin
echo ""

echo "5. Pipeline Events (showing errors and warnings):"
databricks pipelines list-pipeline-events $PIPELINE_ID \
  --filter "update_id='$UPDATE_ID' AND (level='ERROR' OR level='WARN')" \
  --max-results 100 | jq -r '.[] | "\(.timestamp) [\(.level)] \(.message)"'
echo ""

echo "=========================================="
echo "CRITICAL: Download full driver logs"
echo "=========================================="
echo "Go to:"
echo "  https://e2-dogfood.staging.cloud.databricks.com/pipelines/$PIPELINE_ID"
echo "  → Select update $UPDATE_ID"
echo "  → Download driver logs (stderr)"
echo ""
echo "Search for:"
echo "  - '✅ read_table_metadata() COMPLETED'"
echo "  - 'ingestion_type:'"
echo "  - 'Total events returned:'"
echo ""
echo "This will confirm if the ingestion_type='snapshot' fix was applied."
echo "=========================================="

