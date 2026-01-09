#!/usr/bin/env bash
# Helper script to run pytest with correct PYTHONPATH
# Usage: ./run_pytest.sh

cd "$(dirname "$0")/../.."

echo "Running pytest for CockroachDB connector..."
echo "============================================"
echo ""

PYTHONPATH=. pytest sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py -v "$@"

