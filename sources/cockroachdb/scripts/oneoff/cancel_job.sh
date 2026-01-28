#!/usr/bin/env bash
# Cancel a specific CockroachDB changefeed job

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <job_id>"
    echo ""
    echo "Example: $0 1134081029864718337"
    exit 1
fi

JOB_ID=$1

GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

# Load CockroachDB credentials
CRDB_ENV="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cockroachcloud.env"
if [ -f "$CRDB_ENV" ]; then
    source "$CRDB_ENV"
else
    echo "❌ Missing $CRDB_ENV"
    exit 1
fi

echo "🔧 Canceling CockroachDB Job: $JOB_ID"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Use the shared Python module
python3 "$SCRIPT_DIR/cancel_changefeed_job.py" "$JOB_ID"

