#!/usr/bin/env bash
################################################################################
# Enable Rangefeeds for CockroachDB CDC
#
# Description:
#   Enables the kv.rangefeed.enabled cluster setting required for changefeeds.
#   This must be run before using changefeeds for CDC.
#
# Usage:
#   ./enable_rangefeeds.sh [--host HOST] [--port PORT]
#
# Options:
#   --host HOST    CockroachDB host (default: localhost)
#   --port PORT    CockroachDB port (default: 26257)
#
# Examples:
#   ./enable_rangefeeds.sh
#   ./enable_rangefeeds.sh --host my-cluster.cockroachdb.cloud --port 26257
#
################################################################################

set -e

# Default values
HOST="localhost"
PORT="26257"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--host HOST] [--port PORT]"
            exit 1
            ;;
    esac
done

echo "Enabling rangefeeds on $HOST:$PORT..."

# Enable rangefeeds
cockroach sql --insecure --host="$HOST:$PORT" -e \
    "SET CLUSTER SETTING kv.rangefeed.enabled = true;"

# Verify setting
echo ""
echo "Verifying setting..."
cockroach sql --insecure --host="$HOST:$PORT" -e \
    "SHOW CLUSTER SETTING kv.rangefeed.enabled;"

echo ""
echo "✅ Rangefeeds enabled successfully!"
echo "You can now use changefeeds for CDC."

