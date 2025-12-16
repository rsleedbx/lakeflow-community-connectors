#!/bin/bash
################################################################################
# CockroachDB Local Setup Diagnostic Script
#
# Description:
#   Checks all prerequisites for running the CockroachDB connector tests.
#   Run this before running test_local.py to diagnose issues.
#
# Usage:
#   ./check_setup.sh
#
################################################################################

set +e  # Don't exit on errors, we want to check everything

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ISSUES=0

echo "=============================================="
echo "CockroachDB Connector Setup Diagnostic"
echo "=============================================="
echo ""

# Check 1: CockroachDB binary
echo "1. Checking for cockroach binary..."
if command -v cockroach >/dev/null 2>&1; then
    VERSION=$(cockroach version 2>&1 | head -1)
    echo -e "${GREEN}✅ Found:${NC} $VERSION"
else
    echo -e "${RED}❌ cockroach binary not found${NC}"
    echo "   Install from: https://www.cockroachlabs.com/docs/stable/install-cockroachdb"
    ISSUES=$((ISSUES + 1))
fi
echo ""

# Check 2: CockroachDB running
echo "2. Checking if CockroachDB is running..."
if cockroach node status --insecure --host=localhost:26257 >/dev/null 2>&1; then
    echo -e "${GREEN}✅ CockroachDB is running on localhost:26257${NC}"
else
    echo -e "${RED}❌ CockroachDB is not running${NC}"
    echo "   Run: ./local_setup.sh start"
    ISSUES=$((ISSUES + 1))
fi
echo ""

# Check 3: ycsb database exists
echo "3. Checking for ycsb database..."
if cockroach sql --insecure --host=localhost:26257 -e "SHOW DATABASES;" 2>/dev/null | grep -q "ycsb"; then
    echo -e "${GREEN}✅ ycsb database exists${NC}"
    
    # Check for tables
    TABLE_COUNT=$(cockroach sql --insecure --host=localhost:26257 -d ycsb -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';" 2>/dev/null | grep -o "[0-9]*" | head -1)
    if [ -n "$TABLE_COUNT" ] && [ "$TABLE_COUNT" -gt "0" ]; then
        echo "   Found $TABLE_COUNT tables"
    else
        echo -e "   ${YELLOW}⚠️  No tables found${NC}"
        echo "   Run: ./local_setup.sh start"
    fi
else
    echo -e "${RED}❌ ycsb database not found${NC}"
    echo "   Run: ./local_setup.sh start"
    ISSUES=$((ISSUES + 1))
fi
echo ""

# Check 4: Rangefeeds enabled
echo "4. Checking if rangefeeds are enabled..."
RANGEFEED_OUTPUT=$(cockroach sql --insecure --host=localhost:26257 -e "SHOW CLUSTER SETTING kv.rangefeed.enabled;" 2>/dev/null)
if echo "$RANGEFEED_OUTPUT" | grep -qi "true"; then
    echo -e "${GREEN}✅ Rangefeeds are enabled${NC}"
elif echo "$RANGEFEED_OUTPUT" | grep -qi "false"; then
    echo -e "${RED}❌ Rangefeeds are NOT enabled${NC}"
    echo "   This is REQUIRED for changefeeds/CDC"
    echo "   Fix: ./enable_rangefeeds.sh"
    ISSUES=$((ISSUES + 1))
else
    echo -e "${YELLOW}⚠️  Could not determine rangefeed status${NC}"
    echo "   Output: $RANGEFEED_OUTPUT"
    echo "   Verify manually: cockroach sql --insecure -e 'SHOW CLUSTER SETTING kv.rangefeed.enabled;'"
fi
echo ""

# Check 5: Python dependencies
echo "5. Checking Python dependencies..."
if python3 -c "import psycopg2" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ psycopg2 installed${NC}"
else
    echo -e "${RED}❌ psycopg2 not installed${NC}"
    echo "   Run: pip install -r requirements.txt"
    ISSUES=$((ISSUES + 1))
fi

if python3 -c "import pyspark" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ pyspark installed${NC}"
else
    echo -e "${RED}❌ pyspark not installed${NC}"
    echo "   Run: pip install -r requirements.txt"
    ISSUES=$((ISSUES + 1))
fi
echo ""

# Summary
echo "=============================================="
if [ $ISSUES -eq 0 ]; then
    echo -e "${GREEN}✅ All checks passed!${NC}"
    echo ""
    echo "You can now run:"
    echo "  python test_local.py"
else
    echo -e "${RED}❌ Found $ISSUES issue(s)${NC}"
    echo ""
    echo "Quick fix (recommended):"
    echo "  1. ./local_setup.sh start"
    echo "  2. pip install -r requirements.txt"
    echo "  3. python test_local.py"
fi
echo "=============================================="

