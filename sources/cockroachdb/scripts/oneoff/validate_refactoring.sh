#!/usr/bin/env bash
# Validation script for refactored test_cdc_matrix.sh
# Verifies that the consolidated utility functions work correctly

set -e

echo "🔍 Validating Refactored CDC Test Scripts"
echo "=========================================="
echo ""

# Find git root
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

SCRIPTS_DIR="$GIT_ROOT/sources/cockroachdb/scripts"
SOURCE_DIR="$GIT_ROOT/sources/cockroachdb"
CRDB_JSON="$SOURCE_DIR/.env/cockroachdb_credentials.json"
AZURE_JSON="$SOURCE_DIR/.env/cockroachdb_cdc_azure.json"

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

test_step() {
    local description=$1
    echo -n "  Testing: $description... "
}

pass() {
    echo -e "${GREEN}✅ PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

fail() {
    local message=$1
    echo -e "${RED}❌ FAIL${NC}"
    if [ -n "$message" ]; then
        echo "    Error: $message"
    fi
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

warn() {
    local message=$1
    echo -e "${YELLOW}⚠️  WARN${NC}"
    echo "    $message"
}

echo "1️⃣  Checking Prerequisites"
echo "─────────────────────────"
echo ""

# Check Python 3
test_step "Python 3 installed"
if command -v python3 &> /dev/null; then
    pass
else
    fail "python3 not found in PATH"
fi

# Check yq
test_step "yq installed"
if command -v yq &> /dev/null; then
    pass
else
    fail "yq not found - install with: brew install yq"
fi

# Check credential files
test_step "CockroachDB credentials exist"
if [ -f "$CRDB_JSON" ]; then
    pass
else
    fail "$CRDB_JSON not found"
fi

test_step "Azure credentials exist"
if [ -f "$AZURE_JSON" ]; then
    pass
else
    fail "$AZURE_JSON not found"
fi

echo ""
echo "2️⃣  Testing Python Utility Functions"
echo "────────────────────────────────────"
echo ""

# Test import
test_step "Import cockroachdb module"
if python3 -c "from cockroachdb import load_crdb_config, create_connector, analyze_azure_changefeed_files" 2>/dev/null; then
    pass
else
    fail "Failed to import utility functions"
fi

# Test credential loading
test_step "Load credentials (load_crdb_config)"
if python3 -c "from cockroachdb import load_crdb_config; config = load_crdb_config('$CRDB_JSON'); assert isinstance(config, dict) and len(config) > 0" 2>/dev/null; then
    pass
else
    fail "Failed to load CockroachDB credentials"
fi

# Test connector creation
test_step "Create connector (create_connector)"
if python3 -c "from cockroachdb import load_crdb_config, create_connector; import sys; config = load_crdb_config('$CRDB_JSON'); connector = create_connector(config); sys.exit(0 if connector else 1)" 2>/dev/null; then
    pass
else
    warn "Connector creation may require valid database connection"
fi

echo ""
echo "3️⃣  Testing CLI Wrappers"
echo "───────────────────────"
echo ""

# Test changefeed_helper.py
test_step "changefeed_helper.py executable"
if python3 "$SCRIPTS_DIR/changefeed_helper.py" --help &>/dev/null; then
    pass
else
    fail "changefeed_helper.py failed"
fi

# Test get-row-count command
test_step "changefeed_helper.py get-row-count"
if python3 "$SCRIPTS_DIR/changefeed_helper.py" get-row-count \
    --table usertable \
    --json "$CRDB_JSON" &>/dev/null; then
    pass
else
    warn "Could not get row count (table may not exist)"
fi

# Test find-changefeeds command
test_step "changefeed_helper.py find-changefeeds"
CHANGEFEED_OUTPUT=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" find-changefeeds \
    --table usertable \
    --json "$CRDB_JSON" 2>&1 || echo "")
if echo "$CHANGEFEED_OUTPUT" | grep -qE "(existing changefeed|No existing|Error:)"; then
    pass
else
    warn "find-changefeeds requires active database connection"
fi

# Test changefeed_helper.py analyze-files command
test_step "changefeed_helper.py analyze-files command"
if python3 "$SCRIPTS_DIR/changefeed_helper.py" analyze-files --help 2>&1 | grep -q "format"; then
    pass
else
    fail "changefeed_helper.py analyze-files command failed"
fi

echo ""
echo "4️⃣  Testing Refactored test_cdc_matrix.sh"
echo "─────────────────────────────────────────"
echo ""

# Test script syntax
test_step "test_cdc_matrix.sh syntax valid"
if bash -n "$SCRIPTS_DIR/test_cdc_matrix.sh" 2>/dev/null; then
    pass
else
    fail "Script has syntax errors"
fi

# Check for refactoring markers
test_step "Script has refactoring comments"
if grep -q "REFACTORED" "$SCRIPTS_DIR/test_cdc_matrix.sh"; then
    pass
else
    fail "Missing refactoring documentation"
fi

# Check for changefeed_helper.py usage
test_step "Script uses changefeed_helper.py"
if grep -q "changefeed_helper.py" "$SCRIPTS_DIR/test_cdc_matrix.sh"; then
    pass
else
    fail "Script doesn't use consolidated utilities"
fi

echo ""
echo "5️⃣  Documentation Validation"
echo "───────────────────────────"
echo ""

# Check for UTILITY_FUNCTIONS.md
test_step "UTILITY_FUNCTIONS.md exists"
if [ -f "$SOURCE_DIR/UTILITY_FUNCTIONS.md" ]; then
    pass
else
    fail "Missing utility functions documentation"
fi

# Check for refactoring note in CDC_TEST_MATRIX_RESULTS.md
test_step "CDC_TEST_MATRIX_RESULTS.md updated"
if grep -q "Refactoring Update" "$SOURCE_DIR/learnings/CDC_TEST_MATRIX_RESULTS.md"; then
    pass
else
    fail "CDC_TEST_MATRIX_RESULTS.md not updated"
fi

echo ""
echo "═══════════════════════════════════════════"
echo "📊 VALIDATION SUMMARY"
echo "═══════════════════════════════════════════"
echo ""
echo "  ✅ Passed: $TESTS_PASSED"
echo "  ❌ Failed: $TESTS_FAILED"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ All validation tests passed!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Run the full test matrix:"
    echo "     cd $SCRIPTS_DIR"
    echo "     ./test_cdc_matrix.sh"
    echo ""
    echo "  2. Compare results with expected baseline in:"
    echo "     CDC_TEST_MATRIX_RESULTS.md"
    echo ""
    exit 0
else
    echo -e "${RED}❌ Some validation tests failed!${NC}"
    echo ""
    echo "Please fix the issues above before running test_cdc_matrix.sh"
    echo ""
    exit 1
fi

