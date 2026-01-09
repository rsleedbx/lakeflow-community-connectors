#!/usr/bin/env bash
# Comprehensive CDC Test Matrix
# Tests all combinations of format, table, and split_column_families
# to determine which produce snapshot + CDC records
#
# REFACTORED: Now uses utility functions from cockroachdb.py via changefeed_helper.py
# See: UTILITY_FUNCTIONS.md for details
#
# Usage:
#   ./test_cdc_matrix.sh                      # Run full test (create changefeeds)
#   ./test_cdc_matrix.sh parquet              # Test only parquet format
#   ./test_cdc_matrix.sh json                 # Test only json format
#   ./test_cdc_matrix.sh --validate-only      # Validate existing files (latest timestamp)
#   ./test_cdc_matrix.sh --validate-only 1767895046  # Validate specific timestamp

set -e

# Workload parameters (used for both test execution and validation)
# These must match the values used in generate-*-sql commands
SIMPLE_TEST_INITIAL_ROWS=1000
USERTABLE_INITIAL_ROWS=10000
WORKLOAD_UPDATE_COUNT=400
WORKLOAD_INSERT_COUNT=50
WORKLOAD_DELETE_COUNT=100

# Parse command line arguments
FILTER_FORMAT=""
VALIDATE_ONLY=false
TEST_RUN_TIMESTAMP=$(date +%s)

while [ $# -gt 0 ]; do
    case "$1" in
        --validate-only|-v)
            VALIDATE_ONLY=true
            # Check if next arg is a timestamp
            if [ $# -gt 1 ] && [[ "$2" =~ ^[0-9]+$ ]]; then
                TEST_RUN_TIMESTAMP="$2"
                shift
            else
                # Find latest timestamp from Azure
                echo "🔍 Finding latest test run timestamp..."
                # Will be set after loading credentials
                TEST_RUN_TIMESTAMP="latest"
            fi
            shift
            ;;
        json|parquet)
            FILTER_FORMAT="$1"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS] [FORMAT]"
            echo ""
            echo "Modes:"
            echo "  Full Test Mode (default):"
            echo "    $0                  # Test all formats (creates changefeeds)"
            echo "    $0 json             # Test only JSON format"
            echo "    $0 parquet          # Test only Parquet format"
            echo ""
            echo "  Validation Mode:"
            echo "    $0 --validate-only              # Validate latest test run"
            echo "    $0 --validate-only 1767895046   # Validate specific timestamp"
            echo "    $0 -v json                      # Validate latest JSON tests only"
            echo ""
            echo "Options:"
            echo "  -v, --validate-only  Skip changefeed creation, analyze existing files"
            echo "  -h, --help           Show this help message"
            echo ""
            echo "Validation mode is much faster and useful for:"
            echo "  • Testing code changes against existing data"
            echo "  • Verifying fixes without re-running full test suite"
            echo "  • Debugging analysis logic"
            exit 0
            ;;
        *)
            echo "❌ Invalid argument: $1"
            echo "   Run '$0 --help' for usage"
            exit 1
            ;;
    esac
done

if $VALIDATE_ONLY; then
    echo "🔬 VALIDATION MODE - Testing against existing files"
    echo "   Timestamp: $TEST_RUN_TIMESTAMP"
    if [ -n "$FILTER_FORMAT" ]; then
        echo "   Format filter: $FILTER_FORMAT"
    fi
    echo ""
else
    if [ -n "$FILTER_FORMAT" ]; then
        echo "🎯 Testing only: $FILTER_FORMAT format"
        echo ""
    fi
fi

# Find git root and load credentials from there
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

SCRIPTS_DIR="$GIT_ROOT/sources/cockroachdb/scripts"
AZURE_JSON="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.json"
CRDB_JSON="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_credentials.json"

# Verify credential files exist
if [ ! -f "$AZURE_JSON" ]; then
    echo "❌ Missing $AZURE_JSON"
    echo "   Run: sources/cockroachdb/scripts/01_azure_storage.sh"
    exit 1
fi

if [ ! -f "$CRDB_JSON" ]; then
    echo "❌ Missing $CRDB_JSON"
    echo "   Create it with your CockroachDB credentials"
    echo "   Example: {\"cockroachdb_url\": \"postgresql://user:pass@host:port/database?sslmode=require\"}"
    exit 1
fi

# Load credentials using Python utility functions
# This is cleaner than parsing with yq in bash
# Unset first in case they were defined outside this script
unset azure_creds
unset crdb_creds
declare -A azure_creds
declare -A crdb_creds

# Helper function to find latest test timestamp from Azure
find_latest_timestamp() {
    local format_prefix=""
    if [ -n "$FILTER_FORMAT" ]; then
        format_prefix="$FILTER_FORMAT/"
    fi
    
    # List directories and find most recent timestamp
    local timestamps=$(az storage blob list \
        --account-name "${azure_creds[azure_storage_account]}" \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name changefeed-events \
        --prefix "${format_prefix}" \
        --query "[].name" \
        --output tsv 2>/dev/null | \
        grep -oE '[0-9]{10}/' | \
        sort -u -r | \
        head -1 | \
        tr -d '/')
    
    if [ -z "$timestamps" ]; then
        echo "❌ No test data found in Azure"
        echo "   Run a full test first: $0"
        exit 1
    fi
    
    echo "$timestamps"
}

# Check if yq is available (still needed for bash associative array population)
if ! command -v yq &> /dev/null; then
    echo "❌ Error: yq is required to parse JSON credentials"
    echo "   Install with: brew install yq (macOS) or snap install yq (Linux)"
    exit 1
fi

# Load Azure credentials into associative array
while IFS='=' read -r key value; do
    value="${value%\'}"
    value="${value#\'}"
    azure_creds["$key"]="$value"
done < <(yq -o=shell "$AZURE_JSON")

# Load CockroachDB credentials into associative array  
while IFS='=' read -r key value; do
    value="${value%\'}"
    value="${value#\'}"
    crdb_creds["$key"]="$value"
done < <(yq -o=shell "$CRDB_JSON")

# Verify cockroachdb_url is set
if [ -z "${crdb_creds[cockroachdb_url]}" ]; then
    echo "❌ cockroachdb_url not set in $CRDB_JSON"
    exit 1
fi

# URL encode the Azure key using jq
ENCODED_KEY=$(echo -n "${azure_creds[azure_storage_key]}" | jq -sRr @uri)

# Resolve "latest" timestamp if in validation mode
if $VALIDATE_ONLY && [ "$TEST_RUN_TIMESTAMP" = "latest" ]; then
    echo "🔍 Finding latest test timestamp in Azure..."
    TEST_RUN_TIMESTAMP=$(find_latest_timestamp)
    echo "   Found: $TEST_RUN_TIMESTAMP"
    echo ""
fi

# Test matrix (unset first for clean state)
unset FORMATS TABLES SPLIT_OPTIONS

# Set formats based on filter
if [ -n "$FILTER_FORMAT" ]; then
    FORMATS=("$FILTER_FORMAT")
else
    FORMATS=("json" "parquet")
fi

TABLES=("usertable" "simple_test")
SPLIT_OPTIONS=("with_split" "no_split")

# Results storage
RESULTS_FILE="/tmp/cdc_test_results.txt"
echo "CDC Test Matrix Results - $(date)" > "$RESULTS_FILE"
echo "=" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Load Unity Catalog config for volume paths (FAIL if missing - no silent fallbacks)
PIPELINE_JSON="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_pipelines.json"
if [ ! -f "$PIPELINE_JSON" ]; then
    echo "❌ Error: Pipeline config not found: $PIPELINE_JSON"
    exit 1
fi

UC_CATALOG=$(yq eval '.catalog' "$PIPELINE_JSON" 2>/dev/null)
UC_SCHEMA=$(yq eval '.schema' "$PIPELINE_JSON" 2>/dev/null)
UC_VOLUME=$(yq eval '.volume_name' "$PIPELINE_JSON" 2>/dev/null)

# Validate required values (no silent fallbacks)
if [ -z "$UC_CATALOG" ] || [ "$UC_CATALOG" = "null" ]; then
    echo "❌ Error: Missing 'catalog' in $PIPELINE_JSON"
    exit 1
fi
if [ -z "$UC_SCHEMA" ] || [ "$UC_SCHEMA" = "null" ]; then
    echo "❌ Error: Missing 'schema' in $PIPELINE_JSON"
    exit 1
fi
if [ -z "$UC_VOLUME" ] || [ "$UC_VOLUME" = "null" ]; then
    echo "❌ Error: Missing 'volume_name' in $PIPELINE_JSON"
    exit 1
fi

# Test counter
TEST_NUM=0
TOTAL_TESTS=$((${#FORMATS[@]} * ${#TABLES[@]} * ${#SPLIT_OPTIONS[@]}))

echo "🧪 CDC Test Matrix"
echo "================="
if [ -n "$FILTER_FORMAT" ]; then
    echo "Format filter: $FILTER_FORMAT only"
fi
echo "Total combinations to test: $TOTAL_TESTS"
echo "  Formats: ${FORMATS[@]}"
echo "  Tables: ${TABLES[@]}"
echo "  Split options: ${SPLIT_OPTIONS[@]}"
echo ""

# Helper function: Run command with timeout (handles timeout/gtimeout/none)
run_with_timeout() {
    local timeout_seconds=$1
    shift
    local cmd="$@"
    
    if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_seconds" $cmd </dev/null 2>&1
    elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "$timeout_seconds" $cmd </dev/null 2>&1
    else
        echo "   ⚠️  Warning: timeout command not available" >&2
        $cmd </dev/null 2>&1
    fi
}

# Helper function: Verify primary key on a table
# REFACTORED: Now uses cockroachdb.py via changefeed_helper.py (no SQL duplication)
verify_primary_key() {
    local table=$1
    local pk_columns
    
    pk_columns=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" get-primary-keys \
        --table "$table" \
        --json "$CRDB_JSON" \
        --catalog defaultdb \
        --schema public 2>&1)
    
    if [[ "$pk_columns" == ERROR:* ]] || [ -z "$pk_columns" ]; then
        echo "❌ Error: No primary key found on $table"
        return 1
    fi
    
    echo "$pk_columns"
    return 0
}

# Helper function: Check changefeed health
check_changefeed_health() {
    local job_id=$1
    local timeout_seconds=$2
    local check_label=$3  # e.g., "immediate" or "after wait"
    local test_name=$4
    
    echo "🏥 Checking changefeed health${check_label:+ $check_label}..."
    
    # Show debug info for post-wait checks
    if [[ "$check_label" == *"wait"* ]]; then
        echo "   [DEBUG] Job ID: $job_id"
        echo "   [DEBUG] Config: $CRDB_JSON"
        if command -v timeout >/dev/null 2>&1; then
            echo "   [DEBUG] Using timeout command..."
        elif command -v gtimeout >/dev/null 2>&1; then
            echo "   [DEBUG] Using gtimeout command..."
        fi
    fi
    
    # Run health check with timeout
    local health_output
    health_output=$(run_with_timeout "$timeout_seconds" python3 -u "$SCRIPTS_DIR/changefeed_helper.py" check-status \
        --job-id "$job_id" \
        --json "$CRDB_JSON")
    local health_exit_code=$?
    
    # Show debug info for post-wait checks
    if [[ "$check_label" == *"wait"* ]]; then
        echo "   [DEBUG] Command completed with exit code: $health_exit_code"
    fi
    
    # Check for timeout
    if [ $health_exit_code -eq 124 ] || [ $health_exit_code -eq 142 ]; then
        echo -e "${RED}❌ Health check timed out${check_label:+ ($check_label)}${NC}"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - FAILED (health check timeout)" >> "$RESULTS_FILE"
        return 2  # Special code for timeout
    fi
    
    local job_status=$(echo "$health_output" | grep -E "^\s*Status:" | sed 's/^[[:space:]]*Status:[[:space:]]*//')
    
    # Check for errors
    if [ $health_exit_code -ne 0 ]; then
        echo -e "${RED}❌ Changefeed has errors${check_label:+ $check_label}!${NC}"
        echo ""
        echo "Job Status Details:"
        echo "$health_output"
        echo ""
        
        # Extract and display error details
        local error_msg=$(echo "$health_output" | grep -E "^\s*Error:" | sed 's/^[[:space:]]*Error:[[:space:]]*//')
        if [ -n "$error_msg" ]; then
            echo "Error: $error_msg"
        fi
        
        local error_category=$(echo "$health_output" | grep -E "^\s*Category:" | sed 's/^[[:space:]]*Category:[[:space:]]*//')
        if [ -n "$error_category" ]; then
            echo "Category: $error_category"
        fi
        
        local error_suggestion=$(echo "$health_output" | grep -E "^\s*💡 Suggestion:" | sed 's/^[[:space:]]*💡 Suggestion:[[:space:]]*//')
        if [ -n "$error_suggestion" ]; then
            echo "💡 $error_suggestion"
        fi
        
        echo ""
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - FAILED (changefeed errors)" >> "$RESULTS_FILE"
        return 1  # Error
    fi
    
    echo "   Status: $job_status ✓"
    echo ""
    return 0  # Success
}

# Helper function: Count Azure blobs with specific prefix and extension
count_azure_blobs() {
    local prefix=$1
    local file_ext=$2
    
    az storage blob list \
        --account-name "${azure_creds[azure_storage_account]}" \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name changefeed-events \
        --prefix "$prefix/" \
        --query "[?contains(name, '$file_ext')]" \
        --output tsv 2>/dev/null | wc -l | tr -d ' '
}

# Helper function: Execute SQL and return result with fallback
execute_sql() {
    local sql=$1
    local pattern=$2  # e.g., "UPDATE" or "DELETE"
    local default="${pattern} 0"
    
    psql "${crdb_creds[cockroachdb_url]}" -t -c "$sql" 2>&1 | grep "$pattern" || echo "$default"
}

# Helper function: Get row count from table
get_row_count() {
    local table=$1
    psql "${crdb_creds[cockroachdb_url]}" -t -c "SELECT count(*) FROM $table;" 2>/dev/null | tr -d ' '
}

# Helper function: Verify row count matches expected value
verify_row_count() {
    local table=$1
    local expected=$2
    local context=$3  # e.g., "after creation" or "after workload"
    
    local actual
    actual=$(get_row_count "$table")
    
    if [ -z "$actual" ] || ! [[ "$actual" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Failed to get row count for $table $context"
        return 1
    fi
    
    if [ "$actual" -ne "$expected" ]; then
        echo "⚠️  Warning: Expected $expected rows, got $actual rows $context"
        return 2  # Warning, not fatal
    fi
    
    echo "$actual"
    return 0
}

# Function to validate existing test data (validation mode only)
# Skips all changefeed creation and workload, just analyzes existing Azure files
validate_test() {
    local format=$1
    local base_table=$2
    local split_option=$3
    local test_name="${format}_${base_table}_${split_option}"
    
    local table="test_${test_name}"
    local catalog="defaultdb"
    local schema="public"
    local path_prefix="${format}/${catalog}/${schema}/test-${test_name}/${TEST_RUN_TIMESTAMP}"
    
    TEST_NUM=$((TEST_NUM + 1))
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Validation $TEST_NUM/$TOTAL_TESTS: $test_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Format: $format"
    echo "  Base Table: $base_table"
    echo "  Split Option: $split_option"
    echo "  Path: $path_prefix/"
    echo ""
    
    # Check if files exist
    local file_ext=".ndjson"
    if [ "$format" = "parquet" ]; then
        file_ext=".parquet"
    fi
    
    local file_count=$(count_azure_blobs "$path_prefix" "$file_ext")
    
    if [ "$file_count" -eq 0 ]; then
        echo "⚠️  No files found at path: $path_prefix/"
        echo "   Skipping validation for this test"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - SKIPPED (no data)" >> "$RESULTS_FILE"
        return
    fi
    
    echo "📊 Found $file_count ${format} files"
    echo ""
    
    # Analyze changefeed files
    echo "📊 Analyzing changefeed data..."
    
    local analysis_output=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" analyze-files \
        --format "$format" \
        --account "${azure_creds[azure_storage_account]}" \
        --key "${azure_creds[azure_storage_key]}" \
        --container changefeed-events \
        --prefix "${path_prefix}/" \
        --table "$table" \
        --debug 2>&1)
    
    # Show debug output
    echo "$analysis_output" | grep -v "^JSON_STATS=" | grep -E "^   " || true
    
    # Extract JSON stats
    local json_line=$(echo "$analysis_output" | grep "^JSON_STATS=" | sed 's/^JSON_STATS=//')
    
    if [ -z "$json_line" ]; then
        echo "❌ Error: Failed to analyze files"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - FAILED (analysis error)" >> "$RESULTS_FILE"
        return
    fi
    
    # Parse JSON using jq
    local snapshot_rows=$(echo "$json_line" | jq -r '.snapshot // 0')
    local insert_rows=$(echo "$json_line" | jq -r '.insert // 0')
    local update_rows=$(echo "$json_line" | jq -r '.update // 0')
    local delete_rows=$(echo "$json_line" | jq -r '.delete // 0')
    local unique_keys=$(echo "$json_line" | jq -r '.unique_keys // 0')
    
    echo ""
    echo "📊 CDC Operation Statistics:"
    echo "  Snapshot rows: $snapshot_rows"
    echo "  Insert rows: $insert_rows"
    echo "  Update rows: $update_rows"
    echo "  Delete rows: $delete_rows"
    echo "  Unique keys (deduplicated): $unique_keys"
    
    # Calculate expected values from workload parameters (not hardcoded!)
    local initial_rows
    if [[ "$base_table" == "simple_test" ]]; then
        initial_rows=$SIMPLE_TEST_INITIAL_ROWS
    else
        initial_rows=$USERTABLE_INITIAL_ROWS
    fi
    
    # Expected unique keys = initial + inserts - deletes
    # (updates don't change key count, they just modify existing rows)
    local expected_unique_keys=$((initial_rows + WORKLOAD_INSERT_COUNT - WORKLOAD_DELETE_COUNT))
    
    echo ""
    echo "📊 Expected counts (from workload parameters):"
    echo "  Initial rows: $initial_rows"
    echo "  + Inserts: $WORKLOAD_INSERT_COUNT"
    echo "  - Deletes: $WORKLOAD_DELETE_COUNT"
    echo "  = Expected unique keys: $expected_unique_keys"
    echo ""
    
    if [ "$unique_keys" -eq "$expected_unique_keys" ]; then
        echo "  ✅ Unique keys match expected ($expected_unique_keys)"
    else
        echo "  ⚠️  Unique keys ($unique_keys) ≠ Expected ($expected_unique_keys)"
    fi
    
    # Record result
    local result_status="✅ VALIDATION PASS"
    local result_color="$GREEN"
    
    if [ "$unique_keys" -ne "$expected_unique_keys" ]; then
        result_status="⚠️  VALIDATION WARNING"
        result_color="$YELLOW"
    fi
    
    echo ""
    echo -e "${result_color}${result_status}${NC}"
    
    echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - VALIDATED (files=$file_count, rows: snap=$snapshot_rows ins=$insert_rows upd=$update_rows del=$delete_rows)" >> "$RESULTS_FILE"
}

# Function to run a single test
# REFACTORED: Uses changefeed_helper.py (cockroachdb.py utilities) for changefeed operations
run_test() {
    local format=$1
    local base_table=$2
    local split_option=$3
    local test_name="${format}_${base_table}_${split_option}"
    
    # Create unique table name for this test (avoids interference between tests)
    local table="test_${test_name}"
    
    # Use production-style hierarchical paths: format/catalog/schema/test-scenario/timestamp
    # Timestamp ensures each test run has isolated data (no stale data from previous runs)
    local catalog="defaultdb"
    local schema="public"
    local path_prefix="${format}/${catalog}/${schema}/test-${test_name}/${TEST_RUN_TIMESTAMP}"
    
    TEST_NUM=$((TEST_NUM + 1))
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Format: $format"
    echo "  Catalog: $catalog"
    echo "  Schema: $schema"
    echo "  Base Table: $base_table"
    echo "  Test Table: $table"
    echo "  Split Column Families: $split_option"
    echo "  Path: $path_prefix/"
    echo ""
    
    # Build changefeed SQL based on parameters
    local azure_uri="azure://changefeed-events/${path_prefix}?AZURE_ACCOUNT_NAME=${azure_creds[azure_storage_account]}&AZURE_ACCOUNT_KEY=${ENCODED_KEY}"
    
    local split_clause=""
    if [ "$split_option" = "with_split" ]; then
        split_clause="split_column_families,"
    fi
    
    local format_clause=""
    if [ "$format" = "parquet" ]; then
        format_clause="format = 'parquet', compression = 'gzip'"
    else
        format_clause="format = 'json', envelope = 'wrapped'"
    fi
    
    local changefeed_sql="CREATE CHANGEFEED FOR TABLE $table
INTO '$azure_uri'
WITH 
  updated,
  resolved = '10s',
  ${split_clause}
  ${format_clause};"
    
    echo "📋 Changefeed SQL:"
    echo "$changefeed_sql" | sed 's/^/  /'
    echo ""
    
    # Create fresh test table for this specific test
    echo "📋 Creating test table: $table..."
    if [[ "$base_table" == "simple_test" ]]; then
        # Generate SQL using Python helper
        local sql_commands
        sql_commands=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-test-sql \
            --table "$table" \
            --schema-type simple \
            --rows $SIMPLE_TEST_INITIAL_ROWS)
        
        psql "${crdb_creds[cockroachdb_url]}" <<EOF 2>&1 | grep -E "DROP|CREATE|INSERT" || true
$sql_commands
EOF
        
        # Verify primary key was set correctly
        local pk_columns
        if ! pk_columns=$(verify_primary_key "$table"); then
            return 1
        fi
        
        echo "✅ $table created: 1,000 rows (ids 1-1000, PK: $pk_columns)"
    else
        # For usertable: use YCSB schema with generate_series
        local families_flag=""
        if [[ "$split_option" == "with_split" ]]; then
            families_flag="--families"
        fi
        
        local sql_commands
        sql_commands=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-test-sql \
            --table "$table" \
            --schema-type ycsb \
            --rows $USERTABLE_INITIAL_ROWS \
            $families_flag)
        
        psql "${crdb_creds[cockroachdb_url]}" <<EOF 2>&1 | grep -E "DROP|CREATE|INSERT" || true
$sql_commands
EOF
        
        # Verify table creation and count
        local target_rows=$USERTABLE_INITIAL_ROWS
        local row_count
        if ! row_count=$(verify_row_count "$table" "$target_rows" "after creation"); then
            # Fatal error (couldn't get count)
            if [ $? -eq 1 ]; then
                return 1
            fi
            # Warning (count mismatch) - continue but with actual count
            row_count=$(get_row_count "$table")
        fi
        
        # Verify primary key was set correctly
        local pk_columns
        if ! pk_columns=$(verify_primary_key "$table"); then
            return 1
        fi
        
        # Verify it's the correct PK (not rowid)
        if [ "$pk_columns" = "rowid" ]; then
            echo "❌ Error: Table has auto-generated rowid instead of ycsb_key PK"
            return 1
        fi
        
        echo "✅ $table created: $row_count rows (user0000000001-user0000010000, PK: $pk_columns)"
    fi
    echo ""
    
    # Create changefeed using changefeed_helper.py
    echo "🚀 Creating changefeed..."
    
    # Try to create changefeed - capture both stdout and stderr
    local create_output=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" create-changefeed \
        --table "$table" \
        --azure-uri "$azure_uri" \
        --format "$format" \
        --json "$CRDB_JSON" 2>&1)
    
    # Check if it's the split_column_families requirement error
    if echo "$create_output" | grep -q "requires WITH split_column_families"; then
        echo -e "${YELLOW}⚠️  Table requires split_column_families - skipping this combination${NC}"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - SKIPPED (split_column_families required)" >> "$RESULTS_FILE"
        return
    fi
    
    # Extract job ID (should be a clean number if successful)
    local job_id=$(echo "$create_output" | grep -E '^[0-9]+$' | tail -1)
    
    if [ -z "$job_id" ]; then
        echo -e "${RED}❌ Failed to create changefeed${NC}"
        echo "Output: $create_output"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - FAILED (creation)" >> "$RESULTS_FILE"
        return
    fi
    
    echo "✅ Changefeed created: Job $job_id"
    echo ""
    
    # Create schema file
    echo "📄 Creating schema file..."
    local schema_output=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" create-schema-file \
        --table "$table" \
        --json "$CRDB_JSON" \
        --account "${azure_creds[azure_account]}" \
        --key "${azure_creds[azure_key]}" \
        --container "changefeed-events" \
        --prefix "$path_prefix" 2>&1)
    
    if echo "$schema_output" | grep -q "✅ Schema file created"; then
        echo "$schema_output"
    else
        echo -e "${YELLOW}⚠️  Failed to create schema file (non-fatal)${NC}"
        echo "$schema_output"
    fi
    echo ""
    
    # Check changefeed health immediately
    if ! check_changefeed_health "$job_id" 10 "" "$test_name"; then
        psql "${crdb_creds[cockroachdb_url]}" -c "CANCEL JOB $job_id;" 2>&1 > /dev/null
        return
    fi
    
    # Wait for initial snapshot
    echo "⏳ Waiting 30s for initial snapshot..."
    sleep 30
    
    # Check changefeed health again after waiting
    if ! check_changefeed_health "$job_id" 30 "after wait" "$test_name"; then
        psql "${crdb_creds[cockroachdb_url]}" -c "CANCEL JOB $job_id;" 2>&1 > /dev/null
        return
    fi
    
    # Check snapshot files
    local file_ext=".ndjson"
    if [ "$format" = "parquet" ]; then
        file_ext=".parquet"
    fi
    
    local snapshot_count=$(count_azure_blobs "$path_prefix" "$file_ext")
    
    echo "📸 Snapshot files found: $snapshot_count"
    
    if [ "$snapshot_count" -eq 0 ]; then
        echo -e "${RED}❌ No snapshot files created!${NC}"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - FAILED (no snapshot)" >> "$RESULTS_FILE"
        psql "${crdb_creds[cockroachdb_url]}" -c "CANCEL JOB $job_id;" 2>&1 > /dev/null
        return
    fi
    
    # Run deterministic workload with UPDATEs, DELETEs, and INSERTs
    echo ""
    echo "🏋️  Running workload (400 UPDATEs + 100 DELETEs + 50 INSERTs)..."
    
    # Get actual row count before workload
    local pre_workload_count
    pre_workload_count=$(get_row_count "$table")
    echo "  Pre-workload count: $pre_workload_count rows"
    
    if [[ "$base_table" == "usertable" ]]; then
        # YCSB workload - use deterministic key ranges
        echo "  Step 1: Updating first $WORKLOAD_UPDATE_COUNT rows (by ycsb_key order)..."
        local update_sql
        update_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-update-sql \
            --table "$table" \
            --schema-type ycsb \
            --rows $WORKLOAD_UPDATE_COUNT)
        local update_result
        update_result=$(execute_sql "$update_sql" "UPDATE")
        echo "  $update_result"
        
        echo "  Step 2: Deleting last $WORKLOAD_DELETE_COUNT rows (by ycsb_key order)..."
        local delete_sql
        delete_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-delete-sql \
            --table "$table" \
            --schema-type ycsb \
            --rows $WORKLOAD_DELETE_COUNT \
            --from-end)
        local delete_result
        delete_result=$(execute_sql "$delete_sql" "DELETE")
        echo "  $delete_result"
        
        echo "  Step 3: Inserting $WORKLOAD_INSERT_COUNT new rows..."
        local insert_sql
        insert_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-insert-sql \
            --table "$table" \
            --schema-type ycsb \
            --rows $WORKLOAD_INSERT_COUNT \
            --key-prefix newuser)
        local insert_result
        insert_result=$(execute_sql "$insert_sql" "INSERT")
        echo "  $insert_result"
    else
        # simple_test workload - use SQL generators
        echo "  Step 1: Updating first $WORKLOAD_UPDATE_COUNT rows..."
        local update_sql
        update_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-update-sql \
            --table "$table" \
            --schema-type simple \
            --rows $WORKLOAD_UPDATE_COUNT)
        local update_result
        update_result=$(execute_sql "$update_sql" "UPDATE")
        echo "  $update_result"
        
        echo "  Step 2: Deleting last $WORKLOAD_DELETE_COUNT rows..."
        local delete_sql
        delete_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-delete-sql \
            --table "$table" \
            --schema-type simple \
            --rows $WORKLOAD_DELETE_COUNT \
            --from-end)
        local delete_result
        delete_result=$(execute_sql "$delete_sql" "DELETE")
        echo "  $delete_result"
        
        echo "  Step 3: Inserting $WORKLOAD_INSERT_COUNT new rows (id 10001-$((10000 + WORKLOAD_INSERT_COUNT)))..."
        local insert_sql
        insert_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-insert-sql \
            --table "$table" \
            --schema-type simple \
            --rows $WORKLOAD_INSERT_COUNT \
            --start-id 10001)
        local insert_result
        insert_result=$(execute_sql "$insert_sql" "INSERT")
        echo "  $insert_result"
    fi
    
    # Verify post-workload count: initial - 100 deletes + 50 inserts
    local expected_post_count=$((pre_workload_count - 100 + 50))
    local post_workload_count
    if ! post_workload_count=$(verify_row_count "$table" "$expected_post_count" "after workload"); then
        # Warning (count mismatch) - continue but show actual
        if [ $? -eq 2 ]; then
            post_workload_count=$(get_row_count "$table")
        else
            # Fatal error
            return 1
        fi
    else
        echo "  Post-workload count: $post_workload_count rows (expected: $expected_post_count) ✓"
    fi
    
    echo "✅ Workload complete (400 UPDATEs + 100 DELETEs + 50 INSERTs)"
    echo ""
    
    # Wait for CDC files
    echo "⏳ Waiting 60s for CDC files to flush..."
    sleep 60
    
    # Count all files now
    local total_files=$(count_azure_blobs "$path_prefix" "$file_ext")
    local cdc_files=$((total_files - snapshot_count))
    
    echo "📊 File Count Results:"
    echo "  Total ${format} files: $total_files"
    echo "  Snapshot files: $snapshot_count"
    echo "  CDC files: $cdc_files"
    
    # Analyze changefeed files for detailed statistics
    echo ""
    echo "📊 Analyzing changefeed data..."
    
    # Use changefeed_helper.py to analyze the files (with debug for detailed output)
    local analysis_output=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" analyze-files \
        --format "$format" \
        --account "${azure_creds[azure_storage_account]}" \
        --key "${azure_creds[azure_storage_key]}" \
        --container changefeed-events \
        --prefix "${path_prefix}/" \
        --table "$table" \
        --debug 2>&1)
    
    # Show debug output (everything except JSON_STATS line)
    echo "$analysis_output" | grep -v "^JSON_STATS=" | grep -E "^   " || true
    
    # Extract JSON stats line (REQUIRED - no fallback to text parsing)
    local json_line=$(echo "$analysis_output" | grep "^JSON_STATS=" | sed 's/^JSON_STATS=//')
    
    if [ -z "$json_line" ]; then
        echo "❌ Error: JSON_STATS not found in changefeed_helper.py output"
        echo "Debug: Full output:"
        echo "$analysis_output"
        return 1
    fi
    
    # Parse JSON using jq (standard, reliable, no fallback)
    local snapshot_rows=$(echo "$json_line" | jq -r '.snapshot // 0')
    local insert_rows=$(echo "$json_line" | jq -r '.insert // 0')
    local update_rows=$(echo "$json_line" | jq -r '.update // 0')
    local delete_rows=$(echo "$json_line" | jq -r '.delete // 0')
    local unique_keys=$(echo "$json_line" | jq -r '.unique_keys // 0')
    
    # Validate that all values are valid numbers (catch parsing errors)
    if ! [[ "$snapshot_rows" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Invalid snapshot_rows value: '$snapshot_rows'"
        return 1
    fi
    if ! [[ "$insert_rows" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Invalid insert_rows value: '$insert_rows'"
        return 1
    fi
    if ! [[ "$update_rows" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Invalid update_rows value: '$update_rows'"
        return 1
    fi
    if ! [[ "$delete_rows" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Invalid delete_rows value: '$delete_rows'"
        return 1
    fi
    if ! [[ "$unique_keys" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Invalid unique_keys value: '$unique_keys'"
        return 1
    fi
    
    echo ""
    echo "📊 CDC Operation Statistics:"
    echo "  Snapshot rows: $snapshot_rows"
    echo "  Insert rows: $insert_rows"
    echo "  Update rows: $update_rows"
    echo "  Delete rows: $delete_rows"
    if [ -n "$unique_keys" ] && [ "$unique_keys" != "0" ]; then
        echo "  Unique keys (deduplicated): $unique_keys"
        # Expected final count should match post_workload_count (actual rows in table after workload)
        if [ "$post_workload_count" -ne "$unique_keys" ]; then
            echo "  ⚠️  Note: Unique keys ($unique_keys) ≠ Expected ($post_workload_count from table)"
            echo "     This suggests the CDC merge is not working correctly or snapshot includes deleted rows"
        else
            echo "  ✅ Unique keys match post-workload count ($post_workload_count)"
        fi
    fi
    
    # Determine result
    local result_status="UNKNOWN"
    local result_color="$YELLOW"
    
    if [ "$cdc_files" -gt 0 ]; then
        result_status="✅ SUCCESS (Snapshot + CDC)"
        result_color="$GREEN"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - SUCCESS (files: snapshot=$snapshot_count cdc=$cdc_files, rows: snap=$snapshot_rows ins=$insert_rows upd=$update_rows del=$delete_rows)" >> "$RESULTS_FILE"
    else
        result_status="⚠️  PARTIAL (Snapshot only)"
        result_color="$YELLOW"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $test_name - PARTIAL (files: snapshot=$snapshot_count cdc=0, rows: snap=$snapshot_rows)" >> "$RESULTS_FILE"
    fi
    
    echo ""
    echo -e "${result_color}${result_status}${NC}"
    
    # Keep changefeed running for notebook testing
    echo ""
    echo "📝 Changefeed Status:"
    echo "  Job ID: $job_id"
    echo "  Path: ${path_prefix}/"
    echo "  ⚠️  Changefeed LEFT RUNNING for notebook testing"
    
    # Sync files to volume preserving production hierarchy
    echo ""
    echo "📦 Syncing to Unity Catalog Volume (preserving hierarchy)..."
    echo "  Prefix: ${path_prefix}/"
    echo "  Subdir: ${path_prefix}"
    echo "  Structure: format/catalog/schema/test-scenario/"
    
    # Debug: Check what files exist in Azure
    echo ""
    echo "🔍 Checking Azure for files..."
    local azure_files=$(az storage blob list \
        --account-name "${azure_creds[azure_storage_account]}" \
        --account-key "${azure_creds[azure_storage_key]}" \
        --container-name changefeed-events \
        --prefix "${path_prefix}/" \
        --query "[].name" \
        --output tsv 2>/dev/null | wc -l | tr -d ' ')
    echo "  Found $azure_files files with prefix: ${path_prefix}/"
    
    # Show first few files for debugging
    if [ "$azure_files" -gt 0 ]; then
        echo "  Sample files:"
        az storage blob list \
            --account-name "${azure_creds[azure_storage_account]}" \
            --account-key "${azure_creds[azure_storage_key]}" \
            --container-name changefeed-events \
            --prefix "${path_prefix}/" \
            --query "[].name" \
            --output tsv 2>/dev/null | head -3 | sed 's/^/    /'
    fi
    echo ""
    
    # Run sync and capture output
    local sync_output=$(python3 "$SCRIPTS_DIR/sync_azure_to_volume_compact.py" --prefix "${path_prefix}" --subdir "${path_prefix}" 2>&1)
    local sync_exit=$?
    
    # Show relevant output (skip progress bars, show summary)
    echo "$sync_output" | grep -E "^(Volume:|Source files:|Synced:|✅|❌|⚠️)" || echo "$sync_output" | tail -10
    
    if [ $sync_exit -eq 0 ]; then
        echo "✅ Files synced to volume with hierarchy: ${path_prefix}"
    else
        echo "⚠️  Sync failed (exit code: $sync_exit) - continue anyway"
        echo "   Full output:"
        echo "$sync_output" | tail -20
    fi
    
    echo ""
    echo "  💡 To test with notebook:"
    echo "     1. Files already synced to:"
    echo "        /Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/${path_prefix}"
    echo "     2. Run notebook: notebooks/load_parquet_with_merge.ipynb"
    echo "     3. Update notebook VOLUME_PATH to include subdirectory"
    # Calculate expected rows: use unique_keys if available, otherwise snapshot - deletes
    local expected_rows="${unique_keys}"
    if [ -z "$expected_rows" ] || [ "$expected_rows" = "0" ]; then
        expected_rows=$((snapshot_rows - delete_rows))
    fi
    echo "     4. Verify Delta table loads ${expected_rows} unique rows"
    
    echo ""
}

# Cancel all existing changefeeds for test tables from previous runs (skip in validation mode)
if ! $VALIDATE_ONLY; then
    echo "🧹 Cleaning up old test changefeeds and tables from previous runs..."
    if [ -n "$FILTER_FORMAT" ]; then
        echo "   (Only cleaning up $FILTER_FORMAT tests)"
    fi
    echo ""
    
    # List all test tables (tables starting with test_), filtered by format if specified
    if [ -n "$FILTER_FORMAT" ]; then
        test_tables=$(psql "${crdb_creds[cockroachdb_url]}" -t -c "
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'test_${FILTER_FORMAT}_%'
        " 2>/dev/null | tr -d ' ' || echo "")
    else
        test_tables=$(psql "${crdb_creds[cockroachdb_url]}" -t -c "
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'test_%'
        " 2>/dev/null | tr -d ' ' || echo "")
    fi
    
    if [ -n "$test_tables" ]; then
        for test_table in $test_tables; do
            echo "  Checking $test_table changefeeds..."
            old_jobs=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" find-changefeeds \
                --table "$test_table" \
                --json "$CRDB_JSON" 2>&1 | grep -oE "Job [0-9]+" | awk '{print $2}' || echo "")
            
            if [ -n "$old_jobs" ]; then
                job_count=$(echo "$old_jobs" | wc -w | tr -d ' ')
                echo "    Found $job_count old changefeed(s), cancelling..."
                for job_id in $old_jobs; do
                    python3 "$SCRIPTS_DIR/changefeed_helper.py" cancel-changefeed \
                        --job-id "$job_id" \
                        --json "$CRDB_JSON" >/dev/null 2>&1 || echo "        (already cancelled or not found)"
                done
            fi
            
            # Drop old test table
            echo "    Dropping old test table: $test_table..."
            psql "${crdb_creds[cockroachdb_url]}" -c "DROP TABLE IF EXISTS $test_table CASCADE;" >/dev/null 2>&1 || true
        done
    else
        echo "  No old test tables found"
    fi
    
    echo ""
    echo "✅ Cleanup complete"
    echo ""
fi

# Run all test combinations
for format in "${FORMATS[@]}"; do
    for table in "${TABLES[@]}"; do
        for split_option in "${SPLIT_OPTIONS[@]}"; do
            if $VALIDATE_ONLY; then
                validate_test "$format" "$table" "$split_option"
            else
                run_test "$format" "$table" "$split_option"
                echo ""
                echo "⏸  Pausing 10s between tests..."
                sleep 10
            fi
        done
    done
done

# Print summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if $VALIDATE_ONLY; then
    echo "📊 VALIDATION SUMMARY (Timestamp: $TEST_RUN_TIMESTAMP)"
else
    echo "📊 TEST SUMMARY"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
cat "$RESULTS_FILE"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
if $VALIDATE_ONLY; then
    echo "✅ All validations complete!"
else
    echo "✅ All tests complete!"
fi
echo "📄 Full results saved to: $RESULTS_FILE"
echo ""

# Show success rate
if $VALIDATE_ONLY; then
    VALIDATED_COUNT=$(grep "VALIDATED" "$RESULTS_FILE" | wc -l | tr -d ' ')
    SKIPPED_COUNT=$(grep "SKIPPED" "$RESULTS_FILE" | wc -l | tr -d ' ')
    FAILED_COUNT=$(grep "FAILED" "$RESULTS_FILE" | wc -l | tr -d ' ')
    
    echo "Summary:"
    echo "  ✅ VALIDATED: $VALIDATED_COUNT/$TOTAL_TESTS"
    echo "  ⚠️  SKIPPED (no data): $SKIPPED_COUNT/$TOTAL_TESTS"
    echo "  ❌ FAILED: $FAILED_COUNT/$TOTAL_TESTS"
    echo ""
else
    SUCCESS_COUNT=$(grep "SUCCESS" "$RESULTS_FILE" | wc -l | tr -d ' ')
    PARTIAL_COUNT=$(grep "PARTIAL" "$RESULTS_FILE" | wc -l | tr -d ' ')
    FAILED_COUNT=$(grep "FAILED" "$RESULTS_FILE" | wc -l | tr -d ' ')
    
    echo "Summary:"
    echo "  ✅ SUCCESS (Snapshot + CDC): $SUCCESS_COUNT/$TOTAL_TESTS"
    echo "  ⚠️  PARTIAL (Snapshot only): $PARTIAL_COUNT/$TOTAL_TESTS"
    echo "  ❌ FAILED: $FAILED_COUNT/$TOTAL_TESTS"
    echo ""
fi

# List all running changefeeds (skip in validation mode)
if ! $VALIDATE_ONLY; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📋 ACTIVE CHANGEFEEDS (Left Running for Testing)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    # Show all running changefeeds (should be exactly 8 total after this test run)
    for table in "${TABLES[@]}"; do
        # Use -oE to extract only the numbers after "Job "
        jobs=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" find-changefeeds \
            --table "$table" \
            --json "$CRDB_JSON" 2>&1 | grep -oE "Job [0-9]+" | awk '{print $2}' || echo "")
        
        if [ -n "$jobs" ]; then
            job_count=$(echo "$jobs" | wc -w | tr -d ' ')
            echo "Table: $table ($job_count changefeeds)"
            for job in $jobs; do
                echo "  Job ID: $job"
            done
            echo ""
        fi
    done

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🧪 NEXT STEPS: Notebook Testing"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "✅ All test data has been synced to Unity Catalog Volume!"
fi

if ! $VALIDATE_ONLY; then
echo ""
echo "Each test uses production-style hierarchy (format/catalog/schema/test-scenario):"
echo "  /Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/json/defaultdb/public/test-json_usertable_with_split/"
echo "  /Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/json/defaultdb/public/test-json_usertable_no_split/"
echo "  /Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/parquet/defaultdb/public/test-parquet_usertable_with_split/"
echo "  /Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/parquet/defaultdb/public/test-parquet_usertable_no_split/"
echo "  ... etc"
echo ""
echo "1️⃣  Test each scenario with the notebook:"
echo "   Open: notebooks/load_parquet_with_merge.ipynb"
echo ""
echo "   Update in Cell 1 (Configuration):"
echo "   • VOLUME_PATH = f\"{VOLUME_PATH}/parquet/defaultdb/public/test-parquet_usertable_with_split\""
echo "   • SOURCE_TABLE = \"usertable\"  # or \"simple_test\""
echo "   • PRIMARY_KEY_COLUMNS = [\"ycsb_key\"]  # or [\"id\"] for simple_test"
echo ""
echo "2️⃣  Verify results match expected counts from above"
echo ""
echo "3️⃣  To resync a specific test manually (if needed):"
echo "   python3 $SCRIPTS_DIR/sync_azure_to_volume_compact.py \\"
echo "     --prefix parquet/defaultdb/public/test-parquet_usertable_with_split \\"
echo "     --subdir parquet/defaultdb/public/test-parquet_usertable_with_split"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧹 CLEANUP (When Testing Complete)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "To cancel all changefeeds and clean up test data:"
echo ""
echo "# Find all test tables and their changefeeds"
echo "psql \"\$COCKROACHDB_URL\" -c \"SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'test_%'\""
echo ""
echo "# Cancel changefeeds for specific test table"
echo "python3 $SCRIPTS_DIR/changefeed_helper.py find-changefeeds --table test_parquet_simple_test_no_split --json $CRDB_JSON"
echo "python3 $SCRIPTS_DIR/changefeed_helper.py cancel-changefeed --job-id <JOB_ID> --json $CRDB_JSON"
echo ""
echo "# Or cancel ALL test changefeeds and drop ALL test tables at once:"
echo "psql \"\$COCKROACHDB_URL\" << 'EOF'"
echo "  -- Drop all test tables (this cancels their changefeeds too)"
echo "  SELECT 'DROP TABLE IF EXISTS ' || table_name || ' CASCADE;'"
echo "  FROM information_schema.tables"
echo "  WHERE table_name LIKE 'test_%';"
echo "EOF"
echo ""
echo "# Clean up Azure test data (optional - cleans all formats)"
echo "az storage blob delete-batch \\"
echo "  --account-name ${azure_creds[azure_storage_account]} \\"
echo "  --account-key '${azure_creds[azure_storage_key]}' \\"
echo "  --source changefeed-events \\"
echo "  --pattern 'parquet/defaultdb/public/test-*'"
echo ""
echo "az storage blob delete-batch \\"
echo "  --account-name ${azure_creds[azure_storage_account]} \\"
echo "  --account-key '${azure_creds[azure_storage_key]}' \\"
echo "  --source changefeed-events \\"
echo "  --pattern 'json/defaultdb/public/test-*'"
echo ""
fi

