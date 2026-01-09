# Changefeed Error Detection System

## Overview
This document describes the comprehensive error detection and health check system implemented for CockroachDB changefeeds.

## Problem Statement
The original `test_cdc_matrix.sh` had several critical issues:
1. **Silent failures**: Changefeeds would fail (e.g., Azure storage deleted) without any error messages
2. **Script hangs**: Health checks would hang indefinitely instead of timing out
3. **No categorization**: Generic error messages without actionable guidance
4. **No diagnostics**: Difficult to determine root cause of failures

## Solution Architecture

### 1. Error Categorization (`cockroachdb.py`)

Added `_categorize_changefeed_error()` function that parses raw error messages and categorizes them:

**Categories:**
- `auth`: Authentication/authorization failures
- `storage`: Container/blob not found
- `network`: Connectivity issues (transient)
- `config`: Invalid URI or changefeed options
- `permissions`: Access denied (different from auth)
- `rate_limit`: Azure throttling
- `unknown`: Uncategorized errors

**Each category provides:**
- **Description**: Human-readable explanation
- **Retryable**: Whether the operation can be retried
- **Suggestion**: Actionable advice for resolution

Example categorization:
```python
{
    'category': 'network',
    'description': 'Network connectivity issue to Azure Storage.',
    'retryable': True,
    'suggestion': 'Check network connectivity to Azure. This may be a transient error, a retry may succeed.'
}
```

### 2. Enhanced Status Checking

Modified `check_changefeed_status()` to:
- Use `_categorize_changefeed_error()` for all errors
- Return enriched status information including error category
- Distinguish between running-but-errored vs failed states

**Return structure:**
```python
{
    'job_id': int,
    'status': str,                  # 'running', 'failed', 'pending', etc.
    'running_status': str,          # Detailed runtime status
    'error': str,                   # Raw error message
    'is_healthy': bool,             # Overall health flag
    'has_errors': bool,             # Error flag
    'error_category': str,          # Categorized error type
    'error_description': str,       # Human-readable description
    'error_retryable': bool,        # Whether retry is appropriate
    'error_suggestion': str         # Actionable advice
}
```

### 3. CLI Integration (`changefeed_helper.py`)

Enhanced `cmd_check_status` to display:
- Basic status information
- Detailed error analysis section
- Clear visual indicators (✅ for healthy, ❌ for errors)
- Non-zero exit code for failures

**Example output:**
```
  Status: running
  Running Status: transient error: closing object: Put "https://..."

  🔍 Error Analysis:
     Category: network
     Description: Network connectivity issue to Azure Storage.
     Retryable: Yes
     💡 Suggestion: Check network connectivity to Azure. This may be a transient error, a retry may succeed.

  ❌ Changefeed has errors!
```

### 4. Timeout Implementation (`test_cdc_matrix.sh`)

Implemented robust health checks with:
- **10-second timeout**: Immediate health check after creation
- **30-second timeout**: Post-snapshot health check
- **Multiple timeout commands**: Support for `timeout` (Linux) and `gtimeout` (macOS)
- **Stdin redirection**: `</dev/null` to prevent hangs
- **Unbuffered Python**: `-u` flag for immediate output

**Two health check points:**

```bash
# 1. Immediate check (after creation)
🏥 Checking changefeed health...
   Status: running ✓

# 2. Post-wait check (after 30s)
⏳ Waiting 30s for initial snapshot...
🏥 Checking changefeed health after wait...
   [DEBUG] Job ID: 1139286531243409409
   [DEBUG] Config: /path/to/config.json
   [DEBUG] Using timeout command...
   [DEBUG] Command completed with exit code: 1

❌ Changefeed failed during snapshot!
```

### 5. Error Parsing and Display

The test script now:
- Parses structured error output from `changefeed_helper.py`
- Extracts `Error`, `Category`, and `Suggestion` fields
- Displays full job status details
- Records failures in test results file
- Automatically cancels failed changefeeds

**Error detection flow:**
```
1. Run health check with timeout
2. Check exit code (124/142 = timeout)
3. Parse JSON-like output for error fields
4. Display categorized error information
5. Log to results file
6. Cancel changefeed job
7. Continue to next test
```

## Implementation Details

### Timeout Handling

```bash
if command -v timeout >/dev/null 2>&1; then
    health_output=$(timeout 30 python3 -u "$SCRIPTS_DIR/changefeed_helper.py" check-status \
        --job-id "$job_id" \
        --json "$CRDB_JSON" </dev/null 2>&1)
    health_exit_code=$?
elif command -v gtimeout >/dev/null 2>&1; then
    # macOS fallback
    health_output=$(gtimeout 30 python3 -u "$SCRIPTS_DIR/changefeed_helper.py" check-status \
        --job-id "$job_id" \
        --json "$CRDB_JSON" </dev/null 2>&1)
    health_exit_code=$?
else
    # No timeout available - warn user
    echo "   ⚠️  Warning: timeout command not available"
    health_output=$(python3 -u "$SCRIPTS_DIR/changefeed_helper.py" check-status \
        --job-id "$job_id" \
        --json "$CRDB_JSON" </dev/null 2>&1)
    health_exit_code=$?
fi
```

### Error Parsing

```bash
# Check for timeout
if [ $health_exit_code -eq 124 ] || [ $health_exit_code -eq 142 ]; then
    echo "❌ Health check timed out (changefeed may be stuck)"
    return
fi

# Check for errors
if [ $health_exit_code -ne 0 ]; then
    echo "❌ Changefeed failed!"
    echo ""
    echo "Job Status Details:"
    echo "$health_output"
    echo ""
    
    # Parse error fields
    local error_msg=$(echo "$health_output" | grep -E "^\s*Error:" | sed 's/^[[:space:]]*Error:[[:space:]]*//')
    local error_category=$(echo "$health_output" | grep -E "^\s*Category:" | sed 's/^[[:space:]]*Category:[[:space:]]*//')
    local error_suggestion=$(echo "$health_output" | grep -E "^\s*💡 Suggestion:" | sed 's/^[[:space:]]*💡 Suggestion:[[:space:]]*//')
    
    # Display parsed errors
    if [ -n "$error_msg" ]; then echo "Error: $error_msg"; fi
    if [ -n "$error_category" ]; then echo "Category: $error_category"; fi
    if [ -n "$error_suggestion" ]; then echo "💡 $error_suggestion"; fi
fi
```

## Error Categories Reference

| Category | Description | Retryable | Common Causes |
|----------|-------------|-----------|---------------|
| `auth` | Authentication failure | No | Invalid credentials, account key mismatch |
| `storage` | Resource not found | No | Container deleted, incorrect name |
| `network` | Connectivity issue | Yes | Transient network error, "closing object" |
| `config` | Configuration error | No | Malformed URI, unsupported options |
| `permissions` | Access denied | No | Missing IAM roles (e.g., Storage Blob Data Contributor) |
| `rate_limit` | Throttling | Yes | Too many requests, Azure quota exceeded |
| `unknown` | Uncategorized | No | New/unexpected error patterns |

## Benefits

### 1. Fast Failure
- **Before**: Scripts hung indefinitely on errors
- **After**: Fail within 10-30 seconds with clear error messages

### 2. Actionable Diagnostics
- **Before**: Generic "changefeed failed" messages
- **After**: Categorized errors with specific remediation steps

### 3. Automation-Friendly
- **Non-zero exit codes**: Scripts can detect failures programmatically
- **Structured output**: Easy to parse for CI/CD integration
- **Timeout protection**: No infinite loops or hangs

### 4. Developer Experience
- **Clear visual indicators**: Emoji-based status (✅, ❌, 🏥, 💡)
- **Debug information**: Optional verbose output with `[DEBUG]` tags
- **Progressive enhancement**: Works with or without timeout command

## Testing

### Manual Testing
```bash
# Run full test matrix
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/scripts
./test_cdc_matrix.sh

# Test specific health check
python3 -u changefeed_helper.py check-status \
    --job-id 1139286531243409409 \
    --json ../. env/cockroachdb_credentials.json
```

### Expected Behavior

**Healthy Changefeed:**
```
🏥 Checking changefeed health...
   Status: running ✓
```

**Failed Changefeed:**
```
🏥 Checking changefeed health after wait...
   [DEBUG] Job ID: 1139286531243409409
   [DEBUG] Config: /path/to/config.json
   [DEBUG] Using timeout command...
   [DEBUG] Command completed with exit code: 1

❌ Changefeed failed during snapshot!

Job Status Details:
  Status: running
  Running Status: transient error: closing object: Put "https://..."

  🔍 Error Analysis:
     Category: network
     Description: Network connectivity issue to Azure Storage.
     Retryable: Yes
     💡 Suggestion: Check network connectivity to Azure. This may be a transient error, a retry may succeed.
```

**Timeout:**
```
🏥 Checking changefeed health after wait...
   [DEBUG] Using timeout command...
❌ Health check timed out (changefeed may be stuck)
Test 1/8: test_parquet_usertable_no_split - FAILED (health check timeout)
```

## Future Enhancements

1. **Retry Logic**: Automatic retry for retryable errors
2. **Metrics**: Track error frequency by category
3. **Alerting**: Integration with monitoring systems
4. **Historical Analysis**: Log errors for trend analysis
5. **Error Recovery**: Automatic remediation for known issues

## Related Files

- `sources/cockroachdb/cockroachdb.py`: Core error categorization logic
- `sources/cockroachdb/scripts/changefeed_helper.py`: CLI interface
- `sources/cockroachdb/scripts/test_cdc_matrix.sh`: Test matrix with health checks
- `sources/cockroachdb/TIMEOUT_FIX.md`: Detailed timeout implementation guide
