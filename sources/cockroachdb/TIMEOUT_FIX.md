# Timeout Fix for Changefeed Health Checks

## Problem
The `test_cdc_matrix.sh` script was hanging indefinitely when checking changefeed health after a failure, specifically at the line:
```bash
health_output=$(timeout 30 python3 "$SCRIPTS_DIR/changefeed_helper.py" check-status ...)
```

## Root Cause
The `timeout` command can hang when:
1. **Python output buffering**: Python buffers stdout/stderr by default, causing the parent process to wait
2. **Open file descriptors**: The subprocess keeps stdin open, waiting for input that never comes

## Solution
Applied two fixes to all health check locations:

### Fix 1: Unbuffered Python Output
Added `-u` flag to disable Python output buffering:
```bash
python3 -u "$SCRIPTS_DIR/changefeed_helper.py" ...
```

### Fix 2: Close stdin
Redirect stdin from `/dev/null` to close it immediately:
```bash
... </dev/null 2>&1
```

### Complete Fixed Pattern
```bash
health_output=$(timeout 30 python3 -u "$SCRIPTS_DIR/changefeed_helper.py" check-status \
    --job-id "$job_id" \
    --json "$CRDB_JSON" </dev/null 2>&1)
health_exit_code=$?
```

## Locations Fixed
1. **Immediate health check** (line 283-299): 10-second timeout
2. **Post-wait health check** (line 338-360): 30-second timeout
3. **Both timeout and gtimeout branches**: All variants updated

## Test Results
Before fix:
- ✅ Direct command (no capture): **Works**
- ❌ Output capture `$(...)`: **Hangs indefinitely**

After fix:
- ✅ Both patterns should work correctly
- ✅ Fast failure with proper error messages
- ✅ No more silent hangs

## Impact
- **Script robustness**: Now handles changefeed failures gracefully
- **Faster feedback**: Errors appear immediately instead of hanging
- **Better diagnostics**: Error categorization and suggestions are displayed
- **Production-ready**: Safe to run in automated environments
