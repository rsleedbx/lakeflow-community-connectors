# Shared Cancel Changefeed Job Module

## Overview

The cancel changefeed job logic has been extracted into a shared Python module to avoid code duplication between scripts.

**Module**: `cancel_changefeed_job.py`

**Used by**:
- `cancel_job.sh` - Standalone script to cancel a specific job
- `test_azure_cdc.sh` - CDC test script that auto-cancels errored changefeeds

---

## Module API

### `cancel_job(conn_url, job_id, max_attempts=10, poll_interval=3, verbose=True)`

Cancels a CockroachDB changefeed job and waits for confirmation.

**Parameters:**
- `conn_url` (str): PostgreSQL connection URL
- `job_id` (int): Job ID to cancel
- `max_attempts` (int): Maximum polling attempts (default: 10)
- `poll_interval` (int): Seconds between polls (default: 3)
- `verbose` (bool): Print progress messages (default: True)

**Returns:**
```python
{
    'success': bool,           # True if cancelled successfully
    'final_status': str,       # 'canceled', 'removed', 'failed', etc.
    'message': str             # Human-readable message
}
```

**Example:**
```python
import cancel_changefeed_job

result = cancel_changefeed_job.cancel_job(
    conn_url="postgresql://user:pass@host:26257/db?sslmode=require",
    job_id=1134081029864718337,
    max_attempts=5,
    poll_interval=3,
    verbose=False
)

if result['success']:
    print(f"✅ {result['message']}")
else:
    print(f"⚠️  {result['message']}")
```

### `get_connection(conn_url)`

Creates a pg8000 connection to CockroachDB with SSL.

**Parameters:**
- `conn_url` (str): PostgreSQL connection URL

**Returns:** `pg8000.Connection`

### `get_job_status(cursor, job_id)`

Gets the current status of a job.

**Parameters:**
- `cursor`: Database cursor
- `job_id` (int): Job ID to query

**Returns:** `tuple` of (job_id, status, running_status) or `None` if not found

---

## CLI Usage

The module can be run standalone:

```bash
# Set connection URL
export COCKROACHDB_URL="postgresql://user:pass@host:26257/db?sslmode=require"

# Cancel a job
python3 cancel_changefeed_job.py 1134081029864718337
```

**Output:**
```
🔧 Canceling CockroachDB Job: 1134081029864718337

Job 1134081029864718337: Current status = running
  Running status: transient error: closing object...
  Issuing CANCEL command...
  ✅ CANCEL command issued
  ⏳ Polling for completion (every 3s, max 10 attempts)...
    [1/10] Status: cancel-requested
    [2/10] Status: cancel-requested
    [3/10] Status: canceled
  ✅ Job successfully cancelled

✅ Job successfully cancelled
   Final status: canceled
```

---

## Script Integration

### `cancel_job.sh`

**Before (inline Python):** 147 lines

**After (using module):** 32 lines

```bash
#!/bin/bash
# ... setup code ...

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Use the shared Python module
python3 "$SCRIPT_DIR/cancel_changefeed_job.py" "$JOB_ID"
```

### `test_azure_cdc.sh`

**Before (inline Python):** 50+ lines of cancel logic

**After (using module):** Import and call function

```python
import sys, os
sys.path.insert(0, '$SCRIPT_DIR')
import cancel_changefeed_job

# Cancel errored job
result = cancel_changefeed_job.cancel_job(
    conn_url, 
    job_id, 
    max_attempts=5, 
    poll_interval=3, 
    verbose=False
)

if result['success']:
    print(f"      ✅ Cancelled (status: {result['final_status']})")
else:
    print(f"      ⚠️  {result['message']}")
```

---

## Polling Logic

The module polls the job status until one of these conditions:

| Condition | Status | Result |
|-----------|--------|--------|
| Job not found | N/A | ✅ Success (removed) |
| Status = `canceled` | Final | ✅ Success |
| Status = `cancelled` | Final | ✅ Success |
| Status = `failed` | Final | ✅ Success (no longer running) |
| Timeout reached | Any | ⚠️  Warning (still processing) |

**Default timeout:** 30 seconds (10 attempts × 3 seconds)

---

## Benefits of Shared Module

### 1. **No Code Duplication**
- Single source of truth for cancel logic
- Bug fixes apply to all users automatically

### 2. **Easier Maintenance**
- Update polling logic in one place
- Add features once, benefit everywhere

### 3. **Consistent Behavior**
- Same polling strategy across all scripts
- Same error handling

### 4. **Testable**
- Can unit test the module independently
- Can mock for testing scripts

### 5. **Reusable**
- Any future script can import and use
- Can be used programmatically

---

## File Sizes

| File | Lines | Description |
|------|-------|-------------|
| `cancel_changefeed_job.py` | 202 | Shared module |
| `cancel_job.sh` | 32 | Standalone cancel script |
| `test_azure_cdc.sh` | 361 | CDC test script |

**Code saved:** ~115 lines of duplicated Python code eliminated!

---

## Testing

### Test the module directly:
```bash
cd sources/cockroachdb/scripts
source ../.env/cockroachdb_cockroachcloud.env

python3 cancel_changefeed_job.py 1134081029864718337
```

### Test via cancel_job.sh:
```bash
cd sources/cockroachdb/scripts
./cancel_job.sh 1134081029864718337
```

### Test via test_azure_cdc.sh:
```bash
cd sources/cockroachdb/scripts
./test_azure_cdc.sh  # Will auto-cancel errored jobs
```

---

## Future Enhancements

Potential additions to the module:

1. **Batch cancellation:** Cancel multiple jobs at once
2. **Filter by criteria:** Cancel all errored changefeeds
3. **Status monitoring:** Watch job status over time
4. **Export metrics:** Track cancellation success rate
5. **Retry logic:** Automatic retry on temporary failures

---

**Date**: December 22, 2025  
**Status**: ✅ Complete - Shared module implemented and integrated



