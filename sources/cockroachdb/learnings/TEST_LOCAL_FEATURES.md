# test_local.py New Features

## Overview

`test_local.py` now includes automatic process cleanup, optional diagnostic tests, and better hang detection.

## Quick Start

```bash
# Simple - just run it!
python test_local.py

# If tests are hanging, run diagnostic first:
python test_local.py --diagnostic
```

## New Features

### 1. 🧹 Automatic Process Cleanup

**Problem Solved:** Previously, if tests hung or crashed, you'd have leftover processes that would interfere with the next run.

**Solution:** The script now automatically kills existing processes before starting:

```bash
python test_local.py
# Output:
🧹 Cleaning up existing processes...
   Killed existing test_local.py (PID: 12345)
   Killed existing generate_test_data.sh (PID: 67890)
```

**How it works:**
- Uses `pgrep` to find existing `test_local.py` and `generate_test_data.sh` processes
- Sends `SIGTERM` to gracefully terminate them
- Excludes the current process (won't kill itself!)
- Waits 1 second for processes to terminate

**Disable if needed:**
```bash
python test_local.py --no-cleanup
```

### 2. 🔍 Diagnostic Mode

**Problem Solved:** When tests hang, it's hard to tell if the issue is in the connector or CockroachDB itself.

**Solution:** Run a diagnostic test first to check if changefeeds work at all:

```bash
python test_local.py --diagnostic
```

**What it does:**
1. Runs `test_changefeed_direct.py` (bypasses the connector)
2. Tests if CockroachDB changefeeds return data
3. Has a 15-second timeout (will fail fast if hung)
4. Shows detailed output for debugging
5. Continues with full tests even if diagnostic fails

**Example output:**
```
============================================================
Running Diagnostic Test
============================================================
Testing if changefeeds work at all...

Direct CockroachDB Changefeed Test
...
✅ Changefeed is working correctly!

✅ Diagnostic test passed! Proceeding with full tests...
```

### 3. ⚡ Better Hang Detection

**Changes made:**
- `initial_scan='only'` instead of `'yes'` (stops after existing rows, doesn't stream)
- `resolved='1s'` instead of `'5s'` (faster checkpoints)
- Threading-based timeouts (30 seconds) instead of signal-based
- Ctrl+C works immediately (no more hung processes)

### 4. 📊 Enhanced Output

**Before:**
```
📖 Reading data from 'events' (batch size: 5)...
[hangs forever]
```

**After:**
```
📖 Reading data from 'events' (batch size: 5)...
   (Using initial_scan to include existing rows)
✅ Read 5 records
   End offset: {...}
```

## Command-Line Options

### All Options

```bash
python test_local.py --help
```

### Option Reference

| Option | Default | Description |
|--------|---------|-------------|
| (none) | - | Run with all defaults (recommended) |
| `--duration N` | 120 | Generate test data for N seconds |
| `--no-data` | off | Skip data generation (tests will timeout) |
| `--diagnostic` | off | Run changefeed diagnostic before full tests |
| `--no-cleanup` | off | Don't kill existing processes |

### Common Scenarios

```bash
# Quick 30-second test
python test_local.py --duration 30

# Tests are hanging? Run diagnostic first
python test_local.py --diagnostic

# Debugging connector without changefeeds
python test_local.py --no-data

# Multiple test runs in parallel (advanced)
python test_local.py --no-cleanup --duration 30 &
python test_local.py --no-cleanup --duration 30 &
```

## Troubleshooting Workflow

### Tests hang at "📖 Reading data..."

**Step 1: Run diagnostic**
```bash
python test_local.py --diagnostic
```

If diagnostic passes → Issue is in the connector code
If diagnostic fails → Issue is in CockroachDB setup

**Step 2: Check setup**
```bash
./check_setup.sh
```

Verifies:
- CockroachDB is running
- Rangefeeds are enabled
- Tables exist and have data

**Step 3: Read debugging guide**
```bash
cat DEBUGGING_HANGS.md
```

**Step 4: Enable debug mode**
```bash
export DEBUG_CHANGEFEED=1
python test_local.py --diagnostic
```

Shows the exact SQL queries being executed.

## Implementation Details

### Process Cleanup Algorithm

```python
def cleanup_existing_processes():
    1. Find all processes matching "python.*test_local.py"
    2. Exclude current process (by PID)
    3. Send SIGTERM to each
    4. Find all processes matching "generate_test_data.sh"
    5. Send SIGTERM to each
    6. Wait 1 second for graceful shutdown
    7. Continue (don't fail if cleanup fails)
```

**Why this is safe:**
- Only kills processes with matching names
- Excludes the current process
- Non-blocking (doesn't wait for termination)
- Graceful (uses SIGTERM, not SIGKILL)
- Failure-tolerant (catches exceptions)

### Diagnostic Test Flow

```
test_local.py --diagnostic
    ↓
Cleanup existing processes
    ↓
Run test_changefeed_direct.py (subprocess, 15s timeout)
    ↓
test_changefeed_direct.py:
  - Connect to CockroachDB
  - Check table has data
  - Run changefeed with initial_scan='yes'
  - Set 10s timeout (SIGALRM)
  - Fetch one row
  - Parse and display
    ↓
If success → Proceed with full tests
If failure → Show warning, proceed anyway
If timeout → Show error, proceed anyway
```

### Ctrl+C Handling

Works at any point:
- During process cleanup ✅
- During diagnostic test ✅
- During data generator startup ✅
- During changefeed reads ✅
- During data generator shutdown ✅

Always cleans up background processes in `finally` block.

## Files Changed

```
sources/cockroachdb/
├── test_local.py ✏️ (+100 lines)
│   • cleanup_existing_processes()
│   • run_diagnostic_test()
│   • --diagnostic flag
│   • --no-cleanup flag
│   • Auto-cleanup at startup
│
├── README.md ✏️
│   • Updated "Testing Options" section
│   • Added "New Features" subsection
│
├── test_changefeed_direct.py ⭐ NEW
│   • Standalone diagnostic tool
│   • 10-second timeout
│   • Detailed changefeed analysis
│
├── DEBUGGING_HANGS.md ⭐ NEW
│   • 5 common issues & solutions
│   • Debug mode instructions
│   • Workarounds for changefeed problems
│
├── TEST_LOCAL_FEATURES.md ⭐ NEW (this file)
│   • Complete feature documentation
│   • Usage examples
│   • Troubleshooting workflow
│
└── CTRL_C_FIX.md ✅ (existing)
    • Explains threading-based timeout fix
```

## Migration Guide

### If you have scripts that call test_local.py

**Old way:**
```bash
# Had to manually cleanup first
pkill -f test_local.py
pkill -f generate_test_data.sh
python test_local.py
```

**New way:**
```bash
# Cleanup happens automatically
python test_local.py
```

### If you want the old behavior

```bash
# Disable auto-cleanup
python test_local.py --no-cleanup
```

## Performance Impact

- **Cleanup**: ~0.5s (two pgrep calls + SIGTERM)
- **Diagnostic**: ~2-5s (if changefeed works) or 15s (if it times out)
- **Overall**: Negligible impact, huge UX improvement

## Future Enhancements

Possible additions:
- `--quick` mode: Skip data generation, use existing data only
- `--verbose` mode: Show all SQL queries
- `--retry N`: Retry failed tests N times
- `--save-logs`: Save output to file for debugging
- Integration with CI/CD systems

## Credits

These features address the common issues reported by users:
- Hung processes blocking new test runs
- No clear error messages when changefeeds fail
- Ctrl+C not working reliably
- Long waits before realizing tests are hung

