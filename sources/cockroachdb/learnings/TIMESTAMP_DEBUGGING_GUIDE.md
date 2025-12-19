# Timestamp Debugging Guide: Verify Changefeed Retrieval

## Overview

This guide shows how to use timestamp information in logs to debug and verify that changefeeds are being retrieved correctly.

---

## New Debug Information in Logs

### 1. CockroachDB Timestamps (Start of Run)

```
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766108732901528329.0000000000
   Current wall clock time: 2025-12-19 01:45:33.478965+00
   Cursor (resume from): 1766105060072019839.0000000000
   💡 Will fetch changes after cursor timestamp
```

**What this tells you:**
- **Current cluster timestamp**: CockroachDB's internal MVCC timestamp (nanoseconds since epoch)
- **Current wall clock time**: Human-readable timestamp
- **Cursor**: Starting point for this run (where we left off last time)

---

### 2. Cursor Tracking (End of Run)

```
🕐 Cursor Tracking:
   Start cursor: 1766105060072019839.0000000000
   End cursor: 1766108732901528329.0000000000
   ✅ Cursor advanced (changes processed)
```

**Or if no changes:**
```
🕐 Cursor Tracking:
   Start cursor: 1766105060072019839.0000000000
   End cursor: 1766105060072019839.0000000000
   ✅ Cursor unchanged (no new changes found)
```

**What this tells you:**
- **Start cursor == End cursor**: No changes detected (expected when idle)
- **Start cursor < End cursor**: Changes were processed
- **No start cursor (None)**: First run (snapshot mode)

---

### 3. Manual Test Command (End of Logs)

```
💡 Manual Test (verify changefeed retrieval):
   psql $COCKROACHDB_URL << 'SQL'
   EXPERIMENTAL CHANGEFEED FOR usertable
   WITH initial_scan='no', updated, resolved='1s',
        split_column_families, cursor='1766108732901528329.0000000000';
   SQL
   Expected: Timeout (~5s) if no changes, or immediate data if changes exist
```

**How to use:**
Copy and paste this command to manually verify if there are changes since the cursor.

---

## Debugging Scenarios

### Scenario 1: Pipeline Says "No Changes" But You Expect Changes

**Symptoms:**
```
End offset: {'cursor': '1766105060072019839.0000000000'}
Total events returned: 0
🕐 Cursor Tracking:
   Start cursor: 1766105060072019839.0000000000
   End cursor: 1766105060072019839.0000000000
   ✅ Cursor unchanged (no new changes found)
```

**Debugging Steps:**

**Step 1: Check cursor age**
```bash
cd sources/cockroachdb
./scripts/check_changefeed_cursor.sh '1766105060072019839.0000000000'
```

**Output interpretation:**
```
Cursor age: 3672 seconds (~61 minutes)
❌ TIMEOUT: No changes detected
```
→ Confirmed: No changes in the last 61 minutes ✅

```
Cursor age: 120 seconds (~2 minutes)
⚠️  CHANGES DETECTED: Changefeed returned data
```
→ **Problem!** There ARE changes but pipeline didn't process them ❌

**Step 2: Manual changefeed test**
```bash
export COCKROACHDB_URL="postgresql://..."

# Copy the command from pipeline logs
psql $COCKROACHDB_URL << 'SQL'
EXPERIMENTAL CHANGEFEED FOR usertable
WITH initial_scan='no', updated, resolved='1s',
     split_column_families, cursor='1766105060072019839.0000000000';
SQL
```

**What to look for:**
- Immediate data → Changes exist (pipeline issue)
- Timeout after ~10s → No changes (expected)
- Error message → Connection or permission issue

---

### Scenario 2: Cursor Not Advancing

**Symptoms:**
```
Run 1: End cursor: 1766105060072019839.0000000000
Run 2: End cursor: 1766105060072019839.0000000000
Run 3: End cursor: 1766105060072019839.0000000000
```

**Possible causes:**

**A) No changes (expected):**
```bash
# Check if there really are no changes
./scripts/check_changefeed_cursor.sh '1766105060072019839.0000000000'

# If timeout → No changes, cursor should stay same
```

**B) Cursor not being saved:**
```
# Check pipeline logs for:
End offset: {}  ❌ BAD (empty)
End offset: {'cursor': '...'} ✅ GOOD
```

**C) Pipeline restarting from wrong offset:**
```
# Check start_offset in next run:
start_offset: {} ❌ Should have cursor
start_offset: {'cursor': '...'} ✅ Correct
```

---

### Scenario 3: Changes Generated But Not Captured

**Test case:**
```bash
# Generate changes
cockroach workload run ycsb $COCKROACHDB_URL --duration 1m

# Immediately check with cursor
./scripts/check_changefeed_cursor.sh '<cursor_from_last_run>'

# Expected: Should detect changes
```

**If no changes detected:**
1. Check if cursor is too NEW (after the changes)
2. Check if changes are in different table
3. Check if changes are in different database/schema

---

## Timestamp Comparison Math

### Understanding CockroachDB Timestamps

Format: `1766105060072019839.0000000000`
- Part 1: `1766105060072019839` = Nanoseconds since epoch
- Part 2: `.0000000000` = Logical clock component

### Calculate Age

```bash
# From logs
START_CURSOR="1766105060072019839"
END_CURSOR="1766108732901528329"

# Age in nanoseconds
AGE_NS=$((END_CURSOR - START_CURSOR))

# Convert to seconds
AGE_SEC=$((AGE_NS / 1000000000))

# Convert to minutes
AGE_MIN=$((AGE_SEC / 60))

echo "Age: $AGE_SEC seconds ($AGE_MIN minutes)"
```

**Example:**
```
Age: 3672 seconds (61 minutes)
```

---

## Quick Reference Commands

### Check Current CockroachDB Timestamp
```bash
export COCKROACHDB_URL="postgresql://..."
psql $COCKROACHDB_URL -c "SELECT cluster_logical_timestamp()::string, now()::string;"
```

### Check for Changes Since Cursor
```bash
./scripts/check_changefeed_cursor.sh '<cursor>'
```

### Test Changefeed Manually
```bash
psql $COCKROACHDB_URL << 'SQL'
EXPERIMENTAL CHANGEFEED FOR usertable
WITH initial_scan='no', updated, resolved='1s',
     split_column_families, cursor='<YOUR_CURSOR>';
SQL
```

### Generate Test Changes
```bash
cockroach workload run ycsb $COCKROACHDB_URL --duration 30s
```

---

## Log Examples

### Successful Incremental Run (With Changes)

```
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766108732901528329.0000000000
   Current wall clock time: 2025-12-19 01:45:33.478965+00
   Cursor (resume from): 1766105060072019839.0000000000

⏱️  Step 5: Executing changefeed (may timeout if caught up)...

⏱️  Step 6: Processing changefeed events...
   Processing events: 10000 events...
   ✅ Received resolved timestamp: 1766108732901528329.0000000000

Total events returned: 10000

🕐 Cursor Tracking:
   Start cursor: 1766105060072019839.0000000000
   End cursor: 1766108732901528329.0000000000
   ✅ Cursor advanced (changes processed)
```

---

### Successful Incremental Run (No Changes)

```
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766108732901528329.0000000000
   Current wall clock time: 2025-12-19 01:45:33.478965+00
   Cursor (resume from): 1766108700000000000.0000000000

⏱️  Step 5: Executing changefeed (may timeout if caught up)...

✅ Changefeed query timed out after 5.1s (EXPECTED)
   ℹ️  No changes since cursor → caught up!

Total events returned: 0

🕐 Cursor Tracking:
   Start cursor: 1766108700000000000.0000000000
   End cursor: 1766108700000000000.0000000000
   ✅ Cursor unchanged (no new changes found)
```

---

### First Run (Snapshot Mode)

```
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766108732901528329.0000000000
   Current wall clock time: 2025-12-19 01:45:33.478965+00
   No cursor (first run)

💡 Capturing snapshot start timestamp (from CockroachDB)...
   📍 Snapshot start: 1766108732901528329.0000000000

Total events returned: 10000

🕐 Cursor Tracking:
   Start cursor: None (first run)
   End cursor: 1766108732901528329.0000000000
   ✅ Cursor advanced (changes processed)
```

---

## Troubleshooting Flow Chart

```
Pipeline returns 0 events
         │
         ↓
Check logs: Start cursor == End cursor?
         │
    ┌────┴────┐
    │         │
   YES       NO
    │         │
    ↓         ↓
Expected   Problem!
(no changes) Cursor should
         │   have advanced
         ↓
    Run check_changefeed_cursor.sh
         │
    ┌────┴────┐
    │         │
 TIMEOUT   CHANGES
    │      DETECTED
    ↓         │
 Confirmed    ↓
  No changes  Pipeline bug!
              Check:
              1. Timeout too aggressive?
              2. Error in event processing?
              3. Cursor not being returned?
```

---

## Summary

✅ **Always available in logs:**
- Current CockroachDB timestamp (start of run)
- Start and end cursor (for comparison)
- Manual test command (copy/paste to verify)

✅ **Debugging scripts:**
- `./scripts/check_changefeed_cursor.sh <cursor>` - Check if changes exist
- `./scripts/test_changefeed_timeout.sh` - Test changefeed behavior

✅ **Key indicators:**
- Cursor unchanged → No changes (expected when idle)
- Cursor advanced → Changes processed
- Age calculation → How old is the cursor?

**With these tools, you can quickly verify if changefeeds are being retrieved correctly!** 🎯

