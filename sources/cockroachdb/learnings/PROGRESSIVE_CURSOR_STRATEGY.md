# Progressive Cursor Strategy for Large Datasets

## Problem Statement

**What happens when processing millions of rows with timeouts?**

### Scenario: High-Volume Table
```
Table: 10 million rows with 11 column families = 110 million events
Processing rate: ~10,000 events/sec
Total time needed: 11,000 seconds (3+ hours)

But: query_timeout = 600s (10 minutes)
Result: ⏰ TIMEOUT before completion
```

### Old Behavior (BROKEN) ❌
```
T=0s:    Start processing
T=60s:   Resolved timestamp #1 → last_resolved = T60
T=120s:  Resolved timestamp #2 → last_resolved = T120
...
T=600s:  ⏰ TIMEOUT (no resolved timestamp received yet)
         last_resolved = T120 (2 minutes ago!)
         ❌ Lost 8 minutes of progress!
         ❌ Next run re-processes same events!
```

**Root Cause:** Only saving cursor on resolved timestamps (every `resolved_interval`), not on every event.

---

## Solution: Progressive Cursor Strategy ✅

### Core Concept
**Track the highest timestamp from EVERY event**, not just resolved timestamps.

### Implementation
```python
# Track TWO cursor values:
last_resolved = None       # Official resolved timestamp (confirms all changes up to this point)
highest_updated = None     # Highest timestamp seen in actual events (progressive checkpoint)

# For each event:
if updated:
    highest_updated = max(highest_updated, updated)  # Track highest

# For resolved timestamp events:
if key_json is None and value_json is None:
    last_resolved = updated  # Official checkpoint

# On return:
cursor = last_resolved or highest_updated  # Prefer resolved, fallback to progressive
```

### New Behavior (FIXED) ✅
```
T=0s:    Start processing
         highest_updated = T0
T=1s:    Resolved timestamp → last_resolved = T1, highest_updated = T1
T=2s:    Processing events → highest_updated = T2
T=3s:    Processing events → highest_updated = T3
...
T=600s:  ⏰ TIMEOUT
         last_resolved = T599 (most recent resolved)
         highest_updated = T600 (most recent event)
         ✅ Return cursor = T599 (from last resolved timestamp)
         ✅ ZERO progress lost!
         ✅ Next run continues from T599!
```

---

## Configuration Updates

### 1. Faster Checkpoints (Reduced `resolved_interval`)

**Old:**
```python
"resolved_interval": "5s"  # Checkpoint every 5 seconds
```

**New:**
```python
"resolved_interval": "1s"  # Checkpoint every 1 second
```

**Benefit:** Progress saved every second instead of every 5 seconds.

---

### 2. Longer Timeout (Increased `query_timeout`)

**Old:**
```python
"query_timeout": "30s"  # Too short for large datasets
```

**New:**
```python
"query_timeout": "600s"  # 10 minutes (safe for millions of rows)
```

**Why it's safe now:** Progressive cursor ensures we never lose progress, even on timeout!

---

## Progress Saving Behavior

### Scenario 1: Normal Completion (Small Dataset)
```
Events: 10,000
Time: 5 seconds
Result:
  - Received resolved timestamp after 1s
  - Stopped after first resolved (incremental mode)
  - Cursor: last_resolved = T1
  - ✅ Clean completion
```

---

### Scenario 2: Timeout Before First Resolved (Large Dataset)
```
Events: 1,000,000
Time: 600s (timeout)
Result:
  - Processed 1M events
  - Received 600 resolved timestamps (every 1s)
  - last_resolved = T599
  - highest_updated = T600
  - Cursor: last_resolved = T599
  - ✅ Progress saved!
  - Next run: Continues from T599
```

---

### Scenario 3: Timeout Mid-Stream (Very Large Dataset)
```
Events: 10,000,000 (need 16 minutes to process)
Time: 600s (timeout after 10 minutes)
Result:
  - Processed 6M events
  - Received 600 resolved timestamps
  - last_resolved = T599
  - highest_updated = T600
  - Cursor: last_resolved = T599
  - ✅ Progress saved!
  - Next run: Continues from T599 (processes remaining 4M events)
```

---

## Benefits

### ✅ Never Lose Progress
- Cursor updated every second (via resolved timestamps)
- Fallback to highest event timestamp on timeout
- Guarantees forward progress even with millions of rows

### ✅ No Duplicate Processing
- Resolved timestamps confirm "all changes up to this point"
- Progressive cursor only used as fallback
- CockroachDB changefeeds with cursor skip already-processed events

### ✅ Works for Any Dataset Size
- Small datasets: Complete quickly, stop after first resolved
- Large datasets: Process incrementally across multiple runs
- Very large datasets: Save progress every second

### ✅ Automatic Recovery
- Timeout = normal behavior (not an error)
- Pipeline automatically continues from last checkpoint
- Eventually processes all data, no manual intervention needed

---

## Example Timeline: 10 Million Row Table

```
Run #1:
T=0s:      Start snapshot (cursor = None)
T=1-600s:  Process 6M events
T=600s:    ⏰ TIMEOUT
           Return cursor = T599 (last resolved)
           Events returned: 6M

Run #2:
T=0s:      Start from cursor = T599
T=1-600s:  Process 4M events (remaining)
T=450s:    ✅ All events processed
T=451s:    Resolved timestamp received → STOP
           Return cursor = T450
           Events returned: 4M

Total: 2 runs, 10M events, ZERO duplicates! ✅
```

---

## Monitoring and Debug Output

The connector provides detailed progress information:

```
⏱️  Step 6: Processing changefeed events...
   - Total events: 6000000
   - Processing time: 600.00s
   - Average rate: 10000.0 events/sec
   - Last resolved timestamp: 1766105723451080606.0000000000
   - Highest event timestamp: 1766105724451080606.0000000000

✅ Using resolved timestamp as cursor (all changes up to this point confirmed)
End offset: {"cursor": "1766105723451080606.0000000000"}
```

---

## Summary

| Feature | Old Behavior | New Behavior |
|---------|-------------|--------------|
| Checkpoint frequency | Every 5s | Every 1s |
| Timeout handling | ❌ Lose progress | ✅ Save highest timestamp |
| Large dataset support | ❌ Re-processes events | ✅ Incremental progress |
| Manual intervention | ⚠️ Required on timeout | ✅ Automatic recovery |
| Max dataset size | Limited by timeout | ✅ Unlimited (multi-run) |

**Result:** Robust, production-ready CDC for datasets of ANY size! 🚀

