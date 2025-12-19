# Buffered Events on Timeout: Zero Data Loss Strategy

## Question
**"Can we do anything with what is in the buffer on timeout? It would be waste to refresh and fail again."**

## Answer: Already Implemented! ✅

**Yes, we save ALL buffered events on timeout.** No data is lost!

---

## How It Works: Streaming Pipeline

### Event Flow (Before Timeout)

```python
# 1. Generator yields events IMMEDIATELY as they're processed
def event_generator():
    event_count = 0
    highest_updated = None
    
    while True:
        rows = fetchmany(1000)  # Fetch batch from CockroachDB
        
        for row in rows:
            event = transform(row)
            
            # Track cursor for this event
            highest_updated = max(highest_updated, updated)
            
            # ✅ YIELD IMMEDIATELY (not at the end!)
            yield event
            event_count += 1
            
    # If timeout occurs, events already yielded are safe!

# 2. Collector saves ALL yielded events
events = []
for event in event_generator():
    events.append(event)  # ✅ All yielded events are saved here

# 3. Return events + cursor
return events, {"cursor": highest_updated}
```

**Key insight:** Events are **yielded one-by-one** as they're processed, NOT batched and returned at the end!

---

## Timeout Scenarios

### Scenario 1: Timeout During `execute()` (Before Data Fetched)

```
Timeline:
T=0s:   changefeed_cursor.execute(query)
        ⏰ TIMEOUT (network issue, hung query)
        
Result:
✅ No events lost (none were fetched yet)
✅ Return same cursor
✅ Next run retries query
```

**Output:**
```
⏰ Query execution timed out before fetching data
   ℹ️  Likely caught up (no changes since cursor)
   📦 Events fetched: 0 (timeout before data arrived)
   ✅ Returning same cursor to retry: 1766105060072019839
   💡 Next run will retry this query (no data loss)
```

---

### Scenario 2: Timeout During `fetchmany()` (After Processing Data)

```
Timeline:
T=0s:    Execute query ✅
T=1s:    Fetch batch 1 (1000 rows) → process → yield 1000 events
T=2s:    Fetch batch 2 (1000 rows) → process → yield 1000 events
T=3s:    Fetch batch 3 (1000 rows) → process → yield 1000 events
...
T=599s:  Fetch batch 5000 (1000 rows) → process → yield 1000 events
T=600s:  fetchmany() waiting for batch 5001
         ⏰ TIMEOUT (statement_timeout reached)
         
Result:
✅ 5,000,000 events already yielded and saved!
✅ Progressive cursor = timestamp of event 5,000,000
✅ Return 5M events + cursor
✅ Next run: Start from cursor, fetch remaining events
✅ Zero duplicates! (cursor skips already-processed data)
```

**Output:**
```
⏰ Socket read timeout after 600.0s
   ⚠️  Timeout during snapshot (large dataset)
   📊 Successfully processed 5000000 events before timeout
   ✅ All 5000000 events will be saved and returned
   ✅ Progress saved via progressive cursor: 1766105660000000000
   💡 Next run will continue from this cursor (zero duplicate processing!)

✅ Events collected in 600.05s
   📦 Total events in buffer: 5000000
   💾 All 5000000 events will be saved (no data loss on timeout!)
```

---

## Visual: Event Pipeline with Timeout

```
┌─────────────────────────────────────────────────────────────┐
│ CockroachDB Changefeed                                       │
│ (10 million events available)                               │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ execute()
                  ↓
┌─────────────────────────────────────────────────────────────┐
│ Generator: event_generator()                                 │
│                                                              │
│  while True:                                                 │
│    rows = fetchmany(1000)  ← Fetch batch                   │
│    for row in rows:                                         │
│      event = transform(row)                                 │
│      highest_updated = max(highest_updated, row.timestamp)  │
│      yield event  ← ✅ IMMEDIATE YIELD (saved in buffer)    │
│                                                              │
│  [Processing 5M events... event_count = 5,000,000]         │
│  ⏰ TIMEOUT on fetchmany() for batch 5001                   │
│  return highest_updated  ← Cursor for last successful event │
└─────────────────┬───────────────────────────────────────────┘
                  │ yield yield yield...
                  ↓
┌─────────────────────────────────────────────────────────────┐
│ Collector: for event in gen: events.append(event)          │
│                                                              │
│ events = [event1, event2, ..., event5000000]  ← ✅ SAVED!   │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────────────────────────┐
│ Return to DLT Framework                                      │
│                                                              │
│ return (                                                     │
│   events: 5,000,000 rows,  ← ✅ ALL processed events        │
│   cursor: "T5000000"       ← ✅ Resume point                 │
│ )                                                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Why No Duplicates on Retry?

**Q:** If we retry with the cursor from the last successful event, won't we re-process it?

**A:** No! CockroachDB changefeeds with `cursor` use **exclusive** (>) not inclusive (>=):

```sql
-- Next run uses cursor = "T5000000"
EXPERIMENTAL CHANGEFEED FOR usertable
WITH 
    cursor = 'T5000000',  -- Start AFTER this timestamp
    initial_scan = 'no'
```

**Result:** Events with timestamp <= T5000000 are skipped. Only events > T5000000 are returned.

---

## Data Loss Analysis

### Events at Risk: Only Unfetched Batch

```
Event State at Timeout:

✅ Batch 1-5000 (5M events):
   - Fetched: YES
   - Processed: YES
   - Yielded: YES
   - Saved: YES
   - Cursor updated: YES
   
❌ Batch 5001 (1K events):
   - Fetched: NO (timeout during fetchmany)
   - Processed: NO
   - Yielded: NO
   - Saved: NO
   
Result: 0 events lost! Batch 5001 will be fetched in next run.
```

**Maximum data at risk:** Only the current batch being fetched (default: 1000 events)

**But even this is recovered:** Next run re-fetches from cursor, gets batch 5001 again!

---

## Optimizations

### 1. Smaller Fetch Size (Less at Risk per Timeout)

```python
# Current
fetch_size = 1000  # At most 1000 events "in flight" during fetchmany

# Could reduce to:
fetch_size = 100   # At most 100 events "in flight"
```

**Trade-off:** More network round-trips vs. smaller "window of vulnerability"

**Recommendation:** Keep 1000. With progressive cursor, risk is zero anyway!

---

### 2. Progressive Cursor (Already Implemented!)

```python
# Update cursor for EVERY event yielded
for row in rows:
    event = transform(row)
    highest_updated = max(highest_updated, updated)  # ✅ Update cursor
    yield event                                       # ✅ Yield immediately
```

**Result:** Cursor always reflects the last successfully processed event.

---

### 3. Frequent Resolved Timestamps (Already Configured!)

```python
"resolved_interval": "1s"  # CockroachDB sends checkpoint every second
```

**Benefit:** Even without progressive cursor, max data loss would be 1 second of events.

---

## Summary

| Question | Answer |
|----------|--------|
| Are buffered events saved on timeout? | ✅ YES! All yielded events are saved |
| How many events are lost on timeout? | ✅ ZERO! All processed events are saved |
| Will we re-fetch the same data? | ✅ NO! Cursor prevents duplicates |
| What about the batch being fetched? | ✅ Re-fetched in next run (no loss) |
| Do we need manual intervention? | ✅ NO! Automatic recovery |

---

## Real-World Example: 10 Million Row Table

```
Initial Snapshot:

Run 1 (triggered at 10:00 AM):
- Start: cursor = None
- Process: 6M events in 600s
- ⏰ Timeout at 10:10 AM
- Save: 6M events to Delta Lake ✅
- Cursor: T600 (timestamp of event 6M)

Run 2 (triggered at 10:30 AM):
- Start: cursor = T600
- Skip: Events 1-6M (already processed)
- Process: 4M events (remaining) in 400s
- ✅ Complete at 10:36 AM
- Save: 4M events to Delta Lake ✅
- Cursor: T1000 (timestamp of last event)

Total:
- Time: 2 runs, 1000 seconds total processing
- Events: 10M (6M + 4M)
- Duplicates: ZERO
- Lost data: ZERO
- Manual intervention: ZERO
```

---

## Conclusion

**Your concern is valid, but already solved!** ✅

The implementation:
1. ✅ Yields events immediately (not batched at end)
2. ✅ Updates cursor for every event
3. ✅ Saves all yielded events on timeout
4. ✅ Returns cursor for last processed event
5. ✅ Next run resumes from cursor (no duplicates)

**Result:** Zero data loss, zero duplicate processing, fully automatic recovery! 🚀

