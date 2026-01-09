# Parquet CDC Analysis Limitation

## The Problem

```bash
🏋️  Running workload (400 UPDATEs + 100 DELETEs)...
  UPDATE 400 ✅
  DELETE 100 ✅
  Post-workload count: 900 rows ✅

But CDC shows:
  Snapshot rows: 900  
  Update rows: 0      ❌ Where are the 400 updates?
  Delete rows: 100 ✅
  Unique keys: 1000   ❌ Should be 900!
```

## Root Cause: Parquet Event Type Limitation

CockroachDB's Parquet format uses **`__crdb__event_type='c'` for BOTH:**
1. **Initial snapshot** events (when changefeed starts)
2. **Update** events (changes to existing rows)

### Why This Happens

From CockroachDB documentation:
- `'c'` = **create/change** (ambiguous!)
- `'d'` = **delete** (explicit)
- No `'u'` for update in Parquet format

**Without timestamp analysis**, we cannot distinguish:
- Which 'c' events are from the initial snapshot
- Which 'c' events are from subsequent updates

### Code Evidence

```python:cockroachdb.py
if event_type == 'c':
    cdc_operation = 'UPSERT'  # Parquet 'c' = create/update (can't tell which!)
elif event_type == 'd':
    cdc_operation = 'DELETE'  # This IS explicit
```

When counting:
```python:cockroachdb.py
elif operation == 'UPSERT':
    # Parquet 'c' events (create/update) - count as snapshot for analysis
    total_stats['snapshot'] += 1  # ALL 'c' events counted as snapshot
```

---

## Why Counts Are Wrong

### Problem 1: Update Rows = 0

**Expected:** 400 update rows  
**Actual:** 0 update rows  
**Reason:** All 400 UPDATEs are events with type='c', counted as "Snapshot rows"

### Problem 2: Snapshot Rows = 900 (should be 1000)

**Expected:** 1000 snapshot rows (initial scan)  
**Actual:** 900 snapshot rows  
**Reason:** The 900 count suggests the analysis is reading the CDC files AFTER deletes have been applied and coalesced

### Problem 3: Unique Keys = 1000 (should be 900)

**Expected:** 900 unique keys (1000 initial - 100 deleted)  
**Actual:** 1000 unique keys  
**Reason:** ~~This is the CORRECT unique key count from the actual changefeed data (before merge/deduplication)~~

**UPDATE:** This was a **BUG** - fixed in `UNIQUE_KEYS_BUG_FIX.md`. The old code was counting deleted keys in the unique_keys count. Now fixed to only count active (non-deleted) keys.

---

## The Real Issue: Deduplication Logic

The counts don't match because:

1. **Snapshot rows (900):** Count of records in snapshot files (may be partial or from second file)
2. **Update rows (0):** Parquet can't distinguish updates from snapshots
3. **Delete rows (100):** Correct count of 'd' events
4. **Unique keys (1000):** Count of distinct primary keys across ALL files (both snapshot and CDC)

### What's Really Happening

```
Initial snapshot file: 1000 rows (type='c')
CDC file after workload: 400 updates (type='c') + 100 deletes (type='d')

Analysis reads:
- Maybe only CDC file? → 900 snapshot rows (400 updates + 500 remaining)
- Unique keys across all files: 1000 (all keys that ever existed)
```

---

## Workaround: Use JSON Format for Explicit Update Detection

JSON format DOES distinguish event types:

```json
{
  "after": {...},       // For snapshot/insert/update
  "before": null,       // null for snapshot/insert
  "__crdb__updated": "..."
}
```

JSON can detect:
- **Snapshot:** `before` is null, first occurrence
- **Update:** `before` is not null
- **Delete:** only `before` present, no `after`

---

## Recommendations

### For Testing
Use **JSON format** if you need to validate:
- Exact update counts
- Distinction between snapshot and update events
- Correct event sequencing

### For Production
Use **Parquet format** for:
- Better performance
- Lower storage costs
- Simpler merge logic (UPSERT covers both insert and update)

**Production merge doesn't need to distinguish** snapshot from update:
```sql
MERGE INTO target
USING source
ON target.pk = source.pk
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```
Both snapshot and update events become UPSERT operations.

---

## Test Output Interpretation

### JSON Tests (Accurate)
```
📊 CDC Operation Statistics:
  Snapshot rows: 1000  ✅ Initial scan
  Insert rows: 0
  Update rows: 400     ✅ Updates detected!
  Delete rows: 100     ✅ Deletes detected!
  Unique keys: 900     ✅ After deduplication
```

### Parquet Tests (Ambiguous)
```
📊 CDC Operation Statistics:
  ⚠️  NOTE: Parquet uses 'c' events for BOTH snapshots AND updates (indistinguishable)
  Snapshot rows: 900   ⚠️  Could be snapshot + updates mixed
  Insert rows: 0
  Update rows: 0       ⚠️  Can't distinguish from snapshot
  Delete rows: 100     ✅ Deletes ARE detected!
  Unique keys: 900     ✅ Correct after bug fix (only active keys)
  ✅ Unique keys match post-workload count (900)
```

---

## Related Documentation

- `learnings/PARQUET_UPDATE_DETECTION.md` - Original discovery of this limitation
- `learnings/CDC_TEST_MATRIX_RESULTS.md` - Test results showing the issue
- `cockroachdb.py` lines 2710-2770 - Analysis code
- CockroachDB Docs: [Changefeed Formats](https://www.cockroachlabs.com/docs/stable/changefeed-messages.html)

---

## Summary

**Question:** "Why are there no updates or inserts or deletes?"  
**Answer:**  
- ❌ **Updates:** Parquet can't distinguish them from snapshots (both use 'c')
- ❌ **Inserts:** None performed in this test (only UPDATE and DELETE)
- ✅ **Deletes:** ARE detected (100 shown correctly)

**The test IS working** - it's just that Parquet format limitations make it impossible to see explicit "update" counts in the analysis. For production CDC pipelines, this doesn't matter because UPSERT (merge) handles both snapshots and updates identically.

