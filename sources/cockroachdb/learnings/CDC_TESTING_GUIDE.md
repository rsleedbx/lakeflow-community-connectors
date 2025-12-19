# CDC Testing Guide: CockroachDB Connector

## Quick Test: Generate CDC Events

Use CockroachDB's built-in workload tool to generate changes:

```bash
export COCKROACHDB_URL="postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/ycsb?sslmode=require"

# Generate changes for 1 minute
cockroach workload run ycsb $COCKROACHDB_URL --duration 1m
```

This will:
- Generate realistic YCSB workload (reads + writes)
- Update existing rows in `usertable`
- Create CDC events for the pipeline to process

---

## Complete Testing Workflow

### Phase 1: Initial Snapshot (First Run)

**1. Check current row count:**
```bash
psql $COCKROACHDB_URL -c "SELECT count(*) FROM usertable;"
```

Expected: ~10,000 rows (if already loaded)

**2. Trigger pipeline (initial snapshot):**
- Go to Databricks UI
- Start pipeline: `robert_lee_cockroachdb_cdc`
- Expected behavior:
  - `initial_scan = 'only'` (first run)
  - Process all 10,000 rows
  - Complete in ~30-60 seconds
  - Save snapshot start timestamp as cursor

**3. Verify snapshot:**
```sql
-- In Databricks SQL
SELECT count(*) FROM main.robert_lee_cockroachdb_cdc.usertable;
-- Expected: 10,000 rows
```

---

### Phase 2: Generate CDC Events

**4. Generate changes (run this while pipeline is idle):**
```bash
# Generate 1 minute of changes
cockroach workload run ycsb $COCKROACHDB_URL --duration 1m
```

**What this does:**
- Updates ~500-1000 rows per second
- Total updates: ~30,000-60,000 operations in 1 minute
- Creates CDC events in CockroachDB changefeed

**5. Check CockroachDB side:**
```bash
# Verify data changed
psql $COCKROACHDB_URL -c "SELECT count(*), max(field0) FROM usertable;"
```

---

### Phase 3: Incremental CDC (Subsequent Run)

**6. Wait 30 minutes (or manually trigger pipeline):**
- Pipeline runs on 30-minute schedule
- Or manually click "Start" in Databricks UI

**7. Monitor pipeline logs:**

Expected log output:
```
🔄 Incremental mode: Resuming from cursor (skip scan, changes only)

⏱️  Step 2: Building changefeed query...
Query: EXPERIMENTAL CHANGEFEED FOR usertable 
WITH initial_scan='no', updated, resolved='1s', split_column_families, 
cursor='1766105060072019839.0000000000'

⏱️  Step 5: Executing changefeed (may timeout if caught up)...
✅ Query submitted in 0.5s

⏱️  Step 6: Processing changefeed events...
   Processing events: 10000 events...
   ✅ Received resolved timestamp (caught up): 1766105660000000000
   📊 Processed 10000 events before catching up
   ⏹️  Stopping (no more queued changes)

✅ Changefeed processing completed:
   - Total events: 10000
   - Processing time: 5.23s
   - Average rate: 1912.5 events/sec
   - Last resolved timestamp: 1766105660000000000
   - Highest event timestamp: 1766105659950000000

✅ Events collected in 5.25s
   📦 Total events in buffer: 10000
   💾 All 10000 events will be saved (no data loss on timeout!)

⏱️  Step 7: Coalescing 110000 fragmented events by primary key...
✅ Coalesced to 10000 complete rows in 0.85s

🎉 read_table() COMPLETED
Total events returned: 10000
End offset: {'cursor': '1766105660000000000'}
Total time: 6.50s
```

**8. Verify CDC captured changes:**
```sql
-- Check updated timestamp in Delta Lake
SELECT 
  count(*) as total_rows,
  max(_cdc_updated) as latest_update,
  min(_cdc_updated) as earliest_update
FROM main.robert_lee_cockroachdb_cdc.usertable;

-- Expected:
-- total_rows: 10,000
-- latest_update: Recent timestamp (after workload run)
```

---

### Phase 4: Idle CDC (No Changes)

**9. Wait 30 minutes (no workload running):**
- Pipeline triggers automatically
- Should detect "caught up" and stop quickly

Expected log output:
```
🔄 Incremental mode: Resuming from cursor (skip scan, changes only)

⏱️  Step 5: Executing changefeed (may timeout if caught up)...

⏰ Query execution timed out before fetching data
   ℹ️  Likely caught up (no changes since cursor)
   📦 Events fetched: 0 (timeout before data arrived)
   ✅ Returning same cursor to retry: 1766105660000000000
   💡 Next run will retry this query (no data loss)

✅ Events collected in 1.05s
   📦 Total events in buffer: 0

🎉 read_table() COMPLETED
Total events returned: 0
End offset: {'cursor': '1766105660000000000'}
Total time: 1.15s
```

**Performance:** ~1-2 seconds (not 600 seconds!)

---

## Advanced Testing Scenarios

### Test 1: Large Batch of Changes

**Simulate high-volume CDC:**

```bash
# Generate changes for 5 minutes
cockroach workload run ycsb $COCKROACHDB_URL --duration 5m
```

Expected:
- ~150,000-300,000 operations
- Pipeline should process in one run
- May take 30-60 seconds to process
- Progressive cursor saves progress every 1 second

---

### Test 2: Continuous Changes (Stress Test)

**Start long-running workload:**

```bash
# Terminal 1: Generate continuous changes
cockroach workload run ycsb $COCKROACHDB_URL --duration 60m &

# Terminal 2: Monitor pipeline
# Watch pipeline pick up changes every 30 minutes
```

Expected behavior:
- Each pipeline run: Process changes since last cursor
- No duplicate processing (cursor prevents re-processing)
- Pipeline completes in <60s per run

---

### Test 3: Timeout Recovery (Large Dataset)

**Simulate timeout scenario:**

1. Temporarily reduce timeout:
```python
# In ingest.py
"query_timeout": "10s",  # Temporarily short timeout
```

2. Generate large batch:
```bash
cockroach workload run ycsb $COCKROACHDB_URL --duration 5m
```

3. Trigger pipeline:
- Should timeout after 10s
- But saves progressive cursor!
- Next run continues from cursor

4. Restore normal timeout:
```python
"query_timeout": "600s",  # Back to normal
```

---

### Test 4: Monitor Resolved Timestamps

**Watch CockroachDB changefeed in real-time:**

```bash
# Terminal: Watch changefeed directly
psql $COCKROACHDB_URL << 'SQL'
EXPERIMENTAL CHANGEFEED FOR usertable
WITH 
    initial_scan = 'no',
    updated,
    resolved = '5s',
    split_column_families;
SQL
```

**What to observe:**
- Resolved timestamp events every 5 seconds (when idle)
- Data events when workload is running
- Multiple events per row (split_column_families)

---

## Verification Queries

### Check Pipeline Progress

```sql
-- Databricks SQL: Check latest cursor
SELECT 
  MAX(_cdc_updated) as latest_cursor,
  COUNT(*) as total_rows
FROM main.robert_lee_cockroachdb_cdc.usertable;
```

### Compare Source vs. Target

```bash
# CockroachDB: Source count
psql $COCKROACHDB_URL -c "SELECT count(*) FROM usertable;"

# Databricks: Target count
# (Run in Databricks SQL)
SELECT count(*) FROM main.robert_lee_cockroachdb_cdc.usertable;
```

### Check for Duplicates

```sql
-- Should return 0 duplicates
SELECT ycsb_key, count(*) as cnt
FROM main.robert_lee_cockroachdb_cdc.usertable
GROUP BY ycsb_key
HAVING count(*) > 1;
```

---

## Performance Benchmarks

| Scenario | Changes | Events | Time | Rate |
|----------|---------|--------|------|------|
| **Idle CDC** | 0 | 0 | ~1-2s | N/A |
| **Small CDC** | 100 | 1,100 | ~2-3s | 400-500 events/s |
| **Medium CDC** | 10K | 110K | ~30-60s | 1,500-2,000 events/s |
| **Large CDC** | 100K | 1.1M | ~5-10 min | 1,500-2,000 events/s |
| **Initial Snapshot** | 10K | 110K | ~30-60s | 1,500-2,000 events/s |

*Note: Events = Rows × Column Families (11 for usertable)*

---

## Troubleshooting

### Issue: "No changes detected" but workload ran

**Check 1: Verify changes in CockroachDB**
```bash
psql $COCKROACHDB_URL -c "SELECT * FROM usertable ORDER BY field0 DESC LIMIT 5;"
```

**Check 2: Check cursor vs. actual timestamp**
```bash
# Get current time from CockroachDB
psql $COCKROACHDB_URL -c "SELECT cluster_logical_timestamp()::string;"

# Compare with pipeline cursor (in Databricks logs)
```

**Check 3: Verify changefeed works**
```bash
# Test changefeed directly
psql $COCKROACHDB_URL << 'SQL'
SELECT count(*) FROM (
  SELECT * FROM usertable 
  AS OF SYSTEM TIME '-1m'
) t;
SQL
```

---

### Issue: Pipeline hangs on execute

**Check 1: Connection timeout removed?**
```python
# Should be timeout=None, not timeout=30
conn = pg8000.connect(..., timeout=None)
```

**Check 2: Network connectivity**
```bash
# Test basic connectivity
psql $COCKROACHDB_URL -c "SELECT 1;"
```

---

### Issue: Duplicate rows in Delta Lake

**Check 1: Pipeline mode**
```python
# Should be 'cdc', not 'snapshot'
"ingestion_type": "cdc"
```

**Check 2: Primary key configured?**
```sql
-- Check table metadata
DESCRIBE DETAIL main.robert_lee_cockroachdb_cdc.usertable;
```

---

## Summary: Expected Results

✅ **Initial Snapshot:**
- Time: ~30-60 seconds
- Rows: All rows in source table
- Cursor: Snapshot start timestamp

✅ **Incremental CDC (with changes):**
- Time: ~5-60 seconds (depends on change volume)
- Rows: Only changed rows since last cursor
- Cursor: Latest resolved timestamp

✅ **Incremental CDC (no changes):**
- Time: ~1-2 seconds
- Rows: 0
- Cursor: Same as input (no changes)

✅ **Large Dataset Timeout:**
- Time: Up to 600 seconds
- Rows: All processed rows (progressive cursor!)
- Cursor: Highest timestamp processed
- Next run: Continues from cursor (no duplicates)

---

## Quick Commands Reference

```bash
# Generate changes (1 minute)
cockroach workload run ycsb $COCKROACHDB_URL --duration 1m

# Check row count
psql $COCKROACHDB_URL -c "SELECT count(*) FROM usertable;"

# Watch changefeed live
psql $COCKROACHDB_URL << 'SQL'
EXPERIMENTAL CHANGEFEED FOR usertable 
WITH initial_scan='no', updated, resolved='2s', split_column_families;
SQL

# Check current timestamp
psql $COCKROACHDB_URL -c "SELECT cluster_logical_timestamp()::string;"
```

Happy testing! 🚀

