# Debugging Changefeed Hangs

If `test_local.py` hangs at "📖 Reading data from 'events'...", use this guide to diagnose.

## Quick Diagnosis

Run the diagnostic script:

```bash
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb
python test_changefeed_direct.py
```

This will:
- Test the changefeed directly (bypassing the connector)
- Timeout after 10 seconds if it hangs
- Show exactly where the issue is

## Possible Issues & Solutions

###  1. **Changefeed Never Returns Data**

**Symptom:** `test_changefeed_direct.py` times out after 10 seconds

**Cause:** `initial_scan` might not be working as expected in this CockroachDB version

**Solution:** Use `initial_scan='only'` instead to force it to return existing data only:

```python
# In cockroachdb.py, change:
initial_scan = table_options.get("initial_scan", "only")  # Changed from "yes" to "only"
```

### 2. **Rangefeeds Not Enabled**

**Symptom:** Error about rangefeeds in diagnostic output

**Solution:**
```bash
./enable_rangefeeds.sh
./check_setup.sh
```

### 3. **Table is Empty**

**Symptom:** Diagnostic shows "events table has 0 rows"

**Solution:**
```bash
./local_setup.sh start
```

### 4. **Resolved Timestamps Blocking**

**Symptom:** Changefeed returns resolved timestamp first, then hangs

**Cause:** With `resolved='5s'`, CockroachDB sends a watermark every 5 seconds. If there's no data, it waits for the first resolved timestamp.

**Solution:** Reduce resolved interval or handle differently:
```python
# Option 1: Shorter resolved interval
resolved_interval = "1s"  # Instead of "5s"

# Option 2: Don't wait for resolved timestamps in tests
# Skip the first resolved timestamp and immediately return
```

### 5. **psycopg2 Buffering Issues**

**Symptom:** `cursor.execute()` completes but `fetchone()` hangs

**Cause:** psycopg2 might be waiting for more data before returning

**Solution:** Use server-side cursor with `LIMIT`:
```python
# Instead of:
changefeed_cursor.execute("EXPERIMENTAL CHANGEFEED FOR events WITH ...")

# Try:
changefeed_cursor.execute("""
    SELECT * FROM (
        EXPERIMENTAL CHANGEFEED FOR events WITH initial_scan='only'
    ) LIMIT 10
""")
```

## Debug Mode

Enable debug logging to see the actual SQL query:

```bash
export DEBUG_CHANGEFEED=1
python test_local.py
```

This will print:
```
[DEBUG] Changefeed query: EXPERIMENTAL CHANGEFEED FOR events WITH updated, resolved='5s', initial_scan='yes'
[DEBUG] Batch size: 5
[DEBUG] Start offset: {}
```

## Testing Without Changefeeds

If changefeeds continue to hang, test basic connectivity:

```bash
cockroach sql --insecure -d ycsb -e "SELECT COUNT(*) FROM events;"
cockroach sql --insecure -d ycsb -e "SELECT * FROM events LIMIT 5;"
```

## Known Limitations

1. **CockroachDB Version:** `initial_scan` was added in v21.1. Check version:
   ```bash
   cockroach version
   ```

2. **Sinkless Changefeed Limits:** Sinkless changefeeds are meant for low-volume streaming. For large tables (>1M rows), they may be slow or hang.

3. **Network Latency:** Even local connections can have latency. The changefeed might just be slow, not hung.

## Workaround: Use Simple SELECT Instead

If changefeeds are problematic for testing, you can temporarily use a simple SELECT:

```python
# In test_local.py, replace read_table() call with:
cursor = conn.cursor()
cursor.execute("SELECT * FROM events LIMIT 5")
records = cursor.fetchall()
```

This tests the connector's schema and metadata logic without requiring working changefeeds.

