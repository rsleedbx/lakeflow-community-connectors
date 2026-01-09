# CockroachDB Parquet Changefeed Real-Time Limitation

## Issue

Parquet changefeeds **do not flush CDC events in real-time**, even with substantial workloads.

## Test Results

### Workload Executed
- **Test 1**: 196 UPDATE operations → **0 new Parquet files**
- **Test 2**: 12,265 UPDATE operations (30s @ 408 ops/sec) → **0 new Parquet files**
- **Wait time**: Up to 90 seconds after workload
- **Result**: Only initial scan Parquet files exist, no CDC files

### What We Observed

```bash
# Before workload: 11 Parquet files (initial scan only)
parquet-cdc/2025-12-22/202512222012135254607470000000000-...usertable+fam_*.parquet
  Last Modified: 2025-12-22T20:12:14+00:00

# After 12,265 UPDATEs + 90s wait: STILL 11 Parquet files
# No new files created!

# BUT: .RESOLVED markers are being written every ~30s
parquet-cdc/2025-12-22/202512222012439481061460000000000.RESOLVED
parquet-cdc/2025-12-22/202512222013168823858120000000000.RESOLVED
... (many more)
```

## Root Cause

###Parquet Changefeed Batching Behavior

CockroachDB Parquet changefeeds use **aggressive batching** and only flush when:
1. **Size threshold is reached** (typically several MB per file)
2. **Time-based flush intervals** (much longer than JSON, possibly hours)
3. **Changefeed is paused/stopped** (forces flush)

With `split_column_families`, each column family would need to accumulate enough changes independently to trigger a flush.

### Why JSON Works But Parquet Doesn't

| Aspect | JSON Format | Parquet Format |
|--------|-------------|----------------|
| **Flush frequency** | Every few seconds | When MB threshold reached |
| **Minimum data** | 1 event | Several MB |
| **Real-time** | ✅ Yes | ❌ No |
| **Use case** | Streaming CDC | Batch CDC |
| **File size** | Small (~KB) | Large (~MB+) |
| **Immediate visibility** | ✅ Yes | ❌ No |

## Implications

### For Testing
- ❌ **Cannot use Parquet** for quick CDC validation
- ❌ **Cannot see immediate results** after running workloads
- ✅ **Must use JSON** for testing and demos

### For Production
- ✅ **Parquet is fine** for batch/bulk CDC (hourly/daily)
- ❌ **Parquet not suitable** for near-real-time CDC
- ✅ **JSON better** for streaming use cases

## Recommendations

### 1. Use JSON for Testing

```bash
# Test with JSON format for immediate feedback
./test_azure_cdc.sh json
```

**Benefits**:
- ✅ See CDC events within seconds
- ✅ Validate changefeed is working
- ✅ Test statistics analyzer
- ✅ Immediate feedback loop

### 2. Use JSON for Real-Time CDC

If you need near-real-time CDC (<1 minute latency):

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://...'
WITH 
  updated,
  resolved = '10s',
  format = 'json',
  envelope = 'wrapped',
  split_column_families;
```

Then convert to Parquet in Databricks:

```python
# Read JSON changefeed
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("wasbs://.../json-cdc/")

# Write as Parquet/Delta
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "...") \
    .table("bronze_table")
```

### 3. Use Parquet for Batch CDC

If you only need hourly/daily updates:

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://...'
WITH 
  updated,
  resolved = '1h',  -- Longer interval for batch
  format = 'parquet',
  compression = 'gzip',
  split_column_families;
```

Run heavy workloads over hours, let Parquet files naturally accumulate.

## Alternative: Force Flush

### Option 1: Massive Workload

Generate enough data to exceed size threshold:

```bash
# Run VERY heavy workload (hours, millions of ops)
cockroach workload run ycsb "$URL" \
  --duration=1h \
  --concurrency=100 \
  --workload=a  # 50% updates
```

### Option 2: Pause/Resume Changefeed

Pausing a changefeed might force a flush:

```sql
-- Pause to force flush
PAUSE JOB 1134965337404506113;

-- Wait, then resume
RESUME JOB 1134965337404506113;
```

**Warning**: This is disruptive and not recommended for production.

### Option 3: Cancel and Recreate

Canceling a changefeed forces final flush:

```bash
./cancel_job.sh 1134965337404506113
./test_azure_cdc.sh parquet  # Creates new changefeed
```

But you lose the existing changefeed state.

## Updated Test Strategy

### For Development/Testing

```bash
# 1. Use JSON for quick validation
./test_azure_cdc.sh json

# 2. Run analyzer (will show CDC events immediately)
./analyze_changefeed_stats.py

# 3. Verify statistics
# Expected: snapshot + INSERT/UPDATE/DELETE counts
```

### For Production Setup

```bash
# 1. Start with JSON for initial validation
./test_azure_cdc.sh json

# 2. Once validated, switch to Parquet for production
# Cancel existing changefeed
./cancel_job.sh $CHANGEFEED_JOB_ID

# 3. Create Parquet changefeed for batch processing
./test_azure_cdc.sh parquet

# 4. Set expectations: CDC files will appear in hours, not minutes
```

## CockroachDB Documentation

From CockroachDB docs:
- Parquet format is optimized for **analytics and batch processing**
- JSON format is recommended for **streaming and real-time use cases**
- Parquet files are **buffered and batched** for efficiency
- Use `min_checkpoint_frequency` to control flush frequency (but may not help with Parquet)

## Conclusion

**Parquet changefeeds are NOT suitable for real-time CDC demonstration or testing.**

### What Works
- ✅ JSON format for real-time CDC
- ✅ Parquet for bulk/batch CDC (with patience)
- ✅ JSON → Parquet conversion in Databricks

### What Doesn't Work
- ❌ Parquet for immediate CDC visibility
- ❌ Parquet for testing/demos
- ❌ Parquet for < 1 hour latency requirements

### Action Items

1. **Update `test_azure_cdc.sh`** to default to JSON, not Parquet
2. **Update documentation** to warn about Parquet latency
3. **Add JSON test** to demonstrate CDC working correctly
4. **Reserve Parquet** for production batch scenarios

---

**Date**: December 22, 2025  
**Test Workload**: 12,265 UPDATE operations  
**Wait Time**: 90+ seconds  
**Result**: ❌ No CDC Parquet files created  
**Recommendation**: ✅ Use JSON for testing



