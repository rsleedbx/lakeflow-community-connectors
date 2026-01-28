# CockroachDB .RESOLVED Files: When and How to Use Them

**Date:** 2026-01-27  
**Status:** Production Guidance

---

## 🎯 What Are .RESOLVED Files?

`.RESOLVED` files are CockroachDB CDC metadata files that track **changefeed watermarks** (progress timestamps).

### File Characteristics:
- **Format**: Parquet (despite non-.parquet extension)
- **Purpose**: Track the highest timestamp that has been fully emitted by the changefeed
- **Schema**: Contains a `resolved` column with nanosecond-precision timestamps
- **Encoding**: `DECIMAL(2147483647, 0)` - exceeds Spark's max precision (38)
- **Frequency**: Emitted periodically (configurable with `resolved` option)

### Example:
```
202601271650130000000000000000000.RESOLVED
├─ resolved: 1737990613000000000 (nanoseconds since epoch)
└─ Meaning: All changes up to this timestamp have been emitted
```

---

## 🚫 Default Recommendation: **Filter Them Out**

For **99% of use cases**, you should **exclude** `.RESOLVED` files using `pathGlobFilter`:

```python
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("pathGlobFilter", "*usertable*.parquet")  # ← Excludes .RESOLVED
    .load(source_path)
)
```

### Why Filter Them?
1. ✅ **Simpler**: No explicit schema needed
2. ✅ **No DECIMAL errors**: Avoids Spark precision issues
3. ✅ **Sufficient for most CDC**: Data files contain all change events
4. ✅ **Matches test scenarios**: How the connector library works
5. ✅ **Auto schema inference**: Spark infers from data files

---

## ✅ When to Use .RESOLVED Files

Use `.RESOLVED` files **only** when you need:

### 1. **Exactly-Once Delivery Guarantees**

**Scenario:** You're implementing idempotent writes and need to track what's been processed.

```python
# Read both data and .RESOLVED files
raw_df = spark.readStream
    .schema(explicit_schema)  # Must provide explicit schema
    .load(source_path)

# Separate data events from watermarks
data_df = raw_df.filter(F.col("__crdb__event_type").isNotNull())
watermark_df = raw_df.filter(F.col("resolved").isNotNull())

# Use watermark to determine safe processing boundary
latest_watermark = watermark_df.agg(F.max("resolved")).collect()[0][0]
safe_data = data_df.filter(F.col("__crdb__updated") <= latest_watermark)
```

**When to use:**
- Building downstream systems that need to know "all data up to time T is complete"
- Implementing checkpoint/resume logic based on guaranteed completeness
- Need to distinguish between "no data" vs "data not yet available"

---

### 2. **Monitoring Changefeed Lag**

**Scenario:** You want to monitor how far behind your changefeed is.

```python
# Read .RESOLVED files separately
resolved_df = spark.read.schema(resolved_schema).load(f"{source_path}/*.RESOLVED")

# Calculate lag
latest_resolved = resolved_df.agg(F.max("resolved")).collect()[0][0]
current_time = time.time_ns()
lag_seconds = (current_time - latest_resolved) / 1_000_000_000

print(f"Changefeed lag: {lag_seconds:.2f} seconds")
```

**When to use:**
- SLA monitoring for CDC pipelines
- Alerting when changefeed falls behind
- Capacity planning and performance analysis

---

### 3. **Detecting Changefeed Stalls**

**Scenario:** You need to detect if the changefeed has stopped producing data.

```python
# Monitor .RESOLVED file timestamps
resolved_times = resolved_df.select("resolved").collect()
time_gaps = [resolved_times[i+1] - resolved_times[i] for i in range(len(resolved_times)-1)]

max_gap = max(time_gaps)
if max_gap > expected_interval * 2:
    print(f"WARNING: Changefeed may be stalled (gap: {max_gap}ns)")
```

**When to use:**
- Health monitoring for production CDC pipelines
- Automated alerting systems
- Troubleshooting changefeed issues

---

### 4. **Coordinating Multiple Consumers**

**Scenario:** Multiple consumers need to coordinate on what's been fully processed.

```python
# Consumer A writes checkpoint after processing up to watermark
last_watermark = get_latest_resolved_from_resolved_files()
write_checkpoint("consumer_a", last_watermark)

# Consumer B reads from checkpoint
start_from = read_checkpoint("consumer_a")
data_df = spark.readStream.filter(F.col("__crdb__updated") > start_from)
```

**When to use:**
- Multi-stage processing pipelines
- Parallel consumer coordination
- Disaster recovery scenarios

---

## 🛠️ How to Use .RESOLVED Files (If Needed)

### Step 1: Define Explicit Schema

You **must** provide an explicit schema because Spark can't infer `DECIMAL(2147483647, 0)`:

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

# For data files
data_schema = StructType([
    StructField("ycsb_key", StringType(), True),
    # ... your data columns ...
    StructField("__crdb__updated", StringType(), False),  # Force STRING
    StructField("__crdb__event_type", StringType(), False)
])

# For .RESOLVED files
resolved_schema = StructType([
    StructField("resolved", StringType(), False)  # Also force STRING
])
```

### Step 2: Read with Explicit Schema

```python
# Option A: Read everything (data + .RESOLVED)
all_df = spark.readStream.schema(data_schema).load(source_path)

# Option B: Read .RESOLVED separately
resolved_df = spark.readStream.schema(resolved_schema).load(f"{source_path}/*.RESOLVED")
data_df = spark.readStream.schema(data_schema).option("pathGlobFilter", "*usertable*").load(source_path)
```

### Step 3: Process Watermarks

```python
# Convert STRING to timestamp (both use nanoseconds since epoch)
processed_df = df.withColumn(
    "resolved_timestamp",
    F.from_unixtime(F.col("resolved").cast("double") / 1_000_000_000).cast("timestamp")
)

# Use watermark for processing decisions
latest_watermark = processed_df.agg(F.max("resolved_timestamp"))
```

---

## 📊 Comparison: With vs Without .RESOLVED Files

| Aspect | Without .RESOLVED (Recommended) | With .RESOLVED (Advanced) |
|--------|--------------------------------|---------------------------|
| **Schema** | Auto-inferred ✅ | Explicit required ❌ |
| **Complexity** | Simple ✅ | Complex ❌ |
| **Use case** | Standard CDC (99%) ✅ | Exactly-once semantics |
| **Maintenance** | Low ✅ | High (schema changes) ❌ |
| **Performance** | Faster (fewer files) ✅ | Slower (more files) ❌ |
| **Error handling** | No DECIMAL errors ✅ | Must handle DECIMAL ❌ |

---

## 🎓 Key Insights

### 1. Data Files Are Complete
**Myth**: "I need .RESOLVED files to know when data is complete"  
**Reality**: Data files contain all CDC events. `.RESOLVED` is just metadata about what timestamp boundary has been crossed.

### 2. Watermarks Are For Coordination
**Purpose**: `.RESOLVED` files tell you "all changes up to time T have been emitted"  
**Not needed for**: Reading the actual change data

### 3. Most Pipelines Don't Need Them
**Standard CDC workflow**:
1. Read data files (`*usertable*.parquet`)
2. Apply CDC transformations (INSERT/UPDATE/DELETE)
3. Write to Delta table

**This works perfectly without .RESOLVED files!**

---

## 📝 Summary

### ✅ Use .RESOLVED Files When:
- Implementing exactly-once processing guarantees
- Monitoring changefeed lag and health
- Coordinating multiple consumers
- Building advanced CDC orchestration

### ❌ Don't Use .RESOLVED Files For:
- Standard CDC ingestion (99% of cases)
- Initial table loads
- Incremental updates
- Basic Delta table syncing

### 🎯 Default Approach:
```python
# This is all you need for most CDC use cases!
.option("pathGlobFilter", "*usertable*.parquet")  # Exclude .RESOLVED
```

---

## 🔗 Related Documentation

- `DECIMAL_MYSTERY_SOLVED.md` - Full investigation of .RESOLVED file issue
- `verify_decimal_issue.md` - Detailed root cause analysis
- `stream-changefeed-to-databricks-azure.ipynb` Cell 6 - Best solution (file filtering)
- `stream-changefeed-to-databricks-azure.ipynb` Cell 9 - Alternative (explicit schema)

---

**Last Updated:** 2026-01-27  
**Author:** Lakeflow Community Connectors Team
