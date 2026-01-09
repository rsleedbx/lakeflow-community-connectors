# How to Process CDC Events with Parquet Format

**Date:** 2025-12-23  
**Updated:** 2025-12-23 (DELETE support confirmed!)  
**Status:** ✅ Production-ready for full CDC (insert, update, delete)

---

## 🔍 **The Challenge**

CockroachDB's Parquet changefeed format:
- ✅ Captures all data changes (including deletes!)
- ✅ Highly efficient for storage
- ⚠️ Does NOT differentiate inserts from updates (both marked as 'c')
- ✅ **DOES support deletes** (marked as 'd')

**Key Finding:** Deletes ARE tracked in Parquet format!

---

## 📊 **What You Get in Parquet Format**

Each record contains:

| Field | Description | Possible Values |
|-------|-------------|-----------------|
| `__crdb__updated` | Change timestamp | `1766448900465939847.0000000000` |
| `__crdb__event_type` | Operation type | `'c'` (snapshot/change - snapshots AND updates use this), `'i'` (insert), `'d'` (delete) |
| Data columns | Actual field values | `field0`, `field1`, etc. |
| Primary key columns | Key values | `ycsb_key`, `id`, etc. |

**Event Type Mapping:**
- `'c'` = Snapshot, Insert, or Update (cannot distinguish between these)
- `'d'` = Delete (explicitly marked! ✅)

**Key Insight:** Deletes are properly tracked, but inserts/updates both use 'c'!

---

## 🎯 **Strategy 1: Snapshot vs CDC (Recommended)**

### **Method: Filename Sequence Number**

Parquet files include sequence numbers in their names:

```
202512230021433711272550000000000-...-00000000-usertable+fam_1_field0-4.parquet  ← Snapshot
                                    ^^^^^^^^
202512230023448399463150000000001-...-00000000-usertable+fam_1_field0-4.parquet  ← CDC batch 1
                                    ^^^^^^^^
202512230024154509057040000000001-...-00000001-usertable+fam_1_field0-4.parquet  ← CDC batch 2
                                    ^^^^^^^^
```

**Pattern:**
- **Sequence `00000000`** = Initial scan (snapshot)
- **Sequence `00000001+`** = CDC events

### **Implementation:**

```python
def classify_event_source(filename: str) -> str:
    """Determine if file contains snapshot or CDC events."""
    # Extract sequence from filename pattern:
    # TIMESTAMP-JOBID-SHARD-SEQUENCE-TABLE+FAMILY-VERSION.parquet
    parts = filename.split('-')
    if len(parts) >= 4:
        sequence = parts[3]  # e.g., "00000000" or "00000001"
        if sequence == "00000000":
            return "SNAPSHOT"
        else:
            return "CDC"
    return "UNKNOWN"

# Usage:
if classify_event_source(filename) == "SNAPSHOT":
    # Initial load - treat all as inserts
    process_as_initial_load(records)
else:
    # CDC - need to deduplicate and merge
    process_as_cdc_updates(records)
```

---

## 🎯 **Strategy 2: Insert vs Update (State Comparison)**

### **Method: Compare Against Existing State**

Since Parquet doesn't distinguish insert from update, you need to check if the row already exists.

### **Implementation in Databricks:**

```python
from pyspark.sql import functions as F

# Read CDC events
cdc_df = spark.read.parquet("azure://changefeed-events/parquet-cdc/")

# Read existing target table
existing_df = spark.table("main.catalog.target_table")

# Left anti join to find new rows (inserts)
inserts = cdc_df.join(
    existing_df,
    on=primary_key_columns,
    how="left_anti"
)

# Inner join to find existing rows (updates)
updates = cdc_df.join(
    existing_df,
    on=primary_key_columns,
    how="inner"
).select(cdc_df["*"])  # Take CDC version

print(f"Inserts: {inserts.count()}")
print(f"Updates: {updates.count()}")
```

### **Simpler Approach: MERGE (Upsert)**

```python
# Don't distinguish - just MERGE!
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "main.catalog.target_table")

delta_table.alias("target").merge(
    cdc_df.alias("source"),
    "target.id = source.id"  # Primary key condition
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

# Result: Inserts and updates handled automatically!
```

---

## 🎯 **Strategy 3: Delete Detection** ✅

### **Great News: Deletes ARE Supported!**

**Test Result:** DELETE operations properly flush to Parquet files with `__crdb__event_type='d'`

**CockroachDB Behavior:**
- ✅ Parquet changefeeds **DO** emit delete events
- ✅ Deletes are marked with `'d'` in `__crdb__event_type`
- ⚠️ Deletes may take longer to flush (batching threshold)

### **Implementation:**

```python
from pyspark.sql import functions as F

# Read Parquet CDC files
df = spark.read.parquet("azure://changefeed-events/parquet-cdc/")

# Separate deletes from upserts
deletes = df.filter(F.col("__crdb__event_type") == "d")
upserts = df.filter(F.col("__crdb__event_type") == "c")

print(f"Deletes: {deletes.count()}")
print(f"Upserts: {upserts.count()}")

# Process deletes
for row in deletes.collect():
    handle_delete(row['primary_key'])

# Process upserts with MERGE
delta_table.alias("target").merge(
    upserts.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

### **Important Notes:**

1. **Batching Delay:** Deletes may take 60-120+ seconds to flush (waiting for 1MB batch threshold)
2. **Split Column Families:** Each delete creates multiple events (one per column family)
3. **Deduplication:** Use `_coalesce_events_by_key` to merge split events

### **Alternative: Use JSON for Low-Latency Deletes**

If you need sub-second delete propagation:

```sql
-- JSON format has lower latency for small batches
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://changefeed-events/json-cdc'
WITH 
  format = 'json',
  envelope = 'wrapped',
  diff,
  updated,
  resolved = '1s';
```

---

## 🎯 **Strategy 4: Timestamp-Based Processing (Recommended)**

### **The Simplest Approach**

**Don't try to classify operations - just process by timestamp!**

```python
# Your cockroachdb.py already implements this!

def _coalesce_events_by_key(self, events):
    """
    Merge events for the same primary key.
    Latest timestamp wins.
    """
    key_to_events = defaultdict(list)
    
    # Find primary key columns (intersection of all events)
    common_columns = set.intersection(*[set(e.keys()) for e in events])
    pk_columns = [c for c in common_columns 
                  if not c.startswith('_cdc_') 
                  and not c.startswith('__crdb__')]
    
    # Group by primary key
    for event in events:
        pk_values = tuple(event.get(col) for col in pk_columns)
        key_to_events[pk_values].append(event)
    
    # Merge: Take latest value for each field
    coalesced = []
    for pk_values, key_events in key_to_events.items():
        merged = {}
        latest_updated = None
        
        for event in key_events:
            for field, value in event.items():
                if value is not None:
                    merged[field] = value  # Latest non-null wins
            
            if event.get('_cdc_updated'):
                if latest_updated is None or event['_cdc_updated'] > latest_updated:
                    latest_updated = event['_cdc_updated']
        
        merged['_cdc_updated'] = latest_updated
        coalesced.append(merged)
    
    return coalesced
```

**Result:** Automatically handles inserts, updates, and duplicates!

---

## 📋 **Practical Processing Workflow**

### **Step 1: Read Files**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CDC Processing").getOrCreate()

# Read all Parquet files
df = spark.read.parquet("azure://changefeed-events/parquet-cdc/**/*.parquet")

print(f"Total events: {df.count()}")
print(f"Schema: {df.printSchema()}")
```

### **Step 2: Classify Source**

```python
from pyspark.sql import functions as F

# Extract sequence number from input_file_name
df = df.withColumn("source_file", F.input_file_name())
df = df.withColumn(
    "event_source",
    F.when(
        F.col("source_file").contains("00000000-"),
        "SNAPSHOT"
    ).otherwise("CDC")
)

# Separate snapshot and CDC
snapshot_df = df.filter(F.col("event_source") == "SNAPSHOT")
cdc_df = df.filter(F.col("event_source") == "CDC")

print(f"Snapshot events: {snapshot_df.count()}")
print(f"CDC events: {cdc_df.count()}")
```

### **Step 3: Deduplicate by Primary Key**

```python
from pyspark.sql.window import Window

# Define window: Partition by PK, order by timestamp (latest first)
window = Window.partitionBy("ycsb_key").orderBy(F.col("__crdb__updated").desc())

# Take only the latest version of each key
deduplicated_df = df.withColumn("row_num", F.row_number().over(window)) \
                    .filter(F.col("row_num") == 1) \
                    .drop("row_num")

print(f"After deduplication: {deduplicated_df.count()}")
```

### **Step 4: Merge to Target**

```python
from delta.tables import DeltaTable

# Write to Delta table with MERGE
delta_table = DeltaTable.forName(spark, "main.catalog.target_table")

delta_table.alias("target").merge(
    deduplicated_df.alias("source"),
    "target.ycsb_key = source.ycsb_key"
).whenMatchedUpdate(
    condition="source.__crdb__updated > target.__crdb__updated",  # Only if newer
    set={
        "field0": "source.field0",
        "field1": "source.field1",
        # ... other fields
        "__crdb__updated": "source.__crdb__updated"
    }
).whenNotMatchedInsertAll(
).execute()

print("✅ MERGE complete!")
```

---

## 🎓 **Summary: Your Options**

| Need | Solution | Complexity | Parquet Support |
|------|----------|------------|-----------------|
| **Snapshot vs CDC** | Filename sequence number | ⭐ Easy | ✅ Yes |
| **Insert vs Update** | State comparison or MERGE | ⭐⭐ Medium | ⚠️ Both use 'c' |
| **Delete detection** | Check `__crdb__event_type='d'` | ⭐ Easy | ✅ **Yes!** |
| **Simple ingestion** | Timestamp-based deduplication | ⭐ Easy | ✅ Yes |

**Bottom Line:** Parquet format supports all CDC operations, but cannot distinguish inserts from updates.

---

## 💡 **Recommended Approach for Your Use Case**

### **For Data Lake / Analytics:**

```python
# 1. Read all Parquet files (snapshot + CDC)
df = spark.read.parquet("azure://changefeed-events/parquet-cdc/")

# 2. Deduplicate by PK + timestamp (latest wins)
from pyspark.sql.window import Window
window = Window.partitionBy(primary_keys).orderBy(F.col("__crdb__updated").desc())
latest_df = df.withColumn("rn", F.row_number().over(window)) \
              .filter(F.col("rn") == 1) \
              .drop("rn")

# 3. Write to Delta Lake
latest_df.write.format("delta").mode("overwrite").saveAsTable("target_table")
```

**Advantages:**
- ✅ No need to classify operations
- ✅ Handles duplicates from `split_column_families`
- ✅ High performance
- ✅ Idempotent (can rerun safely)

### **For Operational CDC (with Delete Support):**

Use **JSON format**:

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://changefeed-events/json-cdc'
WITH 
  format = 'json',
  envelope = 'wrapped',
  diff,
  updated,
  resolved = '10s';
```

Then process in Python:
```python
import json

for line in json_file:
    event = json.loads(line)
    value = event.get("value", {})
    
    has_before = "before" in value and value["before"]
    has_after = "after" in value and value["after"]
    
    if has_after and not has_before:
        # Snapshot or Insert (after only)
        handle_insert(value["after"])
    elif has_after and has_before:
        # Update (both before and after)
        handle_update(value["before"], value["after"])
    elif has_before and not has_after:
        # Delete (before only)
        handle_delete(value["before"])
```

---

## 🔗 **References**

- **Your Implementation:** `sources/cockroachdb/cockroachdb.py:655-722` (`_coalesce_events_by_key`)
- **CockroachDB Docs:** [Changefeed Messages](https://www.cockroachlabs.com/docs/stable/changefeed-messages)
- **Test Results:** `FINAL_CDC_TEST_RESULTS.md`

---

**Last Updated:** 2025-12-23 16:45 PST  
**Status:** Production-ready for Parquet-based CDC ✅

