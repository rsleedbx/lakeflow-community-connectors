# File Tracking Without Autoloader: Leveraging DLT

## Question
In the community connector (`read_table()`), how do we track which parquet files are already processed without Autoloader? Can we leverage DLT's existing mechanisms instead of writing complex new code?

## Answer: **You Already Are!** ✅ (With Room for Improvement)

---

## 🔍 **Current Implementation Analysis**

### **How It Works Now (Lines 819-928):**

```python
def read_table(
    self, 
    table_name: str, 
    start_offset: Dict[str, str],  # ✅ DLT provides this!
    table_options: Dict[str, str]
) -> Iterator[Dict[str, Any]]:
    """DLT tracks state via start_offset/end_offset."""
    
    return self._read_table_from_volume(table_name, start_offset, table_options)

def _read_table_from_volume(self, table_name, start_offset, table_options):
    """Current file tracking implementation."""
    
    # 1. ✅ Get last cursor from DLT
    last_cursor = start_offset.get("cursor", "") if start_offset else ""
    
    # 2. ✅ List all files
    files = dbutils.fs.ls(self.volume_path)
    
    # 3. ✅ Filter: only files AFTER cursor (lexicographic comparison)
    new_files = [f for f in file_list if f['name'] > last_cursor]
    new_files.sort(key=lambda f: f['name'])
    
    # 4. ⚠️ Process ALL new files (could be many!)
    all_rows = []
    latest_filename = last_cursor
    
    for file_info in new_files:
        filename = file_info['name']
        df = spark.read.parquet(file_path)
        records = df.toPandas().to_dict('records')
        all_rows.extend(records)  # ⚠️ All in memory!
        latest_filename = filename
    
    # 5. ✅ Return updated cursor to DLT
    end_offset = {"cursor": latest_filename}
    return iter(all_rows), end_offset
```

### **Key Insight:**

**You ARE already using DLT's offset tracking mechanism!**

| Component | Who Manages It |
|-----------|----------------|
| **Offset storage** | ✅ DLT (persists to metastore) |
| **Offset passing** | ✅ DLT (`start_offset` parameter) |
| **Offset updating** | ✅ Your code (return `end_offset`) |
| **File filtering** | ✅ Your code (simple string comparison) |

**This is the RIGHT approach!** You're using DLT's built-in state management.

---

## ⚠️ **Current Issues**

### **Issue 1: Memory - Loading All Files at Once**

```python
all_rows = []  # ⚠️ Problem starts here

for file_info in new_files:  # Could be 1000+ files!
    records = df.toPandas().to_dict('records')
    all_rows.extend(records)  # ⚠️ All in driver memory!

return iter(all_rows), end_offset  # ⚠️ Huge list!
```

**Problem:** If there are 100 new files × 10,000 rows each = **1 million rows in driver memory!**

---

### **Issue 2: All-or-Nothing Processing**

```python
for file_info in new_files:
    # Process file...
    latest_filename = filename  # ✅ Update cursor

# Return cursor ONLY at the end
end_offset = {"cursor": latest_filename}
return iter(all_rows), end_offset
```

**Problem:** If processing fails mid-way:
- Files 1-50: processed successfully
- Files 51: crashes
- **Result:** Cursor not updated, will reprocess 1-50 on next run!

---

## ✅ **Solution: Generator Pattern (Simple & Efficient)**

### **Better Implementation:**

```python
def _read_table_from_volume(
    self, 
    table_name: str, 
    start_offset: Dict[str, str], 
    table_options: Dict[str, str]
) -> Iterator[Dict[str, Any]]:
    """Improved: Use generator to yield records incrementally."""
    
    spark = SparkSession.builder.getOrCreate()
    last_cursor = start_offset.get("cursor", "") if start_offset else ""
    
    # Load schema
    schema_info = self._load_schema_from_volume(self.volume_path)
    primary_keys = schema_info.get('primary_keys', []) if schema_info else []
    
    # List files (unchanged)
    if self._dbutils is not None:
        files = self._dbutils.fs.ls(self.volume_path)
        file_list = [
            {'name': f.name, 'path': f.path, 'size': f.size}
            for f in files if f.name.endswith('.parquet')
        ]
    else:
        # Fallback...
        pass
    
    # Filter and sort (unchanged)
    new_files = [f for f in file_list if f['name'] > last_cursor]
    new_files.sort(key=lambda f: f['name'])
    
    if not new_files:
        return iter([]), start_offset
    
    # ✅ NEW: Use generator function
    def file_generator():
        """Yield records file-by-file, updating cursor progressively."""
        nonlocal last_cursor  # Allow updating cursor
        
        for file_info in new_files:
            filename = file_info['name']
            file_path = file_info['path']
            
            try:
                # Read file
                df = spark.read.parquet(file_path)
                
                # ✅ Better: Use iterator instead of toPandas()
                for row in df.toLocalIterator():
                    record = row.asDict()
                    
                    # Transform record
                    transformed = self._process_parquet_records(
                        [record],
                        source_file=filename,
                        primary_key_columns=primary_keys,
                        fallback_timestamp=None,
                        snapshot_cutoff=self._snapshot_cutoff_timestamps.get(table_name)
                    )
                    
                    # Yield each record
                    for t_record in transformed:
                        yield t_record
                
                # ✅ Update cursor after each file completes
                last_cursor = filename
                
            except Exception as e:
                # Log and skip bad file
                print(f"⚠️  Skipping file {filename}: {e}")
                continue
    
    # Return generator and final cursor
    # Note: DLT will consume the generator and persist the cursor
    return file_generator(), {"cursor": last_cursor if new_files else start_offset.get("cursor", "")}
```

### **Benefits:**

| Aspect | Before | After |
|--------|--------|-------|
| **Memory** | ❌ All files in memory | ✅ One file at a time |
| **Processing** | ❌ All-or-nothing | ✅ Incremental (per file) |
| **Cursor update** | ❌ Only at end | ⚠️ Still at end (DLT limitation) |

---

## 🤔 **Community Connector Limitations**

### **Limitation 1: Cursor Updates Only After Full Iteration**

```python
def read_table(...) -> Iterator[Dict]:
    # ...
    return iterator, end_offset  # ✅ Return offset here
    
# But... DLT only persists offset AFTER consuming entire iterator!
```

**The Problem:**

| Stage | What Happens | Cursor State |
|-------|--------------|--------------|
| 1. Return iterator | Files 1-100 queued for processing | ❌ Not updated |
| 2. Process file 1-50 | Successfully yielded to DLT | ❌ Not updated |
| 3. **Crash on file 51** | Exception thrown | ❌ **Still at old position!** |
| 4. Next run | Start from file 1 again | ⚠️ Reprocess 1-50 |

**Root Cause:** Community connector interface has no mechanism for incremental cursor updates:

```python
# ❌ Can't do this in community connector:
def read_table(...):
    for file in files:
        yield records
        update_cursor(file)  # ❌ No such method exists!

# ✅ Can only do this:
def read_table(...):
    return iterator, final_cursor  # Only persisted at END
```

**Impact:** 
- ⚠️ Reprocessing on failures
- ⚠️ Wasted compute resources
- ⚠️ Duplicate records (if not idempotent)
- ⚠️ Longer recovery times

---

### **Limitation 2: No Partial Progress Tracking**

**Community Connector Interface:**
```python
def read_table(
    self, 
    table_name: str, 
    start_offset: Dict[str, str],      # ✅ IN: Last successful position
    table_options: Dict[str, str]
) -> Tuple[Iterator[Dict], Dict[str, str]]:  # ✅ OUT: New position
    """
    Problem: Only ONE offset state transition per call!
    
    start_offset → process all files → end_offset
    
    No way to signal: "I processed files 1-50, save that progress!"
    """
```

**Comparison with Autoloader:**

| Feature | Community Connector | Autoloader |
|---------|-------------------|------------|
| **Checkpoint frequency** | ❌ Once per run (at end) | ✅ Continuous (per microbatch) |
| **Partial progress** | ❌ No | ✅ Yes (file-level tracking) |
| **Failure recovery** | ❌ Restart from beginning | ✅ Resume from last file |
| **File-level state** | ❌ Single cursor string | ✅ Detailed checkpoint metadata |
| **Exactly-once** | ⚠️ Manual (via dedup) | ✅ Automatic |

---

### **Limitation 3: Memory Constraints on Driver**

**Because community connectors run on the driver:**

```python
def read_table(...) -> Iterator[Dict]:
    """All logic runs on DRIVER node!"""
    
    # ⚠️ Driver must:
    files = list_all_files()           # 1. List files (driver)
    for file in files:                 # 2. Iterate files (driver)
        df = spark.read.parquet(file)  # 3. Read file (workers)
        records = df.collect()         # 4. ❌ Collect to driver!
        for record in records:
            yield record               # 5. Yield from driver
```

**Problems:**

| Operation | Impact | Risk |
|-----------|--------|------|
| **File listing** | ⚠️ Thousands of files = slow | Timeout on large directories |
| **Iterator creation** | ⚠️ All data through driver | Driver OOM on large files |
| **No parallelism** | ⚠️ Sequential file processing | Slow for many files |

**Example:**
- 1,000 new parquet files
- 10,000 rows each
- Driver must yield **10 million records** one by one!

**Memory usage:**
```python
# Current implementation:
all_rows = []
for file in new_files:  # 1000 files
    records = df.toPandas().to_dict('records')  # 10K rows per file
    all_rows.extend(records)  # ❌ 10M rows in driver memory!

return iter(all_rows), end_offset  # ❌ 10M row list!
```

---

### **Limitation 4: No Streaming Benefits**

**Community connector = batch pattern:**

```python
# What happens:
@dlt.table
def usertable():
    return connector.read_table(...)  # Returns Iterator[Dict]

# DLT consumes iterator:
records = []
for record in connector.read_table(...):
    records.append(record)

# Then writes batch to Delta:
spark.createDataFrame(records).write.mode("append").save()
```

**Lost benefits:**

| Autoloader Feature | Available in Community Connector? |
|-------------------|----------------------------------|
| **Streaming DataFrame** | ❌ No (batch DataFrame) |
| **Backpressure handling** | ❌ No |
| **Continuous processing** | ❌ No (one batch per run) |
| **Watermarking** | ❌ No |
| **Stateful operations** | ❌ No (via updateStateByKey, etc.) |

---

### **Limitation 5: Single-Table Only**

```python
# ❌ Can't do this in community connector:
def read_table(self, table_name, ...):
    """Separate call per table - can't share state!"""
    pass

# Each table call:
# - Separate offset tracking
# - Separate file listing
# - No cross-table coordination
# - No join/union across tables
```

**Problem for multi-table CDC:**
- Can't guarantee consistent snapshot across tables
- Each table processes independently
- No transaction boundaries

**Autoloader alternative:**
```python
# ✅ Can do this with Autoloader:
@dlt.table
def multi_table_cdc():
    """Read from multiple paths in one stream."""
    return (
        spark.readStream
            .format("cloudFiles")
            .load([path1, path2, path3])  # Multiple tables!
    )
```

---

### **Summary of Community Connector Limitations:**

| Limitation | Impact | Workaround | Best Solution |
|------------|--------|------------|---------------|
| **Cursor at end only** | ⚠️ Reprocessing on failure | Batch files (10 at a time) | Use Autoloader |
| **No partial progress** | ⚠️ All-or-nothing | Smaller batches | Use Autoloader |
| **Driver memory** | ❌ OOM on large datasets | Generator pattern | Use Autoloader |
| **No streaming** | ⚠️ Higher latency | Accept batch mode | Use Autoloader |
| **Single table** | ⚠️ No multi-table coordination | Run separately | Use Autoloader |

**Key Insight:** Community connectors are designed for **simple data sources**, not high-volume file ingestion. For CDC from files, **native DLT + Autoloader is the right tool**.

---

## ✅ **Solution: Batch Processing with Cursor Updates**

### **Option 1: Process Fewer Files Per Run**

```python
def _read_table_from_volume(self, table_name, start_offset, table_options):
    """Process files in small batches to limit reprocessing on failure."""
    
    # ... list and filter files ...
    
    # ✅ NEW: Limit files per run
    MAX_FILES_PER_RUN = 10  # Configurable
    new_files = new_files[:MAX_FILES_PER_RUN]
    
    # Process files (generator pattern)
    def file_generator():
        for file_info in new_files:
            # ... process file ...
            yield records
    
    # Return with cursor pointing to last file in batch
    last_file = new_files[-1]['name'] if new_files else start_offset.get("cursor", "")
    return file_generator(), {"cursor": last_file}
```

**Benefits:**
- ✅ Limits blast radius of failures
- ✅ Each run processes ≤10 files
- ✅ Cursor advances incrementally (10 files at a time)
- ✅ Automatic retry for failed batches (DLT triggers on schedule)

**Trade-offs:**
- ⚠️ Multiple runs needed for many files
- ⚠️ Adds latency for catching up

---

### **Option 2: Leverage DLT's Native Streaming (Best!)**

**Instead of community connector, use native DLT tables:**

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="usertable_bronze",
    comment="CockroachDB CDC ingestion"
)
def usertable_bronze():
    """Use Autoloader - it handles ALL file tracking automatically!"""
    return (
        spark.readStream
            .format("cloudFiles")  # ✅ Autoloader!
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/Volumes/.../schema")
            .option("cloudFiles.useNotifications", "false")
            .load("/Volumes/main/schema/volume/")
            .withColumn("_cdc_operation",
                F.when(F.col("__crdb__event_type") == "c", F.lit("UPSERT"))
                .when(F.col("__crdb__event_type") == "d", F.lit("DELETE"))
            )
    )
```

**What Autoloader Does (Automatically!):**
- ✅ Tracks processed files in checkpoint directory
- ✅ Incremental processing (only new files)
- ✅ Exactly-once semantics
- ✅ Handles failures gracefully (resumes from last checkpoint)
- ✅ No cursor management needed
- ✅ Efficient for thousands of files

**This is DLT's "native way"!**

---

## 📊 **Comparison**

| Approach | File Tracking | Memory | Failure Recovery | Complexity |
|----------|--------------|---------|------------------|------------|
| **Current (all files)** | Manual cursor | ❌ High | ❌ Poor | Medium |
| **Generator + batching** | Manual cursor | ✅ Low | ⚠️ OK | Medium |
| **Native DLT + Autoloader** | ✅ Automatic | ✅ Low | ✅ Excellent | ✅ Low |

---

## 🎯 **Recommendations**

### **Decision Tree: Community Connector vs Native DLT**

```
┌─────────────────────────────────────────────────────────────┐
│ Q: What's your use case?                                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ 📊 Production CDC from many files (>100/day)?              │
│  └─> Use Native DLT + Autoloader ✅                         │
│      Reason: Built for this, handles all edge cases        │
│                                                             │
│ 🧪 Testing / Low-volume (<10 files/day)?                   │
│  └─> Community Connector OK ⚠️                              │
│      Reason: Simple enough, limitations acceptable          │
│                                                             │
│ 🔄 Need exactly-once guarantees?                           │
│  └─> Use Native DLT + Autoloader ✅                         │
│      Reason: Community connector can't guarantee this       │
│                                                             │
│ 💾 Files are large (>100MB each)?                          │
│  └─> Use Native DLT + Autoloader ✅                         │
│      Reason: Community connector = driver memory issues     │
│                                                             │
│ 🚀 Need real-time / low-latency?                           │
│  └─> Use Native DLT + Autoloader ✅                         │
│      Reason: Streaming > batch                              │
│                                                             │
│ 🎯 Multiple tables need consistent snapshots?              │
│  └─> Use Native DLT + Autoloader ✅                         │
│      Reason: Community connector = isolated per table       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

### **For Community Connector (If You Must):**

**Only use if:**
- ✅ Low file volume (<10 files per run)
- ✅ Small files (<10MB each)
- ✅ OK with reprocessing on failures
- ✅ Testing/development environment

**Minimal improvements:**

1. ✅ **Keep using DLT's offset mechanism** (you already are!)
   ```python
   return iterator, {"cursor": latest_filename}
   ```

2. ✅ **Add file batching** to limit blast radius:
   ```python
   batch_size = int(table_options.get("file_batch_size", 5))  # Small!
   new_files = new_files[:batch_size]
   ```

3. ✅ **Use generator pattern** to reduce memory:
   ```python
   def file_generator():
       for file in new_files:
           for row in df.toLocalIterator():  # Don't use .toPandas()
               yield row.asDict()
   return file_generator(), end_offset
   ```

4. ⚠️ **Accept limitations**:
   - Reprocessing on failure
   - No exactly-once (use MERGE in target for idempotency)
   - No real-time processing

---

### **For Production (Recommended):**

**Use native DLT with Autoloader** - it's specifically designed for this:

```python
@dlt.table
def usertable_bronze():
    """Autoloader handles file tracking perfectly."""
    return spark.readStream.format("cloudFiles").load(volume_path)
```

**Why?**
- ✅ No manual cursor tracking
- ✅ Exactly-once processing
- ✅ Automatic checkpointing
- ✅ Handles thousands of files efficiently
- ✅ Built-in failure recovery
- ✅ **This IS the DLT way!**

---

## 💡 **Key Insight**

**Your question:** "Is there a way to leverage existing DLT way?"

**Answer:** 
- For community connector: ✅ You already ARE (offset mechanism)!
- For production: ✅ Use native DLT tables with Autoloader (the REAL DLT way)!

**The "DLT way" is:**
1. Community connectors: Use `start_offset`/`end_offset` (you're doing this ✅)
2. Native tables: Use Autoloader streaming (recommended for file tracking ✅)

---

## 📝 **Community Connector Interface: What It CAN and CANNOT Do**

### **✅ What Community Connectors CAN Do:**

| Capability | How It Works |
|------------|--------------|
| **State tracking** | ✅ Via `start_offset` / `end_offset` |
| **Incremental reads** | ✅ Filter by offset (your responsibility) |
| **Custom logic** | ✅ Any Python code in `read_table()` |
| **Integration** | ✅ Works with any data source |
| **Simple use cases** | ✅ Low-volume, simple data sources |

### **❌ What Community Connectors CANNOT Do:**

| Limitation | Why Not | Impact |
|------------|---------|--------|
| **Incremental cursor updates** | Interface only returns one offset | ⚠️ Reprocessing on failure |
| **Parallel file processing** | Runs on driver only | ⚠️ Slow for many files |
| **Streaming semantics** | Returns batch iterator | ⚠️ No real-time, no backpressure |
| **Exactly-once guarantees** | No transaction support | ⚠️ Must handle duplicates |
| **File-level checkpointing** | Only single cursor string | ⚠️ Can't track per-file state |
| **Multi-table coordination** | Isolated per table | ⚠️ No consistent snapshots |
| **Memory streaming** | Must yield all records | ⚠️ Driver memory constraints |

### **Example: What You Can't Do**

```python
# ❌ Can't do this in community connector:
def read_table(self, table_name, start_offset, table_options):
    for file in files:
        process_file(file)
        
        # ❌ No way to do this:
        dlt.checkpoint_now()  # Doesn't exist!
        dlt.save_progress({"cursor": file})  # Doesn't exist!
        
    # ✅ Can only do this:
    return iterator, final_offset  # All-or-nothing!
```

### **Why These Limitations Exist**

**By design:** Community connectors use a **simple request/response pattern**:

```
DLT calls connector once per run:
┌────────────────────────────────────────┐
│ Input: start_offset                    │
│ ↓                                      │
│ Process: Your code runs (driver)       │
│ ↓                                      │
│ Output: (iterator, end_offset)         │
│ ↓                                      │
│ DLT: Consume iterator, save offset     │
└────────────────────────────────────────┘

No callbacks, no checkpointing, no streaming.
Simple but limited!
```

**Contrast with Autoloader:**

```
Autoloader = continuous streaming:
┌────────────────────────────────────────┐
│ Autoloader continuously:               │
│ 1. Lists files                         │
│ 2. Checkpoints each file ✅            │
│ 3. Streams data                        │
│ 4. Updates checkpoint ✅               │
│ 5. Repeat...                           │
└────────────────────────────────────────┘

Built-in checkpointing at every step!
```

---

## 🔧 **Minimal Code Change (Improve Current)**

```python
def _read_table_from_volume(self, table_name, start_offset, table_options):
    """Improved: Batching + generator pattern."""
    
    # ... (file listing unchanged) ...
    
    # ✅ ADD: Batch size limit
    batch_size = int(table_options.get("file_batch_size", 10))
    new_files = new_files[:batch_size]
    
    # ✅ CHANGE: Generator instead of list
    def file_generator():
        for file_info in new_files:
            df = spark.read.parquet(file_info['path'])
            
            # ✅ CHANGE: Iterator instead of toPandas()
            for row in df.toLocalIterator():
                record = row.asDict()
                # ... transform ...
                yield transformed_record
    
    # ✅ UNCHANGED: Return cursor (DLT tracks it)
    last_file = new_files[-1]['name'] if new_files else start_offset.get("cursor", "")
    return file_generator(), {"cursor": last_file}
```

**Changes:**
1. Process max N files per run (configurable)
2. Use generator (memory efficient)
3. Use `.toLocalIterator()` instead of `.toPandas()`

**DLT handles the rest!**

---

## 🔗 **Related Files**

- **`cockroachdb.py` lines 819-928**: Current `_read_table_from_volume()` implementation
- **`cockroachdb.py` lines 462-464**: `read_table()` interface (offset passing)
- **`DBUTILS_IN_DLT.md`**: DLT environment compatibility
- **`AUTOLOADER_ITERATOR_COMPATIBILITY.md`**: Why Autoloader is better for files

---

## Summary

**Q:** How do we track processed files without Autoloader? Can we use DLT's way?

**A:** 
1. ✅ You **already ARE** using DLT's offset mechanism correctly!
2. ✅ Current approach works for **low-volume scenarios**
3. ⚠️ **Community connector has fundamental limitations**:
   - Cursor updates only at end (reprocessing on failure)
   - All processing on driver (memory constraints)
   - No streaming benefits (batch-only)
   - Single-table isolation (no coordination)
4. ✅ **Best "DLT way":** Use native DLT tables with Autoloader!

---

## 🎯 **Final Recommendation**

### **For Testing/Development:**
- ✅ Keep community connector with small improvements
- ✅ Add batching (5-10 files per run)
- ✅ Use generator pattern

### **For Production:**
- ✅ **Migrate to native DLT + Autoloader**
- ✅ It's designed for exactly this use case
- ✅ Handles all edge cases automatically
- ✅ No workarounds needed

**Community connectors are for simple data sources, not high-volume CDC file ingestion.**

**Autoloader IS the DLT way for files!** 🚀

---

## 📋 **Migration Path**

### **Phase 1: Current (Community Connector)**
```python
@dlt.table
def usertable():
    return connector.read_table(...)  # Batch, limited
```

### **Phase 2: Native DLT (Production)**
```python
@dlt.table
def usertable_bronze():
    """Much simpler, more reliable!"""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load("/Volumes/main/schema/volume/")
    )

@dlt.table
def usertable_silver():
    """Add transformations."""
    from cockroachdb import merge_column_family_fragments
    return merge_column_family_fragments(
        dlt.read_stream("usertable_bronze"),
        primary_keys=["ycsb_key"]
    )
```

**Migration benefits:**
- ✅ Remove custom file tracking code
- ✅ Better failure recovery
- ✅ Handles scale automatically
- ✅ Streaming > batch
- ✅ Less code to maintain

