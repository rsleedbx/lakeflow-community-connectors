# Parquet: Snapshot vs CDC Detection Methods

## Question
To distinguish snapshot from CDC events in Parquet files using timestamp-based analysis, do we check:
1. **File names** (file-level metadata)?
2. **Row-by-row** timestamps (`__crdb__updated`)?

## Answer: **BOTH** (Two Complementary Approaches)

---

## 🎯 **Method 1: File Naming Pattern (Fast, File-Level)**

### CockroachDB File Naming Convention

```
TIMESTAMP-JOBID-SHARD-SEQUENCE-TABLE+FAMILY-VERSION.parquet
                      ^^^^^^^^
                      This tells you!
```

### Example Files

```bash
# Snapshot files (sequence = 00000000)
202512230021433711272550000000000-abc-1-44-00000000-usertable+fam_1_field0-4.parquet
                                           ^^^^^^^^

# CDC files (sequence = 00000001, 00000002, etc.)
202512230023448399463150000000001-abc-1-44-00000001-usertable+fam_1_field0-4.parquet
                                           ^^^^^^^^
202512230024154509057040000000001-abc-1-44-00000002-usertable+fam_1_field0-4.parquet
                                           ^^^^^^^^
```

### Detection Logic (Python)

```python
def classify_file_by_name(filename: str) -> str:
    """
    Fast file-level classification using filename pattern.
    
    Returns: 'SNAPSHOT', 'CDC', or 'UNKNOWN'
    """
    # Pattern: TIMESTAMP-JOBID-SHARD-SEQUENCE-TABLE-VERSION.parquet
    parts = filename.split('-')
    
    if len(parts) >= 4:
        sequence = parts[3]  # e.g., "00000000" or "00000001"
        
        if sequence == "00000000":
            return "SNAPSHOT"  # Initial scan
        elif sequence.startswith("0000000") and sequence != "00000000":
            return "CDC"  # Subsequent batches
    
    return "UNKNOWN"

# Usage:
if classify_file_by_name(blob_name) == "SNAPSHOT":
    print(f"✅ Snapshot file: {blob_name}")
else:
    print(f"📝 CDC file: {blob_name}")
```

### Pros & Cons

**✅ Pros:**
- **Fast**: No need to read file contents
- **Simple**: Just parse filename
- **Reliable**: CockroachDB guarantees this pattern

**❌ Cons:**
- **Coarse-grained**: Entire file is classified as one or the other
- **Problem**: Doesn't help if snapshot and CDC events are mixed in one file (rare, but possible during initial scan completion)

---

## 🎯 **Method 2: Row-Level Timestamp Analysis (Precise, Row-by-Row)**

### CockroachDB Timestamp Column

Every row in Parquet has `__crdb__updated` timestamp:

```python
# Example Parquet row
{
    'ycsb_key': 'user1000000000000',
    'field0': 'abc',
    '__crdb__event_type': 'c',  # 'c' = snapshot OR update (can't tell!)
    '__crdb__updated': '1735861845268766230.0000000000'  # Nanosecond timestamp
}
```

### How It Works

1. **Capture snapshot cutoff timestamp** when changefeed is created:
   ```python
   # When creating changefeed, capture current timestamp
   cursor.execute("SELECT cluster_logical_timestamp()::string")
   snapshot_cutoff = cursor.fetchone()[0]
   # e.g., '1735861845268766230.0000000000'
   ```

2. **Compare each row's timestamp** against cutoff:
   ```python
   def classify_event_by_timestamp(row, snapshot_cutoff):
       """
       Row-level classification using timestamp comparison.
       
       Args:
           row: Parquet record with __crdb__updated field
           snapshot_cutoff: Timestamp when changefeed was created
       
       Returns: 'SNAPSHOT', 'UPDATE', or 'DELETE'
       """
       event_type = row.get('__crdb__event_type', '')
       event_timestamp = row.get('__crdb__updated', '')
       
       if event_type == 'c':
           # 'c' = create/change (ambiguous!)
           if event_timestamp <= snapshot_cutoff:
               return 'SNAPSHOT'  # Before cutoff = initial scan
           else:
               return 'UPDATE'    # After cutoff = CDC update
       elif event_type == 'd':
           return 'DELETE'  # Always clear
       else:
           return 'UNKNOWN'
   ```

3. **Store cutoff per table**:
   ```python
   class LakeflowConnect:
       def __init__(self):
           # Track snapshot cutoff timestamps per table
           self._snapshot_cutoff_timestamps = {}
       
       def read(self, table_name):
           # If newly created changefeed, capture cutoff
           if is_newly_created:
               cursor.execute("SELECT cluster_logical_timestamp()::string")
               self._snapshot_cutoff_timestamps[table_name] = cursor.fetchone()[0]
           
           # Later, when processing Parquet rows:
           cutoff = self._snapshot_cutoff_timestamps.get(table_name)
           for row in parquet_data:
               operation = self._determine_cdc_operation(
                   row['__crdb__event_type'],
                   row['__crdb__updated'],
                   cutoff
               )
   ```

### Code Reference (cockroachdb.py)

```python:942:962
def _determine_cdc_operation(
    self,
    event_type: str,
    event_timestamp: Optional[str] = None,
    snapshot_cutoff: Optional[str] = None
) -> str:
    """
    Map CockroachDB event type to CDC operation.
    
    NOTE: For Parquet format, __crdb__event_type is 'c' for both snapshots and 
    updates (indistinguishable by type alone). Uses timestamp-based logic to distinguish:
    - Events with timestamp <= snapshot_cutoff = SNAPSHOT
    - Events with timestamp > snapshot_cutoff = UPDATE
    """
    if event_type == 'c':
        # For 'c' events, use timestamp to distinguish snapshot from update
        if event_timestamp and snapshot_cutoff:
            try:
                # Compare timestamps as strings (they're in sortable format)
                if event_timestamp > snapshot_cutoff:
                    return 'UPDATE'
            except:
                pass
        # Default to SNAPSHOT if no timestamp logic available
        return 'SNAPSHOT'
    elif event_type == 'i':
        return 'INSERT'
    elif event_type == 'd':
        return 'DELETE'
    else:
        return 'UNKNOWN'
```

### Pros & Cons

**✅ Pros:**
- **Precise**: Per-row classification
- **Handles mixed files**: Can have snapshots and updates in same file
- **Production-grade**: Used in `cockroachdb.py`

**❌ Cons:**
- **Slower**: Must read every row
- **Requires cutoff**: Must capture timestamp at changefeed creation time
- **Stateful**: Need to store/retrieve cutoff timestamps

---

## 📊 **Comparison Table**

| Method | Granularity | Speed | Use Case |
|--------|-------------|-------|----------|
| **File naming** | File-level | ⚡ Fast | Quick file filtering, counting files |
| **Row timestamps** | Row-level | 🐢 Slow | Accurate event classification, production CDC |

---

## 🏗️ **Best Practice: Use BOTH**

### Step 1: File-Level Filter (Fast)
```python
# Count snapshot vs CDC files (no need to read contents)
snapshot_files = []
cdc_files = []

for blob in container_client.list_blobs(name_starts_with="parquet/"):
    if blob.name.endswith('.parquet'):
        if classify_file_by_name(blob.name) == "SNAPSHOT":
            snapshot_files.append(blob.name)
        else:
            cdc_files.append(blob.name)

print(f"📸 Snapshot files: {len(snapshot_files)}")
print(f"📝 CDC files: {len(cdc_files)}")
```

### Step 2: Row-Level Analysis (Precise)
```python
# For production CDC pipeline: use row-level timestamps
cutoff = self._snapshot_cutoff_timestamps.get(table_name)

for file in all_parquet_files:
    df = pd.read_parquet(file)
    
    for _, row in df.iterrows():
        operation = self._determine_cdc_operation(
            row['__crdb__event_type'],
            row['__crdb__updated'],
            cutoff  # Precise per-row classification
        )
        
        if operation == 'SNAPSHOT':
            handle_snapshot(row)
        elif operation == 'UPDATE':
            handle_update(row)
        elif operation == 'DELETE':
            handle_delete(row)
```

---

## 🚨 **Why BOTH Are Needed**

### Problem 1: File Naming is Coarse
- File `00000000` might contain 10,000 rows
- **All classified as SNAPSHOT** at file level
- But what if last 100 rows are actually updates that happened during initial scan?
- **Row-level timestamps catch this!**

### Problem 2: Row Timestamps Need Context
- Row timestamp alone is useless without **cutoff timestamp**
- You must capture cutoff **when changefeed is created**
- File naming pattern helps you know **when to capture cutoff** (at first `00000000` file)

---

## 🎯 **When to Use Each Method**

### Use File Naming Pattern When:
- ✅ Counting/listing files quickly
- ✅ Skipping already-processed snapshot files
- ✅ Debugging changefeed output structure
- ✅ Validating changefeed is producing files

### Use Row-Level Timestamps When:
- ✅ Building production CDC pipeline
- ✅ Accurate event classification required
- ✅ Deduplicating events
- ✅ Applying merge/upsert logic
- ✅ Mixed snapshot + CDC files possible

---

## 📝 **Implementation in test_cdc_matrix.sh**

Currently, the test script uses **file naming** for quick validation:

```bash
# Count snapshot files (sequence 00000000)
snapshot_count=$(az storage blob list \
  --container-name changefeed-events \
  --prefix "$path_prefix/" \
  --query "[?contains(name, '$file_ext') && contains(name, '00000000')]" \
  --output tsv | wc -l)

echo "📸 Snapshot files found: $snapshot_count"
```

This is **fast and sufficient for testing**, but production CDC pipelines in `cockroachdb.py` use **row-level timestamps** for accuracy.

---

## 🔗 **Related Documentation**

- **`PARQUET_ANALYSIS_LIMITATION.md`** - Why Parquet can't distinguish snapshots from updates by event type alone
- **`learnings/PARQUET_CDC_PROCESSING_GUIDE.md`** - Detailed strategies for processing Parquet CDC
- **`cockroachdb.py` lines 942-962** - `_determine_cdc_operation()` implementation
- **`cockroachdb.py` lines 1092-1100** - Snapshot cutoff capture logic

---

## Summary

**Question:** Is snapshot vs CDC detection done via file names or row-by-row checks?  
**Answer:** **BOTH**, depending on use case:

- **File naming pattern**: Fast file-level classification (testing, counting)
- **Row-level timestamps**: Precise per-row classification (production CDC)

For production CDC pipelines, **row-level timestamp comparison is essential** because:
1. Parquet `__crdb__event_type='c'` is ambiguous (snapshot OR update)
2. Only timestamp comparison (against captured cutoff) can distinguish them
3. File naming alone is too coarse for accurate event classification


