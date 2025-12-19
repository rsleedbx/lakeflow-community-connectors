# Dynamic Batch Size Calculation

## Overview

The CockroachDB connector now intelligently calculates `batch_size` based on each table's column family structure, eliminating the need for manual tuning.

## The Problem

CockroachDB organizes columns into **column families** for storage efficiency. When using changefeeds with `split_column_families=true` (required for tables with multiple column families), each column family emits a **separate event per row**.

### Example: YCSB usertable

```sql
-- Table has 11 columns
CREATE TABLE usertable (
    ycsb_key VARCHAR PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    ...
    field9 TEXT
);

-- CockroachDB organizes into 9 column families
-- Each row → 9 changefeed events
```

### The Confusion: Events vs Rows

Users think in **ROWS**, but `batch_size` controls **EVENTS**:

```
batch_size = 100 events
↓
100 events ÷ 9 events/row = ~11 rows ❌ (way too few!)

batch_size = 135,000 events  
↓
135,000 events ÷ 9 events/row = 15,000 rows ✅ (perfect!)
```

## The Solution: Dynamic Calculation

### 1. Query Column Family Count

```sql
SELECT COUNT(DISTINCT family_name) as family_count
FROM crdb_internal.table_columns
WHERE table_schema = 'public' 
  AND table_name = 'usertable';
-- Result: 9 column families
```

### 2. Auto-Calculate Batch Size

```python
target_rows = 15000  # User-friendly: specify desired ROWS
column_families = 9   # Detected automatically
batch_size = target_rows × column_families
# Result: 135,000 events
```

### 3. Cache for Performance

```python
# Query once per table, cache the result
self._column_family_cache[table_name] = column_family_count
```

## Configuration

### Approach 1: Specify Target Rows (Recommended)

```python
table_config = {
    "target_rows": "15000",  # Think in ROWS (intuitive!)
    # batch_size auto-calculated based on column families
}
```

**How it works:**
1. Connector queries `crdb_internal.table_columns`
2. Detects 9 column families
3. Calculates `batch_size = 15,000 × 9 = 135,000`

### Approach 2: Manual Override

```python
table_config = {
    "batch_size": "200000",  # Explicit events (overrides target_rows)
}
```

Use this when you:
- Need precise control over event count
- Want to override the auto-calculation
- Are migrating from older configurations

## Metadata Storage

The column family count is stored in table metadata for future use:

```python
metadata = {
    "primary_keys": ["ycsb_key"],
    "cursor_field": "_cdc_updated",
    "ingestion_type": "cdc",
    "column_family_count": 9  # ← Stored here
}
```

## Benefits

### ✅ Intuitive Configuration
Think in **ROWS** (what you care about), not events (implementation detail)

### ✅ Adapts Automatically
Different tables → different column families → automatically adjusted

### ✅ Schema Evolution
If table structure changes, batch_size auto-adjusts on next run

### ✅ No Manual Tuning
Connector figures out optimal settings for each table

### ✅ Backward Compatible
Old configs with explicit `batch_size` still work

## Example Output

```
📊 Dynamic batch size calculation:
   Target rows: 15,000
   Column families: 9
   Events per row: 9
   → Calculated batch_size: 135,000 events

⏱️  Step 4: Executing changefeed query...
✅ Changefeed query executed in 45.23s

📦 Processing changefeed events...
   Event count: 135,000
   Row count: ~15,000 ✅
```

## Edge Cases

### Single Column Family

```sql
-- Simple table with all columns in one family
CREATE TABLE simple (id INT PRIMARY KEY, name TEXT);
-- Column families: 1
-- target_rows=10000 → batch_size=10,000 (no multiplication needed)
```

### Detection Failure

If `crdb_internal.table_columns` is unavailable or query fails:
- Defaults to `column_families = 1`
- Logs warning message
- Continues with safe fallback

### Zero Rows Returned

If query returns 0 or NULL:
- Enforces minimum `column_families = 1`
- Ensures valid batch_size calculation

## Migration Guide

### From Hardcoded batch_size

**Before:**
```python
"batch_size": "100000"  # Manual tuning required
```

**After:**
```python
"target_rows": "15000"  # Intuitive and adaptive
```

### From Manual Calculation

**Before:**
```python
# Had to manually discover column families
# psql -c "SELECT COUNT(DISTINCT family_name) FROM crdb_internal.table_columns WHERE table_name='usertable'"
# Result: 9
# Manual calc: 15000 * 9 = 135000
"batch_size": "135000"
```

**After:**
```python
# Automatic!
"target_rows": "15000"
```

## Performance Considerations

### Query Cost
- **Single query per table** (lightweight)
- **Cached** for subsequent reads
- **~10ms** execution time

### Memory Impact
Minimal - cache stores one integer per table:
```python
self._column_family_cache = {
    "usertable": 9,
    "customers": 3,
    # ... etc
}
```

## Troubleshooting

### Still Getting Partial Rows?

Check that `split_column_families` is enabled:
```python
"split_column_families": "true"  # Required for multi-family tables
```

### Want More Rows Per Batch?

Increase `target_rows`:
```python
"target_rows": "50000"  # Larger batches
```

### Need Exact Event Control?

Use explicit `batch_size`:
```python
"batch_size": "500000"  # Overrides target_rows
```

## Related Documentation

- [CockroachDB Column Families](https://www.cockroachlabs.com/docs/stable/column-families)
- [Changefeeds with split_column_families](https://www.cockroachlabs.com/docs/stable/changefeeds-on-tables-with-column-families)
- [crdb_internal system catalog](https://www.cockroachlabs.com/docs/stable/crdb-internal)

## Implementation Details

See:
- `cockroachdb.py::_get_column_family_count()` - Query logic
- `cockroachdb.py::read_table_metadata()` - Metadata storage
- `cockroachdb.py::read_table()` - Dynamic batch_size calculation
- `ingest.py` - Configuration example

