# CDC Operation Column Design

## Overview

Both CDC ingestion modes (`append-only` and `update-delete`) now preserve the `_cdc_operation` column in the target Delta table for consistent monitoring and observability.

## Design Decision

### Previous Approach (Inconsistent)
- **Append-only mode**: `_cdc_operation` column present (DELETE, UPSERT)
- **Update-delete mode**: `_cdc_operation` column removed
- **Problem**: Monitoring queries needed mode-aware logic

### Current Approach (Consistent)
- **Both modes**: `_cdc_operation` column present
- **Append-only**: Shows each CDC event (DELETE, UPSERT)
- **Update-delete**: Shows last operation on each row (UPSERT only)

## Benefits

### 1. **Simplified Monitoring**
```python
# Same query works for both modes!
df = spark.read.table(target_table)
df.groupBy("_cdc_operation").count().show()
```

### 2. **Better Observability**
```sql
-- When was this row last modified?
SELECT ycsb_key, _cdc_operation, _cdc_timestamp
FROM target_table
WHERE ycsb_key = 12345;

-- Which rows were recently updated?
SELECT COUNT(*) 
FROM target_table 
WHERE _cdc_operation = 'UPSERT' 
  AND _cdc_timestamp > CURRENT_TIMESTAMP() - INTERVAL 1 HOUR;
```

### 3. **Audit Trail**
Even in `update-delete` mode, you can see:
- When each row was last modified (`_cdc_timestamp`)
- What operation created/updated it (`_cdc_operation`)

### 4. **No Special Cases**
- No need to check if column exists
- No mode-specific query logic
- Consistent schema across both modes

## Column Semantics by Mode

### Append-Only Mode
| Column | Meaning |
|--------|---------|
| `_cdc_operation` | CDC event type: `DELETE` or `UPSERT` |
| `_cdc_timestamp` | When this CDC event occurred |
| Row count | Total CDC events (including all updates/deletes) |

**Example:**
```
ycsb_key | field0 | _cdc_operation | _cdc_timestamp
---------|--------|----------------|-------------------
123      | v1     | UPSERT         | 2026-01-28 10:00
123      | v2     | UPSERT         | 2026-01-28 10:05
123      | NULL   | DELETE         | 2026-01-28 10:10
456      | v1     | UPSERT         | 2026-01-28 10:00
```
**Total rows: 4** (all events preserved)

### Update-Delete Mode
| Column | Meaning |
|--------|---------|
| `_cdc_operation` | Always `UPSERT` (last operation on this row) |
| `_cdc_timestamp` | When this row was last modified |
| Row count | Current state (deduplicated) |

**Example:**
```
ycsb_key | field0 | _cdc_operation | _cdc_timestamp
---------|--------|----------------|-------------------
456      | v1     | UPSERT         | 2026-01-28 10:00
```
**Total rows: 1** (key 123 was deleted, key 456 remains)

## Implementation Details

### Code Change
```python
# OLD (removed _cdc_operation)
data_columns = [col for col in staging_df.columns if col != '_cdc_operation']

# NEW (keeps _cdc_operation)
data_columns = [col for col in staging_df.columns]
```

### MERGE Logic
```python
.whenMatchedUpdate(
    condition="source._cdc_operation = 'UPSERT' AND source._cdc_timestamp > target._cdc_timestamp",
    set={
        "field0": "source.field0",
        "_cdc_operation": "source._cdc_operation",  # ← Preserved!
        "_cdc_timestamp": "source._cdc_timestamp"
    }
)
```

## Storage Impact

### Minimal Overhead
- `_cdc_operation` is a STRING column storing "DELETE" or "UPSERT" (6-7 bytes)
- Compared to typical data columns, this is negligible
- Example: `field0` (VARCHAR) can be 100+ bytes

### Cost vs. Benefit
- **Cost**: ~7 bytes per row
- **Benefit**: 
  - Unified monitoring code
  - Better observability
  - Simpler maintenance
  - No mode-specific logic

**Verdict**: The benefits far outweigh the minimal storage cost.

## Query Patterns

### Monitoring Dashboard
```sql
-- Works for both modes!
SELECT 
  _cdc_operation,
  COUNT(*) as row_count,
  MAX(_cdc_timestamp) as last_event
FROM target_table
GROUP BY _cdc_operation
```

### Recent Changes (Last Hour)
```sql
SELECT *
FROM target_table
WHERE _cdc_timestamp > CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY _cdc_timestamp DESC
```

### Data Quality Check
```python
# Ensure all operations are valid
df = spark.read.table(target_table)
invalid = df.filter(~df._cdc_operation.isin(['DELETE', 'UPSERT']))
assert invalid.count() == 0, "Found invalid _cdc_operation values"
```

## Comparison with Other Systems

### Snowflake CDC
- Uses separate `METADATA$ACTION` column
- Values: "INSERT", "DELETE"
- Similar concept, different naming

### Debezium
- Uses `__op` column
- Values: "c" (create), "u" (update), "d" (delete)
- Requires decoding logic

### Our Approach
- Uses `_cdc_operation` column
- Values: "UPSERT", "DELETE"
- Human-readable, no decoding needed
- Consistent across both modes

## Future Considerations

### Column Family Support
When adding column family support:
- `_cdc_operation` will still be preserved
- Additional column: `_cdc_family` (which column family changed)
- Still human-readable and monitorable

### Multi-Table Sync
If syncing multiple tables:
- `_cdc_operation` remains consistent across all tables
- Simplifies cross-table monitoring queries

## Summary

Preserving `_cdc_operation` in both CDC modes:
- ✅ Simplifies monitoring code (no special cases)
- ✅ Improves observability (can see last operation)
- ✅ Minimal storage overhead (~7 bytes/row)
- ✅ Consistent schema across modes
- ✅ Better developer experience

**Result**: A more maintainable and observable CDC pipeline! 🎉
