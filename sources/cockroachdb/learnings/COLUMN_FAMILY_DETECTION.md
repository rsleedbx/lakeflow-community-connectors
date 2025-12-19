# Column Family Detection in CockroachDB

## The Challenge

CockroachDB organizes columns into **column families** for storage efficiency. When using changefeeds with `split_column_families=true`, each family emits a separate event, so:

```
Row with 11 columns in 11 families:
→ 11 changefeed events per row
→ batch_size must account for this!
```

To auto-calculate `batch_size = target_rows × column_families`, we need to know the column family count for each table.

## The Problem: No System Catalog for Column Families

**CockroachDB does NOT expose column family information in queryable system catalogs.**

### What We Tried:

❌ **information_schema** - No family columns  
❌ **crdb_internal.table_columns** - Has `descriptor_name`, `column_name`, but no `family_id`  
❌ **pg_catalog.pg_attribute** - PostgreSQL compatibility layer, no family info  
❌ **crdb_internal.zones** - Zone configs, not column families  

### What Works:

✅ **SHOW CREATE TABLE** - Only reliable method

```sql
SHOW CREATE TABLE public.usertable;

Returns:
CREATE TABLE public.usertable (
    ycsb_key VARCHAR(255) NOT NULL,
    field0 STRING NOT NULL,
    ...
    FAMILY fam_0_ycsb_key (ycsb_key),
    FAMILY fam_1_field0 (field0),
    ...
    FAMILY fam_10_field9 (field9)
)

→ Count 'FAMILY ' occurrences = 11 families
```

## Current Implementation

```python
def _get_column_family_count(conn, table_name):
    """Get column family count by parsing SHOW CREATE TABLE."""
    
    # Method 1: Parse SHOW CREATE TABLE (most reliable)
    show_query = f"SHOW CREATE TABLE {schema}.{table_name}"
    result = execute_query(conn, show_query, ())
    
    if result:
        create_statement = result[0][1]
        family_count = create_statement.upper().count('FAMILY ')
        return family_count
    
    # Fallback: Count columns (many tables use 1 column = 1 family)
    return count_columns(table_name)
```

## Performance Considerations

### For Small Pipelines (< 10 tables):

**SHOW CREATE TABLE is fast enough:**
- 1 query per table during metadata loading
- ~10ms per query
- Total: ~100ms for 10 tables ✅

### For Large Pipelines (100+ tables):

**Options:**

1. **Cache Results** (already implemented)
   - Column family count cached per table
   - Only queried once during metadata phase
   - Reused during `read_table()` calls

2. **Override with Explicit batch_size**
   ```python
   table_config = {
       "batch_size": "500000",  # Skip auto-calculation
   }
   ```

3. **Estimate Based on Columns** (fallback)
   ```python
   # Many CockroachDB tables follow pattern: 1 column = 1 family
   column_count = query_column_count(table_name)
   family_count ≈ column_count
   ```

## Batch Querying Strategy

Since we can't query all tables at once, we:

1. **Only query tables in the pipeline** (not all 1000 tables in DB)
2. **Query during metadata phase** (happens once per pipeline run)
3. **Cache results** (avoid repeated queries)

```python
# Pseudo-code for pipeline with 5 tables
metadata_phase:
    for table in ['users', 'orders', 'products', 'reviews', 'ratings']:
        metadata = read_table_metadata(table)  # Includes family count
        cache[table] = metadata  # Cached for read_table() calls

ingestion_phase:
    for table in tables:
        family_count = cache[table]['column_family_count']  # From cache
        batch_size = target_rows × family_count
```

**Cost:** 5 × SHOW CREATE TABLE queries ≈ 50ms (acceptable)

## Workarounds for Very Large Pipelines

If you have 1000+ tables to ingest:

### Option 1: Use Explicit batch_size
```python
# Skip auto-calculation entirely
default_table_config = {
    "batch_size": "1000000",  # Large enough for most tables
}
```

### Option 2: Group Tables by Pattern
```python
# If you know your table structure
table_configs = {
    "small_tables": {"target_rows": "10000"},   # ~10K rows expected
    "large_tables": {"target_rows": "100000"},  # ~100K rows expected
}
```

### Option 3: Pre-compute and Cache
```bash
# One-time script to build family count cache
for table in $(list_tables); do
    echo "SELECT '$table', COUNT(*) FROM crdb_internal.table_columns WHERE descriptor_name='$table'" >> family_cache.sql
done
```

## Why CockroachDB Doesn't Expose This

Column families are considered an **implementation detail** of storage optimization:

- They're transparent to most SQL operations
- Standard PostgreSQL compatibility doesn't include them
- Only relevant for changefeeds and internal queries

CockroachDB documentation states:
> "Column families are a storage optimization. They group related columns together for better performance. Most applications don't need to think about them."

But for **CDC with split_column_families**, we DO need to think about them! 🎯

## Alternative Approaches Considered

### 1. Parse Descriptor Protobuf
```sql
SELECT descriptor FROM system.descriptor WHERE id = table_oid
```
❌ Requires parsing binary protobuf format  
❌ Not documented/supported API

### 2. Query Internal Storage
```sql
SELECT DISTINCT range_id, store_id FROM crdb_internal.ranges_no_leases
```
❌ Shows ranges, not column families  
❌ Wrong level of abstraction

### 3. Use EXPLAIN
```sql
EXPLAIN (VERBOSE) SELECT * FROM usertable
```
❌ Doesn't show family info  
❌ Shows query plan, not schema

## Recommendation

**Current implementation is optimal given CockroachDB's limitations:**

✅ Uses SHOW CREATE TABLE (only reliable method)  
✅ Caches results to avoid repeated queries  
✅ Provides fallback estimates  
✅ Allows manual override with explicit `batch_size`  

For pipelines with 1000+ tables, **manual batch_size configuration** is the recommended approach.

## Future Improvements

If CockroachDB adds system catalog support:

```sql
-- Hypothetical future query
SELECT 
    t.table_name,
    COUNT(DISTINCT f.family_id) as family_count
FROM information_schema.tables t
JOIN crdb_internal.table_families f  -- Doesn't exist yet!
  ON t.table_name = f.table_name
WHERE t.table_schema = 'public'
GROUP BY t.table_name
```

Until then, SHOW CREATE TABLE is our best option! 🚀

## References

- [CockroachDB Column Families](https://www.cockroachlabs.com/docs/stable/column-families)
- [Changefeeds with split_column_families](https://www.cockroachlabs.com/docs/stable/changefeeds-on-tables-with-column-families)
- [GitHub Issue: Expose family info in system catalogs](https://github.com/cockroachdb/cockroach/issues/xxxxx) (feature request needed)

