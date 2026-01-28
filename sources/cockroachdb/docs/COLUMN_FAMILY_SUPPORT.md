# Column Family Support in CockroachDB CDC

## Overview

CockroachDB supports **column families** to optimize storage and performance by grouping frequently accessed columns together. When using changefeeds with `split_column_families=true`, CockroachDB writes separate Parquet files for each column family, creating **fragments** that need to be merged.

This notebook now supports column family CDC ingestion!

## What are Column Families?

### Definition
A column family is a group of columns stored together in CockroachDB. By default, all columns belong to a single "primary" column family.

### When to Use Column Families
- **Wide tables** with many columns (50+)
- **Read patterns** where only some columns are accessed frequently
- **Storage optimization** for large TEXT/BLOB columns
- **Performance tuning** for specific access patterns

### Example
```sql
CREATE TABLE users (
  user_id INT PRIMARY KEY,
  email STRING,
  name STRING,
  -- Frequently accessed columns
  FAMILY frequently_read (user_id, email, name),
  
  -- Large, infrequently accessed columns
  profile_data JSONB,
  avatar_blob BYTES,
  FAMILY large_data (profile_data, avatar_blob)
);
```

---

## How Column Families Affect CDC

### Without `split_column_families` (Default)
```
One row update → One Parquet file with ALL columns
```

**Parquet file example:**
```
user_id | email        | name  | profile_data | avatar_blob
--------|--------------|-------|--------------|-------------
123     | joe@acme.com | Joe   | {...}        | 0xABCD...
```

### With `split_column_families=true`
```
One row update → Multiple Parquet files (one per column family)
```

**Parquet files (fragments):**

**File 1 (frequently_read family):**
```
user_id | email        | name  | profile_data | avatar_blob
--------|--------------|-------|--------------|-------------
123     | joe@acme.com | Joe   | NULL         | NULL
```

**File 2 (large_data family):**
```
user_id | email | name | profile_data | avatar_blob
--------|-------|------|--------------|-------------
123     | NULL  | NULL | {...}        | 0xABCD...
```

**After merging:**
```
user_id | email        | name  | profile_data | avatar_blob
--------|--------------|-------|--------------|-------------
123     | joe@acme.com | Joe   | {...}        | 0xABCD...
```

---

## Implementation in This Notebook

### Function: `merge_column_family_fragments()`

Located in **Cell 5**, this function merges fragments into complete rows.

#### How It Works

```python
def merge_column_family_fragments(df, primary_key_columns, spark):
    # Group by: PK + timestamp + operation
    group_by_cols = primary_key_columns + ['_cdc_timestamp', '_cdc_operation']
    
    # Aggregate using first(col, ignorenulls=True)
    # This coalesces NULL values from different fragments
    agg_exprs = [
        F.first(col, ignorenulls=True).alias(col) 
        for col in data_columns
    ]
    
    # Merge fragments
    df_merged = df.groupBy(*group_by_cols).agg(*agg_exprs)
    return df_merged
```

#### Key Points

1. **Grouping Key**: `(PK + timestamp + operation)`
   - Preserves ALL distinct CDC events
   - Same key can have UPDATE and DELETE at same timestamp

2. **Aggregation**: `first(col, ignorenulls=True)`
   - Takes first non-NULL value for each column
   - Merges NULL values from different fragments
   - Each fragment has data for ONE column family (other columns are NULL)

3. **Streaming Compatible**
   - Works with Spark Structured Streaming
   - No need for `.count()` or pre-detection
   - Harmless no-op if no column families exist

---

## CDC Modes with Column Families

### Mode 1: Append-Only + Column Families

**Function:** `ingest_cdc_append_only_multi_family()`

**Use Case:**
- Audit logs with wide tables
- Full history tracking
- Column families for storage optimization

**Behavior:**
```
1. Read CDC files (multiple fragments per event)
2. Transform __crdb__* columns
3. Merge column family fragments
4. Write ALL events to Delta (no deduplication)
```

**Example:**
```python
cdc_mode = "column-family"  # In Cell 1 (but note: this uses merge mode)

# Or call directly:
query = ingest_cdc_append_only_multi_family(
    storage_account_name=storage_account_name,
    container_name=container_name,
    ...
    primary_key_columns=["user_id"],
    spark=spark
)
```

### Mode 2: Update-Delete + Column Families

**Function:** `ingest_cdc_with_merge_multi_family()`

**Use Case:**
- Current state replication
- Column families for performance
- Production CDC with UPDATE/DELETE support

**Behavior:**
```
1. Read CDC files (multiple fragments per event)
2. Transform __crdb__* columns
3. Merge column family fragments
4. Stream to staging table (Serverless-compatible)
5. Deduplicate by primary key (batch mode)
6. Apply MERGE to target Delta table
```

**Example:**
```python
cdc_mode = "column-family"  # In Cell 1

# This mode is automatically selected in Cell 11
```

---

## When to Use Column Family Mode

### ✅ **Use Column Families When:**

1. **CockroachDB Changefeed Created With:**
   ```sql
   CREATE CHANGEFEED FOR TABLE users
   INTO 'azure://...'
   WITH format='parquet', split_column_families=true;
   ```

2. **Table Has Multiple Column Families:**
   ```sql
   SHOW CREATE TABLE users;
   -- Output shows FAMILY clauses
   ```

3. **Wide Tables (50+ columns)**
   - Storage optimization
   - Reduce scan overhead

4. **Large Infrequently-Accessed Columns**
   - JSONB, TEXT, BYTES
   - Separate from frequently-read columns

### ❌ **Don't Use Column Families When:**

1. **Changefeed Uses Default Settings:**
   ```sql
   CREATE CHANGEFEED FOR TABLE users
   INTO 'azure://...'
   WITH format='parquet';  -- split_column_families defaults to false
   ```

2. **Narrow Tables (<20 columns)**
   - Overhead not worth it
   - Use "append-only" or "update-delete" modes

3. **All Columns Accessed Together**
   - No selective access pattern
   - Single column family is sufficient

---

## Performance Considerations

### Overhead
- **Additional GroupBy**: Adds shuffle operation
- **Aggregation**: `first(col, ignorenulls=True)` for each column
- **Cost**: ~10-20% slower than single family mode

### When Overhead is Worth It
- **Wide tables** (50+ columns) with column families
- **Storage savings** outweigh compute cost
- **CockroachDB requires** `split_column_families=true`

### When to Avoid
- **Narrow tables** without column families
- **Performance-critical** ingestion (use single family mode)
- **Simple schemas** without selective access patterns

---

## Troubleshooting

### Issue: "No column family fragmentation detected"

**Symptom:**
```
✅ No column family fragmentation detected
   Returning original DataFrame unchanged
```

**Cause:** Changefeed was created **without** `split_column_families=true`

**Solution:** Use "append-only" or "update-delete" mode instead

---

### Issue: Missing Columns After Merge

**Symptom:** Some columns are NULL in target table

**Possible Causes:**
1. **Incomplete fragments** (CDC files not yet flushed)
2. **Metadata columns excluded** from aggregation
3. **Column family definition** changed in CockroachDB

**Debug:**
```python
# In Cell 5, enable debug mode:
def merge_column_family_fragments(df, primary_key_columns, spark):
    print(f"Grouping by: {primary_key_columns}")
    print(f"Data columns: {data_columns}")
    print(f"Metadata columns: {metadata_columns}")
    ...
```

---

## Example: End-to-End Column Family CDC

### Step 1: Create Table with Column Families (CockroachDB)

```sql
CREATE TABLE ecommerce_orders (
  order_id INT PRIMARY KEY,
  customer_id INT,
  order_date TIMESTAMP,
  total_amount DECIMAL(10,2),
  -- Frequently accessed
  FAMILY order_header (order_id, customer_id, order_date, total_amount),
  
  shipping_address TEXT,
  billing_address TEXT,
  -- Addresses accessed less frequently
  FAMILY addresses (shipping_address, billing_address),
  
  order_notes TEXT,
  metadata JSONB,
  -- Large, rarely accessed
  FAMILY large_data (order_notes, metadata)
);
```

### Step 2: Create Changefeed with Column Families

```sql
CREATE CHANGEFEED FOR TABLE ecommerce_orders
INTO 'azure://cockroachcdc1768934658.blob.core.windows.net/changefeed-events?AUTH=specified&AZURE_ACCOUNT_KEY=...'
WITH 
  format='parquet',
  split_column_families=true,  -- ← REQUIRED
  updated_timestamps=true;
```

### Step 3: Configure Notebook (Cell 1)

```python
# Table names
source_table = "ecommerce_orders"
target_table = f"ecommerce_orders_{cdc_mode.replace('-', '_')}"

# Primary key columns
primary_key_columns = ["order_id"]

# CDC Mode
cdc_mode = "column-family"  # ← Use column family mode
```

### Step 4: Run Ingestion (Cell 11)

The notebook will automatically:
1. Read CDC files from Azure
2. Merge column family fragments
3. Apply MERGE logic to target Delta table

### Step 5: Verify Results (Cell 12)

```sql
SELECT * FROM main.robert_lee_cockroachdb.ecommerce_orders_column_family
WHERE order_id = 12345;
```

**Result:** Complete row with all columns merged from all column families!

---

## Summary

| Mode | Single Family | Multi Family (Column Families) |
|------|---------------|-------------------------------|
| **Append-Only** | `ingest_cdc_append_only_single_family()` | `ingest_cdc_append_only_multi_family()` |
| **Update-Delete** | `ingest_cdc_with_merge_single_family()` | `ingest_cdc_with_merge_multi_family()` |
| **Changefeed Requirement** | Default (`split_column_families=false`) | `split_column_families=true` |
| **Performance** | Faster (no merge overhead) | Slower (adds groupBy/aggregate) |
| **Use Case** | Most tables | Wide tables with column families |

**Bottom Line:** Use column family mode **only if** your CockroachDB changefeed was created with `split_column_families=true`. Otherwise, use single family modes for better performance!

---

## References

- [CockroachDB Column Families](https://www.cockroachlabs.com/docs/stable/column-families.html)
- [CockroachDB Changefeeds](https://www.cockroachlabs.com/docs/stable/changefeed-for.html)
- [Spark `first()` Aggregation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.first.html)
- `cockroachdb.py`: `merge_column_family_fragments()` (lines 5450-5723)
