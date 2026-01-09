# CockroachDB Changefeed: Multi-Catalog/Schema Support

**Date:** 2025-12-23  
**Issue:** File naming collision risk with duplicate table names across catalogs/schemas  
**Status:** ⚠️ **Requires attention for multi-database deployments**

---

## 🔍 **The Problem**

CockroachDB changefeed Parquet files **only include the table name** in the filename, not the database or schema:

### Current Filename Format

```
TIMESTAMP-JOBID-SHARD-NODE-SEQUENCE-<TABLE_NAME>-VERSION.parquet
```

### Actual Example

```
202512230147140107743420000000000-6af85e13912c5e8d-1-44-00000000-single_op_test-1.parquet
                                                                    ^^^^^^^^^^^^^^^
                                                                    Table name only!
```

### The Collision Risk

If you have duplicate table names across databases/schemas:

```sql
-- Database 1
CREATE DATABASE ecommerce;
CREATE TABLE ecommerce.public.orders (...);

-- Database 2
CREATE DATABASE warehouse;
CREATE TABLE warehouse.public.orders (...);

-- Database 3
CREATE DATABASE analytics;
CREATE TABLE analytics.staging.orders (...);
```

All three would produce files like:
```
...-orders-1.parquet  ❌ Which database/schema is this from?
```

---

## 🧪 **Testing the Current Behavior**

### Test 1: Check Actual Filenames

```bash
# Our test with table 'single_op_test' produces:
single-op-test-parquet/2025-12-23/
  └── 202512230147140107743420000000000-...-single_op_test-1.parquet
                                           ^^^^^^^^^^^^^^^
                                           Table name only
```

✅ **Confirmed:** Only table name appears in filename, no database/schema information.

### Test 2: Directory Structure

CockroachDB changefeeds allow specifying a path prefix in the URI:

```sql
CREATE CHANGEFEED FOR TABLE orders
INTO 'azure://container/ecommerce/public/?...'  -- Path prefix
WITH format = 'parquet', ...;
```

This creates:
```
ecommerce/public/2025-12-23/
  └── ...-orders-1.parquet
```

✅ **Workaround Available:** Use path prefixes to separate namespaces.

---

## ✅ **Recommended Solutions**

### Solution 1: Use Path Prefixes (Recommended)

Create separate changefeeds with database-specific path prefixes:

```sql
-- Changefeed for ecommerce.public.orders
CREATE CHANGEFEED FOR TABLE ecommerce.public.orders
INTO 'azure://container/ecommerce/public/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH format = 'parquet', updated, resolved = '10s';

-- Changefeed for warehouse.public.orders
CREATE CHANGEFEED FOR TABLE warehouse.public.orders
INTO 'azure://container/warehouse/public/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH format = 'parquet', updated, resolved = '10s';

-- Changefeed for analytics.staging.orders
CREATE CHANGEFEED FOR TABLE analytics.staging.orders
INTO 'azure://container/analytics/staging/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH format = 'parquet', updated, resolved = '10s';
```

**File Structure:**
```
container/
├── ecommerce/
│   └── public/
│       └── 2025-12-23/
│           └── ...-orders-1.parquet
├── warehouse/
│   └── public/
│       └── 2025-12-23/
│           └── ...-orders-1.parquet
└── analytics/
    └── staging/
        └── 2025-12-23/
            └── ...-orders-1.parquet
```

✅ **Advantages:**
- Clear separation by database/schema
- No filename collisions
- Easy to configure access control per database
- Aligns with Delta Lake/Unity Catalog conventions

---

### Solution 2: Update Connector to Include Path Prefix

Our `cockroachdb.py` connector should be updated to:

1. **Accept catalog/schema information:**

```python
credentials = {
    "cockroachdb_url": "postgresql://...",
    "azure_storage_account": "...",
    "azure_storage_key": "...",
    "azure_storage_container": "changefeed-events",
    "azure_path_prefix": "ecommerce/public",  # New parameter!
    "catalog": "ecommerce",   # For metadata tracking
    "schema": "public"        # For metadata tracking
}
```

2. **Include catalog/schema in changefeed URI:**

```python
def _ensure_azure_changefeed(self, table_name, table_options):
    # Extract catalog/schema from options
    catalog = self.options.get('catalog', 'default')
    schema = self.options.get('schema', 'public')
    
    # Build path with hierarchy
    path_prefix = f"{catalog}/{schema}"
    
    # Construct URI with path prefix
    azure_uri = f"azure://{container}/{path_prefix}/?..."
    
    # Create changefeed with fully qualified table name
    fq_table_name = f"{catalog}.{schema}.{table_name}"
    sql = f"CREATE CHANGEFEED FOR TABLE {fq_table_name} INTO '{azure_uri}' ..."
```

3. **Update file listing to use correct prefix:**

```python
def _list_azure_parquet_files(self, table_name):
    catalog = self.options.get('catalog', 'default')
    schema = self.options.get('schema', 'public')
    
    # List files with catalog/schema prefix
    prefix = f"{catalog}/{schema}/"
    blobs = container_client.list_blobs(name_starts_with=prefix)
    
    # Filter by table name
    return [b for b in blobs if table_name in b.name]
```

---

### Solution 3: Use Separate Changefeeds Per Database

Create entirely separate changefeed jobs for each database:

```sql
-- Job 1: All tables in ecommerce database
CREATE CHANGEFEED FOR TABLE ecommerce.public.*
INTO 'azure://container/ecommerce/?...'
WITH format = 'parquet', ...;

-- Job 2: All tables in warehouse database
CREATE CHANGEFEED FOR TABLE warehouse.public.*
INTO 'azure://container/warehouse/?...'
WITH format = 'parquet', ...;
```

✅ **Advantages:**
- Simpler management (one job per database)
- Automatic handling of new tables
- Clear separation

❌ **Limitations:**
- CockroachDB may not support wildcard table patterns in changefeeds (need to verify)
- More changefeed jobs to manage

---

## 🔧 **Implementation Changes Required**

### 1. Update `cockroachdb.py`

```python
class LakeflowConnect:
    def __init__(self, credentials):
        # ... existing code ...
        
        # NEW: Extract catalog/schema information
        self.catalog = credentials.get('catalog', 'defaultdb')
        self.schema = credentials.get('schema', 'public')
        self.azure_path_prefix = credentials.get('azure_path_prefix')
        
        # Auto-construct path prefix if not provided
        if not self.azure_path_prefix and self.mode == 'azure_parquet':
            self.azure_path_prefix = f"{self.catalog}/{self.schema}"
    
    def _ensure_azure_changefeed(self, table_name, table_options):
        # Use fully qualified table name
        fq_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        # Construct URI with path prefix
        azure_uri = f"azure://{self.azure_storage_container}/{self.azure_path_prefix}/?..."
        
        sql = f"""
        CREATE CHANGEFEED FOR TABLE {fq_table_name}
        INTO '{azure_uri}'
        WITH ...
        """
        # ... rest of implementation ...
```

### 2. Update Test Credentials

```json
{
  "cockroachdb_url": "postgresql://user:pass@host:26257/ecommerce?sslmode=require",
  "azure_storage_account": "cockroachcdc1766453970",
  "azure_storage_key": "...",
  "azure_storage_container": "changefeed-events",
  "azure_path_prefix": "ecommerce/public",
  "catalog": "ecommerce",
  "schema": "public"
}
```

### 3. Update Documentation

Add examples showing multi-database setup in:
- `README.md`
- `TESTING_GUIDE.md`
- `cockroachdb.ipynb`

---

## 📊 **Current Support Matrix**

| Scenario | Current Support | Workaround | Recommended Fix |
|----------|----------------|------------|-----------------|
| **Single database, single schema** | ✅ Works | N/A | None needed |
| **Multiple schemas, unique table names** | ✅ Works | N/A | None needed |
| **Multiple schemas, duplicate table names** | ⚠️ Collision risk | Manual path prefixes | Solution 1 or 2 |
| **Multiple databases, duplicate table names** | ⚠️ Collision risk | Manual path prefixes | Solution 1 or 2 |

---

## 🚀 **Action Items**

### Immediate (For Current Users)

1. **Document the limitation** in README ✅ (this document)
2. **Provide manual workaround** using path prefixes ✅
3. **Test multi-database scenario** to confirm behavior

### Short-term (Next Release)

1. **Add `catalog` and `schema` parameters** to `cockroachdb.py`
2. **Auto-construct path prefixes** using catalog/schema
3. **Update test scripts** to demonstrate multi-database usage
4. **Add validation** to warn if duplicate table names detected

### Long-term (Future Enhancement)

1. **Auto-detect catalog/schema** from `cockroachdb_url`
2. **Support multiple databases** in single connector instance
3. **Add metadata tracking** for catalog/schema in CDC events
4. **Create Unity Catalog integration** with automatic schema mapping

---

## 💡 **Best Practices**

1. **Always use path prefixes** when working with multiple databases/schemas
2. **Use fully qualified table names** in `CREATE CHANGEFEED` statements
3. **Organize by hierarchy:** `catalog/schema/table` in storage paths
4. **Document your namespace** in your pipeline configuration
5. **Use Unity Catalog External Locations** with database-specific paths

---

## 🔗 **References**

- **Actual Filename:** `202512230147140107743420000000000-6af85e13912c5e8d-1-44-00000000-single_op_test-1.parquet`
- **Test Script:** `sources/cockroachdb/scripts/test_single_operations.sh`
- **Connector:** `sources/cockroachdb/cockroachdb.py`
- **CockroachDB Docs:** [Changefeed Sinks](https://www.cockroachlabs.com/docs/stable/changefeed-sinks)

---

**Last Updated:** 2025-12-23 18:30 PST  
**Status:** ⚠️ **Action required for multi-database deployments**  
**Priority:** Medium (affects multi-tenant or multi-database use cases)




