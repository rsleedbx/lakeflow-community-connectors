# Why Dynamic Table Discovery Doesn't Work in Community Connectors

## The Problem

Initially, the CockroachDB `ingest.py` tried to enable dynamic table discovery by instantiating `LakeflowConnect({})`, causing this error:

```
ValueError: Missing required connection parameters: host, database, user
```

## The Fundamental Issue: Chicken-and-Egg Problem

Dynamic table discovery is **architecturally impossible** for Community Connectors due to a chicken-and-egg problem:

## Managed vs. Community Connectors

### Managed Connectors (SQL Server, etc.)
Based on the [Databricks documentation](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline):

✅ **Schema-level ingestion**:
```yaml
objects:
  - schema:
      source_catalog: test
      source_schema: ingestion_whole_schema
      destination_catalog: main
      destination_schema: my_schema
```

✅ **Table-level ingestion**:
```yaml
objects:
  - table:
      source_catalog: test
      source_schema: ingestion_demo
      source_table: lineitem
      destination_catalog: main
      destination_schema: my_schema
```

✅ **Ingestion gateways** for CDC
✅ **Native Databricks integration**

### Community Connectors (CockroachDB, GitHub, etc.)

Based on `libs/spec_parser.py`:

```python
class ObjectSpec(BaseModel):
    """
    Wrapper for an object in the pipeline spec.

    Currently only the `table` key is supported.
    """
    table: TableSpec  # Only table objects, no schema objects
```

❌ **No schema-level ingestion** - `schema` objects are not supported  
❌ **No ingestion gateway architecture**  
✅ **Table-level ingestion only**  
✅ **Dynamic table discovery via `list_tables()`** (our implementation)

```
1. To call list_tables(), we need credentials
2. Credentials are only available when Spark instantiates LakeflowConnect
3. Spark only instantiates LakeflowConnect during a read operation
4. To start a read operation, we need a valid pipeline_spec with tables
5. But we can't build the pipeline_spec without knowing which tables exist!
```

**This is a circular dependency that cannot be resolved.**

## Why Managed Connectors Can Do It

Managed Connectors (SQL Server, etc.) use a different architecture:
- **Ingestion Gateway**: A separate service that maintains a persistent connection
- **Native Databricks Integration**: Credentials are managed at the gateway level
- **Schema Discovery**: Happens in the gateway, before pipeline execution

Community Connectors lack this infrastructure.

## The Solution: Explicit Table Specification

Since dynamic discovery is impossible, CockroachDB connector requires **explicit table specification**.

### How It Works

#### 1. **Discover Tables Manually**

Connect to your CockroachDB cluster and run:

```sql
-- For default public schema
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public';

-- For YCSB workload (used in testing)
-- Returns: usertable
```

#### 2. **Specify Tables in Configuration**

```bash
# For YCSB workload (single table)
./createpipeline.sh cockroachdb_connection "usertable"

# For multiple tables
./createpipeline.sh cockroachdb_connection "customers,orders,products"
```

#### 3. **Pipeline Configuration**

```python
# In DLT pipeline configuration
{
  "source_name": "cockroachdb",
  "connection_name": "cockroachdb_connection",
  "table_list": "usertable"  # Required!
}
```

#### 4. **Credential Flow Timeline**

```
1. ingest.py executes
   ❌ Credentials NOT available
   → Reads pipeline config (connection_name, table_list)
   → Validates table_list is provided
   → Builds pipeline_spec with specified tables
   
2. ingestion_pipeline.py executes
   → Triggers metadata read for each table
   
3. Spark reads metadata
   → spark.read.format("lakeflow_connect")
       .option("databricks.connection", connection_name)
       .option("tableName", "_lakeflow_metadata")
       .load()
   
4. Spark/Databricks Framework resolves connection
   ✅ Credentials NOW available
   → Instantiates LakeflowConnect(options) with credentials
   → Calls read_table_metadata() for each specified table
   
5. Ingestion proceeds with specified tables
```

## Key Differences Summary

| Feature | Managed Connectors | Community Connectors |
|---------|-------------------|---------------------|
| Schema-level ingestion | ✅ Yes | ❌ No |
| Table-level ingestion | ✅ Yes | ✅ Yes |
| Dynamic table discovery | ✅ Native (via gateway) | ❌ No (credentials unavailable) |
| Ingestion gateway | ✅ Yes | ❌ No |
| Credentials available | Gateway maintains connection | Only during Spark read |
| Table specification | Optional (can auto-discover) | Required (explicit list) |
| Configuration | YAML/JSON | Python `ingest.py` |

## How to Discover Tables

Since automatic discovery isn't possible, manually discover tables using SQL:

### Option 1: Direct SQL Query

```sql
-- Connect to your CockroachDB cluster
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;
```

### Option 2: Using CockroachDB CLI

```bash
cockroach sql --url "postgresql://user:pass@host:26257/database?sslmode=verify-full" \
  --execute "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
```

### Option 3: Using test_local.py

```bash
# Run the test script which lists available tables
cd sources/cockroachdb/scripts
python test_local.py --workload ycsb
# Output includes: "✅ List tables: ['usertable']"
```

## Usage Examples

### Example 1: YCSB Workload (Single Table)
```bash
# Discover tables
$ cockroach sql --url "$DB_URL" --execute \
  "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
# Returns: usertable

# Create pipeline
$ ./scripts/createpipeline.sh cockroachdb_connection "usertable"
```
Output:
```
Tables: usertable
Pipeline spec generated for 1 tables
Tables to ingest: usertable
```

### Example 2: Multiple Tables
```bash
# Create pipeline with multiple tables
$ ./scripts/createpipeline.sh cockroachdb_connection "customers,orders,products"
```
Output:
```
Tables: customers,orders,products
Pipeline spec generated for 3 tables
Tables to ingest: customers, orders, products
```

### Example 3: Customize Table Configuration
```python
# In ingest.py, you can customize per-table settings
default_table_config = {
    "initial_scan": "yes",         # Full scan + continue streaming
    "resolved_interval": "10s",    # Resolved timestamps every 10 seconds
    "split_column_families": "true" # Emit separate events for column families
}
```

## Conclusion

**Dynamic table discovery is not supported in Community Connectors** due to architectural limitations:

1. ❌ No ingestion gateway to maintain persistent connections
2. ❌ Credentials only available during Spark execution
3. ❌ Chicken-and-egg problem: need tables to trigger connection, need connection to discover tables

**Solution:** Explicitly specify `table_list` in pipeline configuration after manually discovering tables via SQL queries.

For YCSB workload testing:
```bash
./scripts/createpipeline.sh cockroachdb_connection "usertable"
```

## References

- [Databricks SQL Server Managed Connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline) - Shows schema-level ingestion
- `libs/spec_parser.py` - Community Connector specification parser (only supports `table` objects)
- `pipeline/lakeflow_python_source.py` - Spark DataSource V2 integration (credentials injected here)
- `pipeline/ingestion_pipeline.py` - Core ingestion logic
- `sources/cockroachdb/scripts/test_local.py` - Manual testing tool that lists available tables

