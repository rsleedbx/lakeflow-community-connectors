# Dynamic Table Discovery in Community Connectors

## The Problem

Initially, the CockroachDB `ingest.py` tried to instantiate `LakeflowConnect({})` before credentials were available, causing this error:

```
ValueError: Missing required connection parameters: host, database, user
```

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

## The Solution: Dynamic Table Discovery

Since Community Connectors don't support schema-level ingestion, we implemented **dynamic table discovery** for the CockroachDB connector.

### How It Works

#### 1. **Configuration is Optional**

```bash
# Option A: Explicit tables
./createpipeline.sh cockroachdb_connection "customers,orders,products"

# Option B: Dynamic discovery (all tables)
./createpipeline.sh cockroachdb_connection
```

#### 2. **Credential Flow Timeline**

```
1. ingest.py executes
   ❌ Credentials NOT available
   → Reads pipeline config (connection_name, table_list)
   
2. If table_list is empty:
   → Triggers a metadata read:
     spark.read.format("lakeflow_connect")
       .option("databricks.connection", connection_name)
       .option("tableName", "_lakeflow_metadata")
       .load()
   
3. Spark/Databricks Framework resolves connection
   ✅ Credentials NOW available
   → Instantiates LakeflowConnect(options) with credentials
   
4. Metadata read calls connector.list_tables()
   → Returns all available tables
   
5. Build pipeline_spec with discovered tables
   → Proceeds with ingestion
```

#### 3. **Code Implementation**

```python
# If no table_list, discover dynamically
if all_tables is None:
    print("Performing dynamic table discovery...")
    
    # Trigger metadata read to call list_tables()
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", "")  # Empty triggers list_tables()
        .load()
    )
    
    # Collect discovered tables
    all_tables = [row["tableName"] for row in df.collect()]
    print(f"✅ Discovered {len(all_tables)} tables: {all_tables}")
```

## Key Differences Summary

| Feature | Managed Connectors | Community Connectors |
|---------|-------------------|---------------------|
| Schema-level ingestion | ✅ Yes | ❌ No |
| Table-level ingestion | ✅ Yes | ✅ Yes |
| Dynamic table discovery | ✅ Native | ✅ Via `list_tables()` |
| Ingestion gateway | ✅ Yes | ❌ No |
| Credentials available | Immediately | During Spark read |
| Configuration | YAML/JSON | Python `ingest.py` |

## Benefits of Our Approach

1. **Flexibility**: Users can choose explicit tables or discover all tables
2. **Security**: Credentials remain in Unity Catalog, never exposed
3. **Simplicity**: Single command for both modes:
   - `./createpipeline.sh my_connection` → all tables
   - `./createpipeline.sh my_connection "table1,table2"` → specific tables
4. **Robustness**: Proper error handling if discovery fails

## Usage Examples

### Example 1: Discover All Tables
```bash
./scripts/createpipeline.sh cockroachdb_connection
```
Output:
```
Tables: All tables (via dynamic discovery)
✅ Discovered 5 tables: [customers, orders, products, users, events]
Pipeline spec generated for 5 tables
```

### Example 2: Specific Tables
```bash
./scripts/createpipeline.sh cockroachdb_connection "customers,orders"
```
Output:
```
Tables: customers,orders (explicit)
Using specified tables (2): ['customers', 'orders']
Pipeline spec generated for 2 tables
```

### Example 3: Mix with Table Configuration
```python
# In ingest.py, you can customize per-table settings
default_table_config = {
    "initial_scan": "yes",
    "resolved_interval": "10s",
    "split_column_families": "true"
}
```

## References

- [Databricks SQL Server Managed Connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline)
- `libs/spec_parser.py` - Community Connector specification parser
- `pipeline/lakeflow_python_source.py` - Spark DataSource V2 integration
- `pipeline/ingestion_pipeline.py` - Core ingestion logic

