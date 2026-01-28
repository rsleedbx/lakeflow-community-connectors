## Is your feature request related to a problem? Please describe.

When consuming CockroachDB changefeed Parquet files with CDC consumers (Databricks, AWS Glue, EMR, Azure Synapse), there is no way to automatically determine which columns are the primary key. This forces manual configuration in every downstream consumer and breaks the self-describing nature of Parquet files.

**Current workflow**:
```python
# Must manually specify primary keys in application code
primary_keys = ['ycsb_key']  # How do we know this?

# Or query CockroachDB separately
cursor.execute("""
    SELECT column_name 
    FROM information_schema.key_column_usage 
    WHERE table_name = %s AND constraint_name LIKE '%_pkey'
""", (table_name,))
```

This is particularly problematic when:
- Processing multiple tables with different primary key schemas
- Building generic CDC pipelines that auto-discover tables
- Using Delta Lake MERGE operations (which require knowing the join keys)
- Maintaining external primary key mappings that can become stale

**Comparison with CockroachDB's own formats**:
- **JSON format**: Primary keys encoded as separate key message (see `encoder_json.go` lines 52-56)
- **Avro format**: Primary keys in separate key schema registered with schema registry (see `encoder_avro.go` lines 27-29, 147)
- **Kafka sink**: Primary keys in Kafka message key field (see `sink_kafka.go` line 401)
- **Parquet format**: ❌ **No primary key information** (inconsistent with other formats)

CockroachDB Parquet is the **only changefeed format** that doesn't include primary key information.

## Describe the solution you'd like

Include primary key metadata in production Parquet files, similar to what already exists in test builds.

**Proposed Parquet metadata**:
```
primary_keys: "ycsb_key"
```

Or for composite keys:
```
primary_keys: "order_id,tenant_id"
```

**Implementation note**: This feature already exists in test builds via the `addParquetTestMetadata()` function in `pkg/ccl/changefeedccl/parquet.go` (lines 238-327). The test metadata currently includes:
- `keyCols`: Primary key column names and positions (e.g., `"ycsb_key,0"`)
- `allCols`: All column names and positions (e.g., `"ycsb_key,0,field0,1,field1,2"`)

Test metadata can be enabled via:
```go
var includeParquestTestMetadata = buildutil.CrdbTestBuild ||
    envutil.EnvOrDefaultBool("COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA", false)
```

**Proposed production metadata** (minimal subset):
```
cockroach.primary_keys: "col1,col2"
```

This would enable downstream consumers to:
1. **Auto-detect primary keys** without querying CockroachDB
2. **Auto-generate MERGE operations** in Spark/Databricks
3. **Create proper indexes** in target systems (e.g., Snowflake, BigQuery)
4. **Validate data integrity** during ingestion
5. **Build self-describing CDC pipelines** that work across multiple tables

## Describe alternatives you've considered

**Alternative 1: Query CockroachDB at setup time**
```python
def get_primary_keys(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.key_column_usage 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]
```

**Drawbacks**:
- Requires CockroachDB connection (may not be available in all environments)
- Adds network round-trip overhead
- Doesn't work for offline/archived Parquet files
- Primary key mapping can become stale if schema changes

**Alternative 2: External configuration file**
```python
# Maintain external primary key mapping
TABLE_PRIMARY_KEYS = {
    'usertable': ['ycsb_key'],
    'orders': ['order_id', 'tenant_id'],
    'products': ['product_id']
}
```

**Drawbacks**:
- Manual maintenance required
- Can become stale
- Must be synchronized across all consumers
- Error-prone for large numbers of tables

**Alternative 3: Post-process Parquet files**
```python
import pyarrow.parquet as pq

def add_primary_key_metadata(file_path, primary_keys):
    table = pq.read_table(file_path)
    metadata = {b'primary_keys': ','.join(primary_keys).encode('utf-8')}
    existing_metadata = table.schema.metadata or {}
    existing_metadata.update(metadata)
    new_schema = table.schema.with_metadata(existing_metadata)
    new_table = table.cast(new_schema)
    pq.write_table(new_table, file_path)
```

**Drawbacks**:
- Requires rewriting all Parquet files
- Doubles storage I/O cost
- Must still determine primary keys from another source
- Increases latency in CDC pipeline

**Alternative 4: Use test metadata in production**

Enable test metadata via environment variable:
```bash
export COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA=true
```

**Drawbacks**:
- Undocumented/unsupported for production use
- Test metadata includes extra information not needed for CDC (all column positions)
- May have different stability guarantees than production features

## Additional context

### CockroachDB Format Inconsistency

CockroachDB's other changefeed formats **already include** primary key information:

| Format | Primary Key Encoding | Code Reference |
|--------|---------------------|----------------|
| **JSON** | ✅ Separate key message | `encoder_json.go:52-56` |
| **Avro** | ✅ Separate key schema | `encoder_avro.go:27-29, 147` |
| **Kafka** | ✅ Kafka message key field | `sink_kafka.go:401` |
| **Parquet** | ❌ **Not in production** (✅ available in test builds) | `parquet.go:238-327` (test only) |

**JSON format** (from `encoder_json.go` lines 52-56):
```go
// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value.
```

**Avro format** (from `encoder_avro.go` lines 27-29):
```go
// confluentAvroEncoder encodes changefeed entries as Avro's binary or textual
// JSON format. Keys are the primary key columns in a record. Values are all
// columns in a record.
```

**Kafka sink** (from `sink_kafka.go` lines 399-401):
```go
msg := &sarama.ProducerMessage{
    Topic:    topic,
    Key:      sarama.ByteEncoder(key),    // ← Primary keys
    Value:    sarama.ByteEncoder(value),  // ← All columns
}
```

**Why this matters**: JSON/Avro/Kafka formats all separate primary keys from values, making them self-describing. Parquet format should follow the same pattern by including primary key metadata.

### Overhead Analysis

**Current test metadata overhead**:
- `keyCols`: ~50-200 bytes (depends on PK column names)
- `allCols`: ~200-1000 bytes (depends on table schema)
- **Total**: ~250-1200 bytes per file

**Proposed production metadata overhead**:
- `cockroach.primary_keys`: ~50-200 bytes
- **Total**: ~50-200 bytes per file (0.00001% of typical file size)

For a 10 MB Parquet file, this adds 0.0002% overhead.

### Code References

**Existing test implementation**:

**File**: `pkg/ccl/changefeedccl/parquet.go` (lines 238-327)

```go
func addParquetTestMetadata(
    row cdcevent.Row, encodingOpts changefeedbase.EncodingOptions, parquetOpts []parquet.Option,
) ([]parquet.Option, error) {
    // ... builds keyCols and allCols maps ...
    
    keyCols := map[string]int{}
    var keysInOrder []string
    if err := row.ForEachKeyColumn().Col(func(col cdcevent.ResultColumn) error {
        keyCols[col.Name] = -1
        keysInOrder = append(keysInOrder, col.Name)
        return nil
    }); err != nil {
        return parquetOpts, err
    }
    
    // ... determine column offsets ...
    
    parquetOpts = append(parquetOpts, 
        parquet.WithMetadata(map[string]string{"keyCols": serializeMap(keysInOrder, keyCols)}))
    parquetOpts = append(parquetOpts, 
        parquet.WithMetadata(map[string]string{"allCols": serializeMap(valuesInOrder, valueCols)}))
    return parquetOpts, nil
}
```

**Metadata serialization** (lines 329-342):

```go
// serializeMap serializes a map to a string. For example, orderedKeys=["b", "a"] 
// m={"a": 1", "b": 2, "c":3} will return the string "b,2,a,1".
func serializeMap(orderedKeys []string, m map[string]int) string {
    var buf bytes.Buffer
    for i, k := range orderedKeys {
        if i > 0 {
            buf.WriteString(",")
        }
        buf.WriteString(k)
        buf.WriteString(",")
        buf.WriteString(strconv.Itoa(m[k]))
    }
    return buf.String()
}
```

**Test metadata usage** (lines 368-382):

```go
// TestingGetEventTypeColIdx returns the index of the extra column added to
// every parquet file which indicate the type of event that generated a
// particular row.
func TestingGetEventTypeColIdx(rd parquet.ReadDatumsMetadata) (int, error) {
    columnsNamesString, ok := rd.MetaFields["allCols"]
    if !ok {
        return -1, errors.Errorf("could not find column names in parquet metadata")
    }
    _, columnNameSet, err := deserializeMap(columnsNamesString)
    if err != nil {
        return -1, err
    }
    return columnNameSet[parquetCrdbEventTypeColName], nil
}
```

### Proposed Implementation

**Minimal production metadata** (simpler than test format):

```go
func addProductionMetadata(
    row cdcevent.Row, parquetOpts []parquet.Option,
) ([]parquet.Option, error) {
    var primaryKeys []string
    if err := row.ForEachKeyColumn().Col(func(col cdcevent.ResultColumn) error {
        primaryKeys = append(primaryKeys, col.Name)
        return nil
    }); err != nil {
        return parquetOpts, err
    }
    
    // Simple comma-separated format
    parquetOpts = append(parquetOpts, 
        parquet.WithMetadata(map[string]string{
            "cockroach.primary_keys": strings.Join(primaryKeys, ",")
        }))
    return parquetOpts, nil
}
```

**Reading the metadata** (downstream consumer):

```python
import pyarrow.parquet as pq

def read_primary_keys(parquet_file):
    """Read primary keys from Parquet metadata."""
    metadata = pq.read_metadata(parquet_file)
    file_metadata = metadata.metadata
    
    if b'cockroach.primary_keys' in file_metadata:
        pk_str = file_metadata[b'cockroach.primary_keys'].decode('utf-8')
        return pk_str.split(',')  # ["ycsb_key"] or ["order_id", "tenant_id"]
    else:
        raise ValueError("No primary key metadata found")

# Auto-generate Spark MERGE
primary_keys = read_primary_keys('file.parquet')
merge_condition = ' AND '.join([f's.{pk} = t.{pk}' for pk in primary_keys])

spark.sql(f"""
    MERGE INTO target t
    USING source s
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Benefits

1. **Self-describing files**: Parquet files contain all information needed for CDC processing
2. **Automatic MERGE generation**: Databricks/Spark can auto-generate MERGE operations
3. **Reduced configuration**: No need for external primary key mappings
4. **Improved reliability**: Primary key information travels with the data
5. **Offline processing**: Can process archived Parquet files without CockroachDB access
6. **Multi-table pipelines**: Generic CDC code works across tables with different schemas
7. **Schema evolution**: Primary key changes are automatically reflected in metadata

### Related Issues

- Issue #161962: `.RESOLVED` files use `DECIMAL(2147483647, 0)` breaking Spark consumers

### Impact

**Affected users**:
- All CockroachDB changefeed users consuming Parquet files
- Databricks users (especially with Auto Loader)
- AWS Glue/EMR users
- Azure Synapse users
- Data engineers building CDC pipelines
- Organizations with multi-table CDC requirements

**User testimonial**:
Our Databricks CDC pipeline processes 50+ CockroachDB tables. Without primary key metadata, we maintain a 500-line configuration file mapping tables to primary keys. Every schema change requires manual updates across multiple environments. Primary key metadata would eliminate this operational burden entirely.
