# CockroachDB Changefeed: Primary Key Handling Across Formats

**Date**: January 28, 2026  
**Analysis**: Comparison of how different changefeed formats handle primary keys

---

## Summary

| Format | Primary Key Encoding | Key Separate from Value? | Schema Registry? | Self-Describing? |
|--------|---------------------|--------------------------|------------------|------------------|
| **JSON** | ✅ Separate key message | ✅ YES | ❌ No | ✅ YES (key structure obvious) |
| **Avro** | ✅ Separate key schema | ✅ YES | ✅ YES (Confluent) | ✅ YES (schema includes PK) |
| **Kafka** | ✅ Kafka message key field | ✅ YES | Depends on format | ✅ YES (key part of message) |
| **Parquet** | ❌ **Not included** | ❌ **NO** | ❌ No | ❌ **NO** (must query or hardcode) |

---

## Detailed Analysis

### 1. JSON Format

**Code**: `pkg/ccl/changefeedccl/encoder_json.go` (lines 52-56)

```go
// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value.
```

**How it works**:
- **Separate key encoding**: `EncodeKey()` returns primary keys as JSON array
- **Key structure** (line 281-296):
  ```go
  func (e *versionEncoder) encodeKeyRawAsArray(
      ctx context.Context, it cdcevent.Iterator,
  ) (json.JSON, error) {
      kb := json.NewArrayBuilder(1)
      if err := it.Col(func(col cdcevent.ResultColumn) error {
          if err := kb.Add(col.Datum); err != nil {
              return err
          }
          return nil
      }); err != nil {
          return nil, err
      }
      return kb.Build(), nil
  }
  ```

**Example output**:
```json
// Key message (primary keys only)
[123, "tenant-abc"]

// Value message (all columns)
{
  "id": 123,
  "tenant_id": "tenant-abc",
  "name": "John Doe",
  "email": "john@example.com",
  "__crdb__": {"updated": "1234567890.0000000000"}
}
```

**Optional `key_in_value`** (line 308-314):
```go
func (e *versionEncoder) encodeKeyInValue(
    ctx context.Context, updated cdcevent.Row, b *json.FixedKeysObjectBuilder, noSchema bool,
) error {
    keyJSON, err := e.encodeKeyRaw(ctx, updated.ForEachKeyColumn(), updated.Metadata, noSchema)
    // ... adds key to value payload
}
```

With `key_in_value=true`:
```json
{
  "key": [123, "tenant-abc"],
  "id": 123,
  "tenant_id": "tenant-abc",
  "name": "John Doe"
}
```

**Consumer impact**:
- ✅ **Kafka consumers**: Primary keys in message key field (standard Kafka pattern)
- ✅ **Cloud storage**: Key and value written to separate files or columns
- ✅ **Self-describing**: Structure makes primary keys obvious

---

### 2. Avro Format

**Code**: `pkg/ccl/changefeedccl/encoder_avro.go` (lines 27-29)

```go
// confluentAvroEncoder encodes changefeed entries as Avro's binary or textual
// JSON format. Keys are the primary key columns in a record. Values are all
// columns in a record.
```

**How it works**:
- **Separate key schema**: `PrimaryIndexToAvroSchema()` creates schema for keys
- **Schema registry**: Keys and values registered separately with `-key` and `-value` suffixes
- **Key encoding** (lines 128-150):
  ```go
  func (e *confluentAvroEncoder) EncodeKey(ctx context.Context, row cdcevent.Row) ([]byte, error) {
      // ...
      if e.customKeyColumn == "" {
          registered.schema, err = avro.PrimaryIndexToAvroSchema(row, tableName, e.schemaPrefix)
          if err != nil {
              return nil, err
          }
      }
      // Register key schema with schema registry
      registered.registryID, err = e.schemaRegistry.RegisterSchemaForSubject(
          ctx, tableName+confluentSubjectSuffixKey, registered.schema.Codec().Schema())
      // ...
  }
  ```

**Schema registry subjects**:
```
usertable-key     -> {"type": "record", "fields": [{"name": "ycsb_key", "type": "int"}]}
usertable-value   -> {"type": "record", "fields": [...all columns...]}
```

**Example Avro key schema**:
```json
{
  "type": "record",
  "name": "usertable",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "tenant_id", "type": "string"}
  ]
}
```

**Consumer impact**:
- ✅ **Schema registry**: Consumers can fetch key schema to identify primary keys
- ✅ **Kafka consumers**: Key schema separate from value schema (Confluent standard)
- ✅ **Type information**: Full type metadata for primary key columns
- ✅ **Self-describing**: Schema explicitly identifies primary keys

---

### 3. Kafka Sink

**Code**: `pkg/ccl/changefeedccl/sink_kafka.go` (lines 399-407)

```go
msg := &sarama.ProducerMessage{
    Topic:    topic,
    Key:      sarama.ByteEncoder(key),    // ← Primary keys
    Value:    sarama.ByteEncoder(value),  // ← All columns
    Headers:  recordHeaders,
}
```

**How it works**:
- **Kafka message structure**: Keys stored in message key field (standard Kafka)
- **Format-agnostic**: Key/value separation at Kafka protocol level
- **Consumer access**: Kafka consumers read `message.key()` and `message.value()` separately

**Consumer impact**:
- ✅ **Standard Kafka pattern**: All Kafka consumers expect key/value separation
- ✅ **Partition key**: Primary keys used for partition assignment
- ✅ **Compaction**: Log compaction uses key for deduplication
- ✅ **Self-describing**: Kafka protocol naturally separates keys

---

### 4. Parquet Format (Cloud Storage)

**Code**: `pkg/ccl/changefeedccl/parquet.go`

```go
// newParquetSchemaDefintion returns a parquet schema definition based on the
// cdcevent.Row and the number of cols in the schema.
func newParquetSchemaDefintion(
    row cdcevent.Row, encodingOpts changefeedbase.EncodingOptions,
) (*parquet.SchemaDefinition, error) {
    // ... iterates over ALL columns (includes primary keys mixed with data)
    if err := row.ForAllColumns().Col(func(col cdcevent.ResultColumn) error {
        columnNames = append(columnNames, col.Name)
        columnTypes = append(columnTypes, col.Typ)
        // ❌ No indication which columns are primary keys
        return nil
    }); err != nil {
        return nil, err
    }
    // ...
}
```

**How it works**:
- **All columns in one file**: Primary keys mixed with data columns
- **No key/value separation**: Unlike JSON/Avro/Kafka
- **No metadata**: Production files don't indicate which columns are PKs

**Test-only metadata** (lines 238-327):
```go
func addParquetTestMetadata(...) {
    // Enabled ONLY if:
    var includeParquestTestMetadata = buildutil.CrdbTestBuild ||
        envutil.EnvOrDefaultBool("COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA", false)
    
    // Adds:
    parquetOpts = append(parquetOpts, 
        parquet.WithMetadata(map[string]string{
            "keyCols": "ycsb_key,0",  // ← Primary keys with positions
            "allCols": "ycsb_key,0,field0,1,..."
        }))
}
```

**Parquet file structure**:
```
Schema:
- ycsb_key: INT           ← Primary key, but no indication
- field0: STRING          ← Data column
- field1: STRING          ← Data column
- __crdb__event_type: STRING
- __crdb__updated: STRING

Metadata (production):
  ❌ NONE
  
Metadata (test builds only):
  ✅ keyCols: "ycsb_key,0"
  ✅ allCols: "ycsb_key,0,field0,1,field1,2,..."
```

**Consumer impact**:
- ❌ **Must query CockroachDB** to identify primary keys
- ❌ **Or hardcode** primary keys in application
- ❌ **Or enable test metadata** (unsupported in production)
- ❌ **Not self-describing**: No way to determine PKs from file alone

---

## Why This Matters

### Kafka/JSON/Avro → Cloud Storage (Standard CDC Pipeline)

When using Kafka Connect or other tools to sink Kafka topics to cloud storage:

1. **Source**: CockroachDB → Kafka (key/value separation)
2. **Kafka topic**: Message key contains primary keys
3. **Kafka Connect**: S3/GCS/Azure sink connector
4. **Result**: Parquet files with primary key information embedded

**Example (Kafka Connect S3 Sink)**:
- Converts Kafka message key → `_key` column in Parquet
- Converts Avro key schema → metadata in Parquet
- **Result**: Self-describing Parquet files with PK information

### CockroachDB Direct → Cloud Storage (Current Approach)

When using CockroachDB changefeed directly to cloud storage:

1. **Source**: CockroachDB → Azure/S3/GCS
2. **No key/value separation**: All columns in one file
3. **No primary key metadata**: Not included in production files
4. **Result**: Non-self-describing Parquet files

**Workaround**:
```python
# Must query CockroachDB separately
primary_keys = get_primary_keys_from_db(table_name)

# Or hardcode
primary_keys = ['ycsb_key']  # Manual maintenance required
```

---

## Industry Standard: CDC Primary Keys

### Debezium (Kafka-based CDC)

Kafka message structure:
```json
// Key (primary keys only)
{
  "schema": {
    "type": "struct",
    "fields": [
      {"field": "id", "type": "int64"}
    ]
  },
  "payload": {"id": 123}
}

// Value (all columns + metadata)
{
  "schema": { "fields": [...] },
  "payload": {
    "before": null,
    "after": {"id": 123, "name": "John"},
    "op": "c"
  }
}
```

**Primary key identification**: In key schema

### Kafka Connect

Avro schema for keys:
```json
{
  "type": "record",
  "name": "Key",
  "fields": [
    {"name": "id", "type": "long"}
  ]
}
```

**Primary key identification**: Separate key schema

### Airbyte

Destination connector receives:
```json
{
  "stream": "users",
  "primary_key": ["id"],
  "data": {"id": 123, "name": "John"}
}
```

**Primary key identification**: In message metadata

### Fivetran

Configuration:
```yaml
tables:
  - name: users
    primary_key: [id]
    columns: [id, name, email]
```

**Primary key identification**: In sync configuration

---

## Recommendation

### For Feature Request

**Argument**: Parquet format should follow the same pattern as JSON/Avro/Kafka

| Format | Key/Value Separation | Primary Key Info | Justification |
|--------|---------------------|------------------|---------------|
| JSON | ✅ Separate messages | ✅ In key message | Standard JSON CDC pattern |
| Avro | ✅ Separate schemas | ✅ In key schema | Confluent/Kafka standard |
| Kafka | ✅ Separate fields | ✅ In message key | Kafka protocol standard |
| Parquet | ❌ Single file | ❌ **Missing** | ⚠️ **Inconsistent** |

**Solution**: Add primary key metadata to Parquet files
```
cockroach.primary_keys: "id,tenant_id"
```

This aligns Parquet format with JSON/Avro/Kafka formats and industry CDC standards.

---

## Code References

### JSON Encoder
- **File**: `pkg/ccl/changefeedccl/encoder_json.go`
- **Key encoding**: Lines 52-56 (comments), 195-208 (implementation)
- **Key as array**: Lines 281-296
- **Key in value**: Lines 308-314

### Avro Encoder
- **File**: `pkg/ccl/changefeedccl/encoder_avro.go`
- **Key encoding**: Lines 27-29 (comments), 128-150 (implementation)
- **Primary key schema**: Line 147 (`PrimaryIndexToAvroSchema`)
- **Schema subjects**: Lines 23-24 (`-key` and `-value` suffixes)

### Avro Schema Generation
- **File**: `pkg/ccl/changefeedccl/avro/avro.go`
- **Primary index to schema**: Lines 868-873

### Kafka Sink
- **File**: `pkg/ccl/changefeedccl/sink_kafka.go`
- **Message structure**: Lines 399-407
- **Key field**: Line 401 (`Key: sarama.ByteEncoder(key)`)

### Parquet Writer
- **File**: `pkg/ccl/changefeedccl/parquet.go`
- **Schema definition**: Lines 48-86 (no PK tracking)
- **Test metadata**: Lines 238-327 (includes PK info)
- **Test metadata flag**: Lines 29-33
