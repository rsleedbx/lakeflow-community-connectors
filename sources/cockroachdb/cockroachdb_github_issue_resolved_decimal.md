## Describe the problem

When using `format=parquet` with `resolved` option in changefeeds, CockroachDB generates `.RESOLVED` files with a `resolved` column encoded as `DECIMAL(2147483647, 0)`. This exceeds Apache Spark's maximum DECIMAL precision limit of 38, causing all Spark-based CDC consumers to fail during schema inference.

**Error**:
```
[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision 2147483647 exceeds max precision 38
```

**Inconsistency**: Data files (`*usertable*.parquet`) use `STRING` type for the `__crdb__updated` column, while `.RESOLVED` files use `DECIMAL(2147483647, 0)` for the same timestamp data type.

## To Reproduce

1. Create a CockroachDB changefeed with Parquet format and resolved timestamps:
```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://container/path/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH 
    format='parquet',
    resolved='10s',
    updated,
    initial_scan='yes';
```

2. Wait for CDC events and `.RESOLVED` files to be written to Azure Blob Storage

3. Attempt to read the directory with Apache Spark (Databricks Auto Loader, AWS Glue, EMR, or Synapse):
```python
# Databricks Auto Loader example
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load("abfss://container@account.dfs.core.windows.net/path/")
)
```

4. Observe error during schema inference when Spark reads `.RESOLVED` files

## Expected behavior

`.RESOLVED` files should use the same `STRING` type as data files for timestamp columns, ensuring compatibility with Apache Spark and consistent encoding across all changefeed Parquet files.

Expected schema for `.RESOLVED` files:
```
resolved: STRING
```

Actual schema:
```
resolved: DECIMAL(2147483647, 0)
```

## Additional data / screenshots

### Code Evidence

### 1. .RESOLVED Files Use DECIMAL

**File**: `pkg/ccl/changefeedccl/parquet_sink_cloudstorage.go` (lines 145-157)

```go
sch, err := parquet.NewSchema([]string{metaSentinel + "resolved"}, []*types.T{types.Decimal})
if err != nil {
    return err
}

writer, err := parquet.NewWriter(sch, &buf)
if err != nil {
    return err
}

if err := writer.AddRow([]tree.Datum{eval.TimestampToDecimalDatum(resolved)}); err != nil {
    return err
}
```

### 2. Precision Set to math.MaxInt32

**File**: `pkg/util/parquet/schema.go` (lines 169-186)

```go
case types.DecimalFamily:
    // According to PostgresSQL docs, scale or precision of 0 implies max
    // precision and scale. This code assumes that CRDB matches this behavior.
    // https://www.postgresql.org/docs/10/datatype-numeric.html
    precision := typ.Precision()
    scale := typ.Scale()
    if typ.Precision() == 0 {
        precision = math.MaxInt32  // 2147483647 = 2^31 - 1
    }
    if typ.Scale() == 0 {
        scale = precision
    }

    result.node, err = schema.NewPrimitiveNodeLogical(colName,
        repetitions, schema.NewDecimalLogicalType(precision,
            scale), parquet.Types.ByteArray, defaultTypeLength,
            defaultSchemaFieldID)
```

**Result**: `.RESOLVED` files contain `DECIMAL(2147483647, 0)` in Parquet schema metadata.

### 3. Data Files Use STRING (Inconsistent)

**File**: `pkg/ccl/changefeedccl/parquet.go` (lines 95-98, 193-194)

```go
func appendMetadataColsToSchema(...) {
    if encodingOpts.UpdatedTimestamps {
        columnNames = append(columnNames, parquetOptUpdatedTimestampColName)
        columnTypes = append(columnTypes, types.String)  // ← STRING, not DECIMAL
    }
    ...
}

func (w *parquetWriter) populateDatums(...) {
    ...
    if w.encodingOpts.UpdatedTimestamps {
        datums = append(datums, tree.NewDString(updated.AsOfSystemTime()))  // ← STRING format
    }
    ...
}
```

### Inconsistency Summary

| File Type | Column | Type | Spark Compatible? |
|-----------|--------|------|-------------------|
| Data files (`*usertable*.parquet`) | `__crdb__updated` | `STRING` | ✅ YES |
| `.RESOLVED` files | `resolved` | `DECIMAL(2147483647, 0)` | ❌ NO |

Both represent the same data type (`hlc.Timestamp`) but use different Parquet encodings.

### When Introduced

**Commit**: `07101ad7c1c185f5a2e99d2000a70a303d614ef7`  
**Date**: June 2, 2023  
**Issue**: #103129  
**Title**: "changefeedccl: support the resolved option with format=parquet"

### Current Workaround

Exclude `.RESOLVED` files using path filtering:
```python
.option("pathGlobFilter", "*usertable*.parquet")
```

### Suggested Fix

Change `.RESOLVED` files to use STRING format, matching data files:

**File**: `pkg/ccl/changefeedccl/parquet_sink_cloudstorage.go` (line 145)

```go
// Current (breaks Spark):
sch, err := parquet.NewSchema([]string{metaSentinel + "resolved"}, []*types.T{types.Decimal})

// Proposed (matches data files):
sch, err := parquet.NewSchema([]string{metaSentinel + "resolved"}, []*types.T{types.String})
```

**File**: `pkg/ccl/changefeedccl/parquet_sink_cloudstorage.go` (line 157)

```go
// Current:
if err := writer.AddRow([]tree.Datum{eval.TimestampToDecimalDatum(resolved)}); err != nil {

// Proposed:
if err := writer.AddRow([]tree.Datum{tree.NewDString(resolved.AsOfSystemTime())}); err != nil {
```

This matches the format already used in data files (`__crdb__updated` column) and resolves Spark compatibility.

## Environment

- **CockroachDB version**: v23.1+ (all versions after commit `07101ad7c1c185f5a2e99d2000a70a303d614ef7`)
- **Server OS**: All (Azure Blob Storage sink)
- **Client app**: Apache Spark 3.x, Databricks Runtime (all versions), AWS Glue, AWS EMR, Azure Synapse
- **Changefeed format**: `format=parquet, resolved`
- **File type affected**: `.RESOLVED` files only (data files work correctly)

## Additional context

**Impact**: All Apache Spark-based CDC consumers fail when:
1. Using Databricks Auto Loader, AWS Glue, EMR, or Synapse with CockroachDB changefeeds
2. Reading directories containing `.RESOLVED` files
3. Schema inference enabled (default behavior)

**Root cause**: 
- `.RESOLVED` files use `types.Decimal` which triggers `math.MaxInt32` (2147483647) precision in `pkg/util/parquet/schema.go` line 176
- Data files use `types.String` via `AsOfSystemTime()` method for the same timestamp type
- No consistency check between the two code paths

**References**:
- PostgreSQL DECIMAL behavior (referenced in source code): https://www.postgresql.org/docs/10/datatype-numeric.html
- Parquet logical types spec: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
- Apache Spark DECIMAL limit: 38 (DecimalType.scala)
