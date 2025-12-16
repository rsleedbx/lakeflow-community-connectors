# **CockroachDB Changefeed API Documentation**

## **Overview**

This documentation covers CockroachDB's Change Data Capture (CDC) functionality using **sinkless changefeeds**. Sinkless changefeeds send results directly to the SQL client session without requiring external sink configuration, making them ideal for real-time data ingestion.

CockroachDB changefeeds allow you to use inline SELECT statements (CDC queries) to define and filter the data that gets emitted, providing powerful filtering and transformation capabilities at the source.

## **Authorization**

### **Connection Method: PostgreSQL Wire Protocol**

CockroachDB is wire-compatible with PostgreSQL, allowing the use of standard PostgreSQL client drivers.

**Required Parameters:**
- `host`: The hostname or IP address of the CockroachDB cluster (e.g., `free-tier.gcp-us-central1.cockroachlabs.cloud`)
- `port`: The port number (default: `26257` for CockroachDB, `5432` for some cloud deployments)
- `database`: The database name
- `user`: The database user with changefeed permissions
- `password`: The user's password
- `sslmode`: SSL mode (typically `require` or `verify-full` for CockroachCloud)

**Example Connection String:**
```
postgresql://user:password@host:port/database?sslmode=require
```

**Authentication Headers/Parameters:**
- Standard PostgreSQL authentication is used
- SSL/TLS certificates may be required for CockroachCloud deployments

**Required Permissions:**
- `CHANGEFEED` privilege on tables to be monitored
- `SELECT` privilege on tables to be queried
- `CONNECT` privilege on the database

**Example Python Connection:**
```python
import psycopg2

conn = psycopg2.connect(
    host="free-tier.gcp-us-central1.cockroachlabs.cloud",
    port=26257,
    database="defaultdb",
    user="myuser",
    password="mypassword",
    sslmode="require"
)
```

## **Object List**

### **Table Discovery**

Tables in CockroachDB can be discovered using standard PostgreSQL system catalogs.

**API Endpoint:** SQL Query via `information_schema.tables`

**Example Query:**
```sql
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'crdb_internal') 
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;
```

**Response Format:**
```json
[
  {"table_schema": "public", "table_name": "users"},
  {"table_schema": "public", "table_name": "orders"},
  {"table_schema": "inventory", "table_name": "products"}
]
```

**Notes:**
- Tables must have a primary key to support changefeeds
- System tables and internal CockroachDB tables are excluded
- Multi-tenant deployments may have schema-scoped tables

## **Object Schema**

### **Schema Retrieval**

Table schemas can be retrieved using standard PostgreSQL `information_schema.columns`.

**Example Query:**
```sql
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public' 
  AND table_name = 'users'
ORDER BY ordinal_position;
```

**Response Fields:**
- `column_name`: Name of the column
- `data_type`: PostgreSQL/CockroachDB data type (e.g., `varchar`, `bigint`, `timestamp with time zone`)
- `character_maximum_length`: Max length for character types
- `numeric_precision`: Precision for numeric types
- `numeric_scale`: Scale for numeric types
- `is_nullable`: Whether the column allows NULL values (`YES` or `NO`)
- `column_default`: Default value expression

**Example Response:**
```json
[
  {
    "column_name": "id",
    "data_type": "bigint",
    "is_nullable": "NO",
    "column_default": "unique_rowid()"
  },
  {
    "column_name": "username",
    "data_type": "character varying",
    "character_maximum_length": 255,
    "is_nullable": "NO"
  },
  {
    "column_name": "created_at",
    "data_type": "timestamp with time zone",
    "is_nullable": "YES",
    "column_default": "now()"
  }
]
```

## **Get Object Primary Keys**

### **Primary Key Discovery**

Primary keys can be retrieved using `information_schema.key_column_usage`.

**Example Query:**
```sql
SELECT 
    kcu.column_name,
    kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
  AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = 'public'
  AND tc.table_name = 'users'
ORDER BY kcu.ordinal_position;
```

**Response Format:**
```json
[
  {"column_name": "id", "ordinal_position": 1}
]
```

**Notes:**
- CockroachDB requires all tables to have a primary key for changefeeds
- Composite primary keys are supported (multiple columns)
- If no explicit primary key is defined, CockroachDB creates a hidden `rowid` column

## **Object's Ingestion Type**

### **CDC (Change Data Capture)**

All tables in CockroachDB support CDC through changefeeds, making the ingestion type `cdc` for all objects.

**Ingestion Type:** `cdc`

**Characteristics:**
- Supports INSERT, UPDATE, and DELETE operations
- Provides both before and after values for updates
- Tracks deletions as separate events
- Real-time streaming of changes

**Change Event Structure:**
- `key`: Primary key values
- `value`: JSON object containing the row data (after the change)
- `updated`: Timestamp of the change (CockroachDB MVCC timestamp)
- `after`: The row state after the change
- `before` (optional): The row state before the change (if `diff` option enabled)

## **Read API for Data Retrieval**

### **Sinkless Changefeed (Recommended for Connector)**

Sinkless changefeeds are the primary method for real-time CDC in this connector.

**API Method:** SQL Statement (`EXPERIMENTAL CHANGEFEED FOR`)

**Basic Syntax:**
```sql
EXPERIMENTAL CHANGEFEED FOR table_name;
```

**With CDC Query (Filtered Changefeed):**
```sql
EXPERIMENTAL CHANGEFEED FOR 
  SELECT id, username, email, updated_at 
  FROM users 
  WHERE status = 'active';
```

**Available Options:**
- `cursor`: Start position (timestamp in CockroachDB MVCC format, e.g., `'1234567890.0000000000'`)
- `updated`: Include update timestamps
- `resolved`: Emit resolved timestamps for tracking progress
- `diff`: Include both before and after values for updates
- `envelope`: Change data format (default: `wrapped`)

**Example with Options:**
```sql
EXPERIMENTAL CHANGEFEED FOR users 
WITH updated, resolved='10s', diff, cursor='1234567890.0000000000';
```

**Native Result Structure:**

CockroachDB changefeeds return **4 columns** natively:

```python
# Reading changefeed with psycopg2
cursor.execute("EXPERIMENTAL CHANGEFEED FOR users WITH updated, resolved='10s'")

for row in cursor:
    key = row[0]       # JSON array: primary key values, e.g. ["123"]
    value = row[1]     # JSON object: row data with "after" (and "before" if diff enabled)
    updated = row[2]   # String: MVCC timestamp, e.g. "1734346800.0000000000"
    topic = row[3]     # String: table name, e.g. "users"
    
    # For resolved timestamp rows: key=None, value=None, updated=timestamp
    if key is None and value is None:
        # This is a watermark/checkpoint
        continue
```

**Connector Transformation:**

The connector reads these native columns and transforms them into Lakeflow CDC format:

```python
# What the connector does:
result = {
    # Connector-added CDC metadata fields (not in source database):
    "_cdc_key": key,              # Mapped from changefeed 'key' column
    "_cdc_updated": updated,      # Mapped from changefeed 'updated' column
    "_cdc_operation": "INSERT",   # Derived from changefeed 'value' column
    
    # Original table columns (from changefeed 'value.after'):
    "id": 123,
    "username": "alice",
    "email": "alice@example.com",
    # ... other columns
}
```

This transformation happens in the connector code - **the source database is never modified**.

**Change Event Format (JSON Content of key/value columns):**

**INSERT Event:**
```json
{
  "key": ["1"],
  "value": {
    "after": {
      "id": 1,
      "username": "alice",
      "email": "alice@example.com",
      "created_at": "2025-12-16T10:00:00Z",
      "updated_at": "2025-12-16T10:00:00Z"
    }
  },
  "updated": "1734346800.0000000000"
}
```

**UPDATE Event (with diff):**
```json
{
  "key": ["1"],
  "value": {
    "before": {
      "id": 1,
      "username": "alice",
      "email": "alice@example.com",
      "updated_at": "2025-12-16T10:00:00Z"
    },
    "after": {
      "id": 1,
      "username": "alice",
      "email": "alice_new@example.com",
      "updated_at": "2025-12-16T11:00:00Z"
    }
  },
  "updated": "1734350400.0000000000"
}
```

**DELETE Event:**
```json
{
  "key": ["1"],
  "value": null,
  "updated": "1734354000.0000000000"
}
```

**Resolved Timestamp (Progress Marker):**
```json
{
  "resolved": "1734354000.0000000000"
}
```

### **Cursor Management**

The `cursor` option allows resuming from a specific point in time:
- Format: CockroachDB MVCC timestamp (e.g., `'1734346800.0000000000'`)
- Represents nanosecond-precision timestamp
- Can be obtained from `resolved` events or `updated` timestamps

**Incremental Read Strategy:**
1. Start changefeed with optional cursor (for resuming)
2. Process change events as they arrive
3. Track the latest `resolved` timestamp
4. On restart, use the last `resolved` timestamp as the cursor

### **Handling Deletes**

Deletes are automatically included in changefeed output:
- Event has `"value": null`
- The `key` field contains the primary key of the deleted row
- The `updated` timestamp indicates when the deletion occurred

**No separate API call needed** - deletes are part of the changefeed stream.

### **Pagination**

Not applicable - changefeeds are streaming APIs that emit events as they occur.

**Backfilling (Initial Snapshot):**
- By default, changefeeds start with a snapshot of existing data
- To skip initial snapshot: use `cursor` with current timestamp
- Large tables may take time for initial snapshot

### **Table-Specific Options**

**Required Parameters:**
- `table_name`: Name of the table to monitor (required)

**Optional Parameters:**
- `cursor`: Resume position (optional, for incremental reads)
- `include_diff`: Whether to include before/after values (boolean, default: false)
- `select_query`: Custom SELECT statement for filtered changefeed (optional)
- `resolved_interval`: Interval for resolved timestamps (e.g., `'10s'`, default: `'10s'`)

**Example API Request (Conceptual):**
```python
# Start changefeed for a table
changefeed = conn.cursor()
query = """
    EXPERIMENTAL CHANGEFEED FOR users
    WITH updated, resolved='10s', diff, cursor='1734346800.0000000000';
"""
changefeed.execute(query)

# Fetch changes
for row in changefeed:
    event = json.loads(row[0])  # Parse JSON event
    # Process event
```

## **Field Type Mapping**

### **CockroachDB to Python/Spark Types**

| CockroachDB Type | PostgreSQL Equivalent | Python Type | Spark Type | Notes |
|---|---|---|---|---|
| `INT`, `INT2`, `INT4`, `INT8`, `BIGINT` | `bigint` | `int` | `LongType` | 64-bit integer |
| `SERIAL`, `BIGSERIAL` | `bigint` | `int` | `LongType` | Auto-incrementing |
| `FLOAT`, `FLOAT4`, `FLOAT8`, `DOUBLE PRECISION`, `REAL` | `double precision` | `float` | `DoubleType` | 64-bit float |
| `DECIMAL`, `NUMERIC` | `numeric` | `Decimal` | `DecimalType` | Arbitrary precision |
| `BOOL`, `BOOLEAN` | `boolean` | `bool` | `BooleanType` | True/False |
| `STRING`, `VARCHAR`, `CHAR`, `TEXT` | `character varying` | `str` | `StringType` | Variable length text |
| `BYTES`, `BYTEA` | `bytea` | `bytes` | `BinaryType` | Binary data |
| `DATE` | `date` | `datetime.date` | `DateType` | Calendar date |
| `TIME`, `TIME WITHOUT TIME ZONE` | `time` | `datetime.time` | `StringType` | Time of day |
| `TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE` | `timestamp` | `datetime.datetime` | `TimestampType` | No timezone |
| `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `timestamp with time zone` | `datetime.datetime` | `TimestampType` | With timezone (recommended) |
| `INTERVAL` | `interval` | `str` | `StringType` | Duration/interval |
| `UUID` | `uuid` | `str` | `StringType` | UUID string |
| `INET` | `inet` | `str` | `StringType` | IP address |
| `JSON`, `JSONB` | `jsonb` | `dict` | `StringType` | JSON data (store as string, parse in Spark) |
| `ARRAY` | `array` | `list` | `ArrayType` | Array of values |

**Special Handling:**
- **Timestamps:** Always use `TIMESTAMPTZ` (with timezone) for better portability
- **JSON:** CockroachDB stores JSON as `JSONB` (binary format); extract as string in Spark
- **Arrays:** Map to Spark `ArrayType` with appropriate element type
- **NULL values:** Mapped to `None` in Python, `null` in Spark

## **Write API**

This connector is **read-only** and does not support writing data back to CockroachDB. The changefeed API is designed for CDC (Change Data Capture) to ingest changes from CockroachDB into downstream systems.

**Validation:**
Changes can be validated by querying the table directly:
```sql
SELECT * FROM users WHERE id = 1;
```

## **Known Quirks & Edge Cases**

1. **Experimental Feature**: `EXPERIMENTAL CHANGEFEED FOR` is marked as experimental in some CockroachDB versions. It's stable enough for production but the syntax may evolve.

2. **Connection Limits**: Each changefeed holds an open connection. Monitor connection pools to avoid exhaustion.

3. **Large Tables**: Initial snapshot for very large tables can be slow. Consider using cursor to skip initial data if only new changes are needed.

4. **Resolved Timestamps**: Resolved timestamps are essential for tracking progress. Without them, it's difficult to know when all changes up to a point have been processed.

5. **Network Interruptions**: If the connection drops, restart the changefeed with the last `cursor` position to resume.

6. **Schema Changes**: Changefeeds may not automatically handle schema changes (ADD COLUMN, DROP COLUMN). Monitor for errors and restart changefeeds after schema migrations.

7. **Multi-Tenant**: CockroachCloud clusters may have multiple databases/schemas. Ensure the connector has access to the correct database.

8. **Clock Skew**: CockroachDB uses MVCC timestamps based on node clocks. Ensure NTP is properly configured on CockroachDB nodes.

## **Sources and References**

### **Research Log**

| Source Type | URL | Confidence | What it confirmed |
|---|---|---|---|
| Official Docs | https://www.cockroachlabs.com/docs/stable/changefeed-for | High | Changefeed syntax, options, event format |
| Official Docs | https://www.cockroachlabs.com/docs/stable/create-changefeed | High | CREATE CHANGEFEED statement, sinkless vs external sinks |
| Official Docs | https://www.cockroachlabs.com/docs/stable/connection-parameters | High | Connection string format, SSL options |
| Official Docs | https://www.cockroachlabs.com/docs/stable/data-types | High | Data type mapping |
| PostgreSQL Docs | https://www.postgresql.org/docs/current/information-schema.html | High | Schema discovery via information_schema |
| Community | CockroachDB Community Forums | Medium | Best practices for sinkless changefeeds |

### **Notes**

- **Preferred Authentication**: Standard PostgreSQL authentication with SSL/TLS for CockroachCloud
- **Rationale for Sinkless Changefeeds**: Sinkless changefeeds are simpler to implement and don't require external infrastructure (Kafka, cloud storage, etc.)
- **CDC Query Support**: Inline SELECT statements are supported in newer CockroachDB versions for filtered changefeeds

## **TBD Items**

- **Exact CockroachDB version requirements** for CDC query support (SELECT in changefeed) - depends on user's cluster version
- **SSL certificate handling** for CockroachCloud - may require additional configuration for custom CA certificates

