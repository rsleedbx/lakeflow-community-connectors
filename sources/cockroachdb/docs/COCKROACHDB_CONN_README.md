# cockroachdb_conn.py

CockroachDB connection management utilities using pg8000.

---

## Functions

### `get_cockroachdb_connection()`

Creates a pg8000 connection to CockroachDB with SSL support.

**Signature**:
```python
def get_cockroachdb_connection(
    cockroachdb_host: str,
    cockroachdb_port: int,
    cockroachdb_user: str,
    cockroachdb_password: str,
    cockroachdb_database: str
)
```

**Parameters**:
- `cockroachdb_host` (str): CockroachDB host (without port). Example: `"myhost.cockroachlabs.cloud"`
- `cockroachdb_port` (int): CockroachDB port. Default: `26257`
- `cockroachdb_user` (str): Database user. Example: `"myuser"`
- `cockroachdb_password` (str): Database password
- `cockroachdb_database` (str): Database name. Example: `"defaultdb"`

**Returns**:
- `pg8000.Connection`: Active database connection

**Features**:
- ✅ Automatically configures SSL context for CockroachDB Cloud
- ✅ Disables hostname verification (common for CockroachDB Cloud)
- ✅ Handles host string with accidental port (strips port if present)

---

## Usage Examples

### Basic Connection

```python
from cockroachdb_conn import get_cockroachdb_connection

conn = get_cockroachdb_connection(
    cockroachdb_host="myhost.cockroachlabs.cloud",
    cockroachdb_port=26257,
    cockroachdb_user="myuser",
    cockroachdb_password="mypassword",
    cockroachdb_database="defaultdb"
)

# Execute query
with conn.cursor() as cur:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(version)

# Close connection
conn.close()
```

### In Databricks Notebook

```python
# Cell 1: Configuration
cockroachdb_host = "myhost.cockroachlabs.cloud"
cockroachdb_port = 26257
cockroachdb_user = "myuser"
cockroachdb_password = dbutils.secrets.get("cockroachdb", "password")
cockroachdb_database = "defaultdb"

# Cell 2: Import and connect
from cockroachdb_conn import get_cockroachdb_connection

conn = get_cockroachdb_connection(
    cockroachdb_host=cockroachdb_host,
    cockroachdb_port=cockroachdb_port,
    cockroachdb_user=cockroachdb_user,
    cockroachdb_password=cockroachdb_password,
    cockroachdb_database=cockroachdb_database
)

print("✅ Connected to CockroachDB")
```

### With Context Manager (Recommended)

```python
from cockroachdb_conn import get_cockroachdb_connection

conn = get_cockroachdb_connection(
    cockroachdb_host="myhost.cockroachlabs.cloud",
    cockroachdb_port=26257,
    cockroachdb_user="myuser",
    cockroachdb_password="mypassword",
    cockroachdb_database="defaultdb"
)

try:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mytable")
        count = cur.fetchone()[0]
        print(f"Row count: {count}")
finally:
    conn.close()
```

---

## SSL Configuration

The function automatically configures SSL for CockroachDB Cloud:

```python
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False  # CockroachDB Cloud compatibility
ssl_context.verify_mode = ssl.CERT_NONE
```

For self-hosted CockroachDB with custom SSL certificates, you may need to modify the SSL context.

---

## Error Handling

```python
from cockroachdb_conn import get_cockroachdb_connection

try:
    conn = get_cockroachdb_connection(
        cockroachdb_host="myhost.cockroachlabs.cloud",
        cockroachdb_port=26257,
        cockroachdb_user="myuser",
        cockroachdb_password="wrongpassword",
        cockroachdb_database="defaultdb"
    )
    print("✅ Connected")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    # Example error: authentication failed
```

---

## Dependencies

```python
import pg8000  # Pure Python PostgreSQL driver
import ssl     # SSL context for secure connections
```

Install dependencies:
```bash
pip install pg8000
```

Or in Databricks:
```python
%pip install pg8000 --quiet
```

---

## Related Modules

- `cockroachdb_azure.py` - Azure Blob Storage utilities for CDC
- `cockroachdb_ycsb.py` - YCSB test data generation
- `cockroachdb_debug.py` - CDC diagnosis utilities

---

## Notes

### Why pg8000?

- ✅ Pure Python (no compiled dependencies)
- ✅ Works in Databricks notebooks
- ✅ Compatible with CockroachDB's PostgreSQL wire protocol
- ✅ No need for psycopg2 (which requires system libraries)

### Host Parsing

The function handles both formats:
- `"myhost.cockroachlabs.cloud"` (correct)
- `"myhost.cockroachlabs.cloud:26257"` (strips port)

This prevents connection errors from accidentally including port in the host string.

---

Date: 2026-01-30
