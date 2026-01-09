# CockroachDB Connector Timeout Options

## Overview

The CockroachDB connector has multiple layers of timeout control to prevent hanging queries and ensure reliable operation.

## 1. **CockroachDB Statement Timeout (Recommended)**

**Location:** Set via SQL `SET statement_timeout`  
**Scope:** Per-session, applies to all queries in that session  
**When it works:** CockroachDB itself cancels the query after the timeout

### Configuration:

```python
default_table_config = {
    "query_timeout": "60s",  # Default: 60 seconds
}
```

### Syntax:
- `"30s"` - 30 seconds
- `"5m"` - 5 minutes
- `"1h"` - 1 hour
- `"0"` - Disable timeout (not recommended)

### How it works:

```python
# Connector automatically executes before changefeed query:
cursor.execute("SET statement_timeout = '60s'")
cursor.execute("EXPERIMENTAL CHANGEFEED FOR ...")  # Will timeout after 60s
```

### Advantages:
- ✅ Server-side timeout (most reliable)
- ✅ CockroachDB cancels the query cleanly
- ✅ Frees server resources
- ✅ Works with any client library

### Disadvantages:
- ❌ Requires one extra query per connection

---

## 2. **Connection Timeout**

**Location:** `pg8000.connect(timeout=...)`  
**Scope:** Only for establishing the connection  
**When it works:** When connecting to CockroachDB

### Configuration:

```python
conn = pg8000.connect(
    host=self.host,
    port=self.port,
    user=self.user,
    password=self.password,
    database=self.database,
    timeout=30,  # Connection timeout in seconds
)
```

### Default:
- pg8000: 60 seconds
- psycopg2: No timeout (waits forever)

### When to use:
- Network issues
- Firewall blocking
- CockroachDB cluster unreachable

---

## 3. **Socket Timeout (pg8000)**

**Location:** Socket-level timeout  
**Scope:** All socket operations (read/write)  
**When it works:** During data transfer

### Configuration:

```python
import socket

# Set socket timeout after connection
conn = pg8000.connect(...)
sock = conn._usock  # Access underlying socket
sock.settimeout(60)  # 60 seconds timeout for all operations
```

### Advantages:
- ✅ Catches network hangs
- ✅ Works for slow data transfer

### Disadvantages:
- ❌ May timeout legitimate slow queries
- ❌ Harder to configure precisely

---

## 4. **Python Signal-based Timeout (Unix only)**

**Location:** Python signal module  
**Scope:** Any Python code block  
**When it works:** Only on Unix/Linux (not Windows)

### Configuration:

```python
import signal

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError("Query timeout")

# Set timeout before query
signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(60)  # 60 seconds

try:
    cursor.execute(changefeed_query)
except TimeoutError:
    print("Query timed out!")
finally:
    signal.alarm(0)  # Disable alarm
```

### Advantages:
- ✅ Works with any library
- ✅ Can timeout any code block

### Disadvantages:
- ❌ Unix/Linux only (not Windows)
- ❌ Not available in Databricks (signal.alarm disabled)
- ❌ Can't be used in multi-threaded code

---

## 5. **Threading Timeout**

**Location:** Python threading module  
**Scope:** Any Python code block  
**When it works:** Cross-platform

### Configuration:

```python
import threading

class QueryThread(threading.Thread):
    def __init__(self, cursor, query):
        super().__init__()
        self.cursor = cursor
        self.query = query
        self.result = None
        self.error = None
    
    def run(self):
        try:
            self.cursor.execute(self.query)
            self.result = self.cursor.fetchall()
        except Exception as e:
            self.error = e

# Execute with timeout
thread = QueryThread(cursor, changefeed_query)
thread.start()
thread.join(timeout=60)  # Wait max 60 seconds

if thread.is_alive():
    # Thread still running = timeout
    print("Query timed out!")
    # Note: Can't forcefully kill thread, query still runs on server
else:
    # Thread finished
    if thread.error:
        raise thread.error
    result = thread.result
```

### Advantages:
- ✅ Cross-platform
- ✅ Works with any library

### Disadvantages:
- ❌ Can't kill the thread (query still runs on server)
- ❌ Resource leak if many timeouts
- ❌ Complex to implement

---

## Recommended Configuration

### For Testing (Fast Feedback):
```python
default_table_config = {
    "initial_scan": "only",     # Snapshot mode (no streaming)
    "batch_size": "100",        # Small batch for testing
    "query_timeout": "30s",     # Short timeout
}

# Connection timeout
timeout=10  # 10 seconds to connect
```

### For Production (Streaming CDC):
```python
default_table_config = {
    "initial_scan": "yes",      # Full scan + streaming
    "batch_size": "10000",      # Larger batches
    "query_timeout": "0",       # Disable timeout (streaming query)
    "resolved_interval": "10s", # Heartbeat every 10s
}

# Connection timeout
timeout=30  # 30 seconds to connect
```

### For Large Initial Scans:
```python
default_table_config = {
    "initial_scan": "only",     # Snapshot mode
    "batch_size": "50000",      # Large batches
    "query_timeout": "10m",     # 10 minutes for large tables
}
```

---

## Debugging Hangs

If queries are hanging, check in order:

1. **Is it a connection issue?**
   - Lower connection timeout: `timeout=5`
   - Check: Can you connect with psql?

2. **Is it a query timeout?**
   - Add statement_timeout: `"query_timeout": "60s"`
   - Check: Run query manually in CockroachDB SQL shell

3. **Is it a streaming issue?**
   - Change to snapshot mode: `"initial_scan": "only"`
   - Check: Does it work without streaming?

4. **Is it a large result set?**
   - Reduce batch size: `"batch_size": "100"`
   - Check: Monitor CockroachDB query stats

---

## Current Implementation

The connector currently uses:

1. ✅ **CockroachDB statement_timeout** - Set automatically based on `query_timeout` option
2. ✅ **Connection timeout** - `pg8000.connect(timeout=30)`
3. ❌ Socket timeout - Not implemented (could be added if needed)
4. ❌ Signal timeout - Not available in Databricks
5. ❌ Threading timeout - Too complex, better to use statement_timeout

This provides reliable timeout control at both the connection and query levels.








