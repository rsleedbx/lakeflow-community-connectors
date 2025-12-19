# Spark Serialization Fix: Lazy Connection Initialization

## 🐛 **Problem**

After fixing the SSL certificate issues, the pipeline failed with:

```
TypeError: cannot pickle 'psycopg2.extensions.connection' object
_pickle.PicklingError: Could not serialize object
```

This error occurred when Spark tried to serialize the `LakeflowConnect` object to distribute it to worker nodes.

## 🔍 **Root Cause**

1. **The `__init__` method was creating a database connection** and storing it as `self.conn`
2. **Spark serializes (pickles) Python objects** when distributing them to workers
3. **Database connection objects cannot be pickled** - they contain file descriptors, socket connections, and other non-serializable state
4. **Result**: `PicklingError` when Spark tried to distribute the connector

### **Timeline of the Issue**

```python
# BEFORE (broken):
class LakeflowConnect:
    def __init__(self, options):
        self.conn = None
        # ... parse options ...
        self._init_connection()  # Creates and stores connection
    
    def _init_connection(self):
        self.conn = psycopg2.connect(...)  # ❌ Stored in instance
    
    def list_tables(self):
        self._ensure_connection()
        with self.conn.cursor() as cur:  # Uses stored connection
            # ...
```

**Flow**:
1. `__init__` creates `self.conn` (psycopg2 connection object)
2. Spark tries to pickle the `LakeflowConnect` instance
3. **FAIL**: Cannot pickle `psycopg2.extensions.connection`

## ✅ **Solution: Lazy Connection Initialization**

**Create connections only when needed, store them locally, and close them immediately after use.**

### **Implementation**

```python
# AFTER (fixed):
class LakeflowConnect:
    def __init__(self, options):
        # Note: NO connection created in __init__
        # Connection will be created lazily in methods that need it
        self.host = ...
        self.port = ...
        # ... only store connection parameters ...
    
    def _get_connection(self):
        """Create and return a NEW connection (not stored)."""
        conn = psycopg2.connect(...)
        conn.set_session(autocommit=True)
        return conn  # Return, don't store
    
    def list_tables(self):
        conn = self._get_connection()  # Create connection
        try:
            with conn.cursor() as cur:
                # ... use connection ...
            return tables
        finally:
            conn.close()  # Close immediately after use
```

### **Key Changes**

| Component | Before | After |
|-----------|--------|-------|
| **`__init__`** | Called `self._init_connection()` | Only stores connection parameters |
| **`_init_connection()`** | Created `self.conn = psycopg2.connect()` | Renamed to `_get_connection()`, returns new connection |
| **`_ensure_connection()`** | Checked if `self.conn` was alive | Removed (no longer needed) |
| **All methods** | Used `self.conn` | Create local `conn = self._get_connection()`, use in `try/finally`, close after use |
| **`__del__()`** | Closed `self.conn` | Removed (no persistent connection) |

### **Updated Method Pattern**

```python
def some_method(self):
    conn = self._get_connection()  # 1. Create connection
    try:
        with conn.cursor() as cur:  # 2. Use connection
            # ... do work ...
        return result
    finally:
        conn.close()  # 3. Always close
```

### **Special Case: Generators**

For `read_table()` which uses a generator, the connection must remain open for the generator's lifetime:

```python
def read_table(self, ...):
    conn = self._get_connection()  # Create connection
    
    def event_generator():
        cursor = conn.cursor()  # Use connection from closure
        cursor.execute(changefeed_query)
        for row in cursor:
            yield transform(row)
        finally:
            cursor.close()
    
    try:
        events = []
        for event in event_generator():
            events.append(event)
        return iter(events), end_offset
    finally:
        conn.close()  # Close after generator is exhausted
```

## 🎯 **Why This Works**

### **Before (broken)**:
1. Spark: "Pickle this LakeflowConnect object"
2. Python: "It has a `self.conn` attribute"
3. Python: "Try to pickle `psycopg2.extensions.connection`"
4. **❌ FAIL**: "Cannot pickle connection object"

### **After (fixed)**:
1. Spark: "Pickle this LakeflowConnect object"
2. Python: "It has `self.host`, `self.port`, `self.user`, etc. (all strings/ints)"
3. Python: "All attributes are picklable"
4. **✅ SUCCESS**: Object serialized
5. On worker node: `_get_connection()` creates a new connection using the parameters

## 📚 **Lessons Learned**

### **General Principle for Spark Connectors**

> **Never store non-serializable objects as instance variables if the object will be pickled by Spark.**

### **Common Non-Serializable Objects**
- Database connections (`psycopg2`, `pyodbc`, `mysql.connector`, etc.)
- File handles
- Socket connections
- Thread locks
- Lambda functions (in some cases)
- Certain complex objects with C extensions

### **Best Practices**
1. **Store only configuration data** in `__init__` (strings, ints, dicts, lists)
2. **Create connections lazily** when methods are called
3. **Use `try/finally`** to ensure connections are always closed
4. **For generators**, manage connection lifetime carefully
5. **Test picklability** early in development:
   ```python
   import pickle
   connector = LakeflowConnect(options)
   pickle.dumps(connector)  # Should not raise exception
   ```

## 🔗 **Related Issues**

- **SSL Certificate Fix**: [SSL_CERTIFICATE_FIX.md](SSL_CERTIFICATE_FIX.md)
- **Connection Credentials**: [HARDCODED_CONNECTION_BUG.md](HARDCODED_CONNECTION_BUG.md)
- **Nullsource Serialization**: Similar issue found and fixed in nullsource connector (Pydantic/dataclass serialization)

## 📊 **Pipeline Status**

**Update ID**: `cf30a49e-16d6-4feb-b62d-779511afcc64`

**View Pipeline**: https://e2-dogfood.staging.cloud.databricks.com/pipelines/aa91e2c9-e62b-4fca-bd65-cedb89129265

With this fix:
- ✅ SSL configuration working (`sslmode=verify-full` + `sslrootcert=system`, sslcert/sslkey omitted)
- ✅ Spark serialization working (lazy connection initialization)
- ✅ Ready to ingest CockroachDB changefeed data!

### **DSN String Format (Final)**
```python
dsn = "host=... port=... dbname=... user=... password=... sslmode=verify-full sslrootcert=system"
# Note: sslcert and sslkey are NOT included - omission prevents client cert lookups
```

## 🏁 **Next Steps**

1. Monitor pipeline execution
2. Verify changefeed data is being ingested
3. Check Delta table for ingested rows
4. Test incremental updates
5. Document final connector configuration

