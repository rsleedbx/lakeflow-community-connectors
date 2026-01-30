# Refactoring: Use Reusable Connection Utility

## Problem

Connection logic was duplicated in `run_full_diagnosis_from_config()`:

```python
# Duplicated connection code (20+ lines)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

host_clean = host.split(':')[0] if ':' in host else host

conn = pg8000.native.Connection(
    host=host_clean,
    port=port,
    database=database,
    user=user,
    password=password,
    ssl_context=ssl_context
)
```

This violated DRY principle - we already had `cockroachdb_conn.py` for connection management!

## Solution

### 1. Added Native API Function to `cockroachdb_conn.py`

The existing `get_cockroachdb_connection()` used the old cursor-based API. Added a new function for the modern native API:

```python
def get_cockroachdb_connection_native(
    cockroachdb_host: str,
    cockroachdb_port: int,
    cockroachdb_user: str,
    cockroachdb_password: str,
    cockroachdb_database: str
):
    """
    Create connection using pg8000.native (no cursor needed).
    
    This is the MODERN API - use this for new code!
    """
    # SSL setup
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Parse host (in case port accidentally included)
    host = cockroachdb_host.split(':')[0] if ':' in cockroachdb_host else cockroachdb_host
    
    # Return native connection
    conn = pg8000.native.Connection(
        user=cockroachdb_user,
        password=cockroachdb_password,
        host=host,
        port=cockroachdb_port,
        database=cockroachdb_database,
        ssl_context=ssl_context
    )
    return conn
```

### 2. Updated `run_full_diagnosis_from_config()` to Use Utility

**Before (20+ lines of duplicated code):**
```python
# Create SSL context (required for CockroachDB Cloud)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Parse host (in case port is accidentally included)
host_clean = host.split(':')[0] if ':' in host else host

conn = pg8000.native.Connection(
    host=host_clean,
    port=port,
    database=database,
    user=user,
    password=password,
    ssl_context=ssl_context
)
print("✅ Connection established\n")
```

**After (5 lines using utility):**
```python
from cockroachdb_conn import get_cockroachdb_connection_native

conn = get_cockroachdb_connection_native(
    cockroachdb_host=host,
    cockroachdb_port=port,
    cockroachdb_user=user,
    cockroachdb_password=password,
    cockroachdb_database=database
)
print("✅ Connection established\n")
```

### 3. Removed Unnecessary Imports

Removed from `run_full_diagnosis_from_config()`:
```python
import pg8000.native  # No longer needed
import ssl            # No longer needed
```

These are now encapsulated in `cockroachdb_conn.py`.

## Benefits

### 1. **DRY - Don't Repeat Yourself**
- Connection logic in ONE place: `cockroachdb_conn.py`
- No duplication across files

### 2. **Easier Maintenance**
- Update SSL config? Change once in `cockroachdb_conn.py`
- Fix host parsing bug? Change once
- All users automatically benefit

### 3. **Consistent Behavior**
- All code uses same connection logic
- Same SSL configuration
- Same host parsing
- Same error handling

### 4. **Cleaner Code**
- `run_full_diagnosis_from_config()` is now 15 lines shorter
- Focus on business logic, not connection boilerplate
- Easier to read and understand

### 5. **Two APIs Available**
Now `cockroachdb_conn.py` provides both APIs:

**Old API (cursor-based):**
```python
from cockroachdb_conn import get_cockroachdb_connection

conn = get_cockroachdb_connection(...)
with conn.cursor() as cur:
    cur.execute("SELECT ...")
    result = cur.fetchone()
```

**New API (native):**
```python
from cockroachdb_conn import get_cockroachdb_connection_native

conn = get_cockroachdb_connection_native(...)
result = conn.run("SELECT ...")  # Direct, no cursor
```

## Usage in Notebook

The notebook already uses the old API via a wrapper:

```python
# Cell 5 - Connection setup
from cockroachdb_conn import get_cockroachdb_connection as _get_connection

def get_cockroachdb_connection():
    return _get_connection(
        cockroachdb_host=cockroachdb_host,
        cockroachdb_port=cockroachdb_port,
        cockroachdb_user=cockroachdb_user,
        cockroachdb_password=cockroachdb_password,
        cockroachdb_database=cockroachdb_database
    )
```

Now diagnosis functions can similarly use the native version:

```python
from cockroachdb_conn import get_cockroachdb_connection_native

conn = get_cockroachdb_connection_native(
    cockroachdb_host=config["cockroachdb"]["host"],
    cockroachdb_port=config["cockroachdb"]["port"],
    cockroachdb_user=config["cockroachdb"]["user"],
    cockroachdb_password=config["cockroachdb"]["password"],
    cockroachdb_database=config["cockroachdb"]["database"]
)
```

## Files Changed

### 1. `cockroachdb_conn.py`
- **Added**: `get_cockroachdb_connection_native()` function
- **Updated**: Module docstring to document both APIs
- **Added**: Import for `pg8000.native`

### 2. `cockroachdb_debug.py`
- **Removed**: Duplicated SSL setup code
- **Removed**: Duplicated host parsing code
- **Removed**: Duplicated `pg8000.native.Connection` creation
- **Removed**: Imports for `pg8000.native` and `ssl` from function
- **Added**: Import and use of `get_cockroachdb_connection_native()`

## Testing

Verify the refactoring works:

```python
# Reload to pick up changes
import importlib, cockroachdb_conn, cockroachdb_ycsb, cockroachdb_debug
importlib.reload(cockroachdb_conn)
importlib.reload(cockroachdb_ycsb)
importlib.reload(cockroachdb_debug)

from cockroachdb_debug import run_full_diagnosis_from_config

# Run diagnosis - should work identically
run_full_diagnosis_from_config(spark, config)
```

Should produce identical results, but now using the reusable connection utility!

## Future Improvements

### 1. Connection Pooling
Could add connection pooling to `cockroachdb_conn.py`:

```python
def get_connection_pool(...):
    """Create a connection pool for better performance."""
    pass
```

### 2. Connection Context Manager
Could add context manager support:

```python
with get_cockroachdb_connection_native(...) as conn:
    result = conn.run("SELECT ...")
# Auto-closes connection
```

### 3. Retry Logic
Could add connection retry logic for transient failures:

```python
def get_cockroachdb_connection_native(..., max_retries=3):
    """Connect with automatic retry on failure."""
    pass
```

All these improvements would benefit ALL code using the utility!

## Key Takeaway

**Always use existing utilities instead of duplicating logic!**

Benefits:
- ✅ Less code duplication
- ✅ Easier maintenance
- ✅ Consistent behavior
- ✅ Centralized improvements

When you need to connect to CockroachDB:
- ❌ Don't write connection code inline
- ✅ Use `get_cockroachdb_connection_native()` from `cockroachdb_conn.py`
