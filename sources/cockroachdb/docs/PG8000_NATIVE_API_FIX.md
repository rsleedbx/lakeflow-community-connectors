# Fix: pg8000.native API Compatibility

## Problem

Functions `get_table_stats()` and `get_column_sum()` were using the old cursor-based API:

```python
with conn.cursor() as cur:
    cur.execute("SELECT ...")
    result = cur.fetchone()
```

But we're using `pg8000.native.Connection` which doesn't have a `cursor()` method.

**Error:**
```
AttributeError: 'Connection' object has no attribute 'cursor'
```

## Root Cause

There are two different APIs in pg8000:

### Old API (DBAPI 2.0):
```python
import pg8000
conn = pg8000.connect(...)
with conn.cursor() as cur:
    cur.execute("SELECT ...")
    result = cur.fetchone()
```

### New API (pg8000.native):
```python
import pg8000.native
conn = pg8000.native.Connection(...)
result = conn.run("SELECT ...")  # Returns list of tuples directly
```

We're using the **new API** (pg8000.native) in the diagnosis function, but the helper functions were using the **old API**.

## Solution

Updated both functions to use `conn.run()` directly:

### 1. Fixed `get_table_stats()`

**Before:**
```python
def get_table_stats(conn, table_name: str) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(ycsb_key), MAX(ycsb_key), COUNT(*) FROM {table_name}")
        result = cur.fetchone()
        min_key, max_key, count = result
        
        return {
            'min_key': min_key,
            'max_key': max_key,
            'count': count,
            'is_empty': min_key is None and max_key is None
        }
```

**After:**
```python
def get_table_stats(conn, table_name: str) -> Dict[str, Any]:
    # pg8000.native uses conn.run() directly (no cursor needed)
    result = conn.run(f"SELECT MIN(ycsb_key), MAX(ycsb_key), COUNT(*) FROM {table_name}")
    min_key, max_key, count = result[0]  # First row of results
    
    return {
        'min_key': min_key,
        'max_key': max_key,
        'count': count,
        'is_empty': min_key is None and max_key is None
    }
```

### 2. Fixed `get_column_sum()`

**Before:**
```python
def get_column_sum(conn, table_name: str, column_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT SUM(...) FROM {table_name}
        """)
        result = cur.fetchone()
        return result[0] if result[0] is not None else 0
```

**After:**
```python
def get_column_sum(conn, table_name: str, column_name: str) -> int:
    # pg8000.native uses conn.run() directly (no cursor needed)
    result = conn.run(f"""
        SELECT SUM(...) FROM {table_name}
    """)
    return result[0][0] if result and result[0][0] is not None else 0
```

## Key Differences

### Old API (cursor-based):
- `cur.fetchone()` returns a tuple: `(value1, value2, value3)`
- Access: `result[0]` for first column

### New API (pg8000.native):
- `conn.run()` returns a list of tuples: `[(value1, value2, value3)]`
- Access: `result[0][0]` for first row, first column

## Files Changed

**File**: `cockroachdb_ycsb.py`

1. `get_table_stats()` - Updated to use `conn.run()`
2. `get_column_sum()` - Updated to use `conn.run()`

Both functions now work with `pg8000.native.Connection`.

## Testing

Verify the fix works:

```python
# Run the all-in-one diagnosis
import importlib, cockroachdb_ycsb, cockroachdb_debug
importlib.reload(cockroachdb_ycsb)
importlib.reload(cockroachdb_debug)
from cockroachdb_debug import run_full_diagnosis_from_config

run_full_diagnosis_from_config(spark, config)
```

Should now work without `AttributeError`.

## Why pg8000.native?

We use `pg8000.native` because:
1. **Simpler API** - No cursor management
2. **Direct results** - Returns data directly
3. **Better for scripts** - Less boilerplate code
4. **Modern approach** - Recommended by pg8000 docs

The old cursor-based API is still supported for backward compatibility, but we chose the newer, cleaner API for our codebase.

## Related Functions

All functions using CockroachDB connection should use `pg8000.native`:

✅ `get_table_stats()` - Fixed
✅ `get_column_sum()` - Fixed  
✅ `get_column_families()` - Already using `conn.run()`
✅ `compare_row_by_row()` - Already using `conn.run()`
✅ `find_mismatched_rows()` - Already using `conn.run()`

All functions are now consistent with pg8000.native API.
