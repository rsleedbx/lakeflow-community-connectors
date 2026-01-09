# pg8000 Parameter Indexing Fix

## The Problem

When using `pg8000.native.Connection.run()` with parameterized queries, we encountered:

```
IndexError: list index out of range
File "pg8000/native.py", line 200, in make_vals
    vals.append(arg_list[p])
```

## Root Cause

**pg8000.native uses 0-based parameter indexing**, not PostgreSQL's standard 1-based indexing!

| Database Driver | Placeholder Format | Example Query |
|----------------|-------------------|---------------|
| PostgreSQL (standard) | `$1, $2, $3, ...` | `SELECT * FROM t WHERE id = $1` |
| psycopg2 | `%s, %s, %s, ...` | `SELECT * FROM t WHERE id = %s` |
| **pg8000.native** | **`$0, $1, $2, ...`** | **`SELECT * FROM t WHERE id = $0`** |

## The Error Sequence

When we used standard 1-based indexing:

```python
# Query: "SELECT * FROM tables WHERE schema = $1"
# Params: ('public',)
conn.run(query, *params)

# Inside pg8000.native.Connection.run():
# 1. Parses query, finds $1 placeholder
# 2. Calls make_vals(args) where args = ('public',)
# 3. make_vals tries: vals.append(arg_list[1])
# 4. But arg_list = ['public'] only has index 0
# 5. IndexError: list index out of range
```

## The Fix

Convert psycopg2-style `%s` placeholders to pg8000-style `$0, $1, $2, ...` (0-based):

```python
def _execute_query(self, conn, query: str, params: tuple = None):
    if self._current_driver == "pg8000":
        # Convert %s to $0, $1, $2, ... (0-based indexing for Python lists)
        converted_query = query
        if params and '%s' in query:
            param_num = 0  # Start at 0, not 1!
            while '%s' in converted_query:
                converted_query = converted_query.replace('%s', f'${param_num}', 1)
                param_num += 1
        
        # Pass params as separate positional arguments
        if params:
            return conn.run(converted_query, *params)
        else:
            return conn.run(converted_query)
```

## Examples

### Before (WRONG - 1-based):
```python
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
converted = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1"
# ❌ IndexError when pg8000 tries to access arg_list[1]
```

### After (CORRECT - 0-based):
```python
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
converted = "SELECT table_name FROM information_schema.tables WHERE table_schema = $0"
# ✅ Works! pg8000 accesses arg_list[0]
```

## Testing

To verify this locally, run:

```bash
export COCKROACH_PASSWORD='your-password'
python3 test_pg8000_direct.py
```

The test will show whether `$0` or `$1` indexing works with your pg8000 version.

## Implementation History

- **Attempt #19**: Used 1-based indexing ($1, $2, $3) → IndexError
- **Attempt #20**: Tried passing params as list instead of unpacking → Still IndexError
- **Attempt #21**: Switched to 0-based indexing ($0, $1, $2) → **SUCCESS** ✅

## References

- pg8000 Issue: This behavior is non-standard and differs from PostgreSQL wire protocol
- Workaround: Use 0-based indexing for pg8000.native specifically
- Alternative: Use pg8000's regular (non-native) cursor API which follows standard DB-API 2.0








