# Sink URI Matching Improvement

## Summary

Refactored changefeed detection to use `sink_uri` column instead of parsing the `description` field, resulting in cleaner, more reliable code.

---

## Problem with Previous Approach

### Before: Matching Against `description` Field

The `description` column in `SHOW CHANGEFEED JOBS` contains the entire CREATE statement:

```sql
CREATE CHANGEFEED FOR TABLE usertable INTO 'azure://container/parquet/defaultdb/public/usertable/usertable_cdc?...' WITH format='parquet', updated, resolved='10s'
```

**Issues**:
- ❌ Complex pattern: `%FOR TABLE {source_table}%{path_without_prefix}%`
- ❌ Confusing logic: Had to remove format prefix for matching
- ❌ Fragile: Depends on SQL statement structure
- ❌ Unclear: Why remove prefix? Why include table name?
- ❌ Hard to maintain: Pattern changes if SQL format changes

**Old Code**:
```python
# Remove format prefix (confusing!)
path_without_prefix = path.replace(f"{cdc_format}/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"

cur.execute("""
    SELECT job_id, status, description
    FROM [SHOW CHANGEFEED JOBS] 
    WHERE description LIKE %s
    AND status IN ('running', 'paused')
    LIMIT 1
""", (path_pattern,))
```

---

## Solution: Match Against `sink_uri` Field

### After: Using `sink_uri` Column

The `sink_uri` column contains exactly what we created:

```
azure://container/parquet/defaultdb/public/usertable/usertable_cdc?AZURE_ACCOUNT_NAME=xxx&AZURE_ACCOUNT_KEY=yyy
```

**Benefits**:
- ✅ Simple pattern: `%{container}/{path}%`
- ✅ Clear intent: Match the exact URI we created
- ✅ Robust: Doesn't depend on SQL statement format
- ✅ No string manipulation needed
- ✅ Format prefix stays in pattern (no removal needed)

**New Code**:
```python
# Simple and clear!
sink_uri_pattern = f"%{container_name}/{path}%"

cur.execute("""
    SELECT job_id, status, sink_uri
    FROM [SHOW CHANGEFEED JOBS] 
    WHERE sink_uri LIKE %s
    AND status IN ('running', 'paused')
    LIMIT 1
""", (sink_uri_pattern,))
```

---

## Comparison

### Pattern Matching

**Old (description)**:
```python
path = "parquet/defaultdb/public/usertable/usertable_cdc"
path_without_prefix = path.replace("parquet/", "")  # Why?
pattern = f"%usertable%{path_without_prefix}%"
# = "%usertable%defaultdb/public/usertable/usertable_cdc%"
```

**New (sink_uri)**:
```python
path = "parquet/defaultdb/public/usertable/usertable_cdc"
pattern = f"%{container_name}/{path}%"
# = "%cockroachcdc/parquet/defaultdb/public/usertable/usertable_cdc%"
```

### What Gets Matched

**Old**: Matches description field
```
CREATE CHANGEFEED FOR TABLE usertable INTO 'azure://...' WITH ...
```

**New**: Matches sink_uri field
```
azure://cockroachcdc/parquet/defaultdb/public/usertable/usertable_cdc?...
```

---

## Code Changes

### Cell 9 (Changefeed Creation)

**Before**:
```python
path_without_prefix = path.replace(f"{cdc_format}/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"

cur.execute("""
    SELECT job_id, status, description
    FROM [SHOW CHANGEFEED JOBS] 
    WHERE description LIKE %s
    LIMIT 1  # Only finds first match
""", (path_pattern,))

existing = cur.fetchone()
if existing:
    job_id, status, description = existing
    print(f"Changefeed exists: {job_id}")
```

**After**:
```python
sink_uri_pattern = f"%{container_name}/{path}%"

cur.execute("""
    SELECT job_id, status, sink_uri
    FROM [SHOW CHANGEFEED JOBS] 
    WHERE sink_uri LIKE %s
    # No LIMIT - finds ALL matches to detect duplicates
""", (sink_uri_pattern,))

existing_changefeeds = cur.fetchall()
if existing_changefeeds:
    print(f"Found {len(existing_changefeeds)} changefeed(s)")
    for job_id, status, sink_uri in existing_changefeeds:
        print(f"  • Job {job_id}: {status}")
    if len(existing_changefeeds) > 1:
        print("⚠️  WARNING: Multiple changefeeds detected!")
```

### Cell 17 (Cleanup - Cancel Changefeed)

**Before**:
```python
path_without_prefix = path.replace(f"{cdc_format}/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"

cur.execute("""
    SELECT job_id 
    FROM [SHOW CHANGEFEED JOBS] 
    WHERE description LIKE %s
    LIMIT 1  # Only cancels first match
""", (path_pattern,))

result = cur.fetchone()
if result:
    cur.execute(f"CANCEL JOB {result[0]}")
    print(f"Cancelled changefeed {result[0]}")
```

**After**:
```python
sink_uri_pattern = f"%{container_name}/{path}%"

cur.execute("""
    SELECT job_id, sink_uri
    FROM [SHOW CHANGEFEED JOBS] 
    WHERE sink_uri LIKE %s
    # No LIMIT - cancels ALL matches
""", (sink_uri_pattern,))

changefeeds = cur.fetchall()
if changefeeds:
    print(f"Cancelling {len(changefeeds)} changefeed(s)...")
    for job_id, sink_uri in changefeeds:
        cur.execute(f"CANCEL JOB {job_id}")
        print(f"  ✅ Cancelled Job {job_id}")
    if len(changefeeds) > 1:
        print(f"⚠️  Cancelled {len(changefeeds)} changefeeds (duplicates!)")
```

---

## Why This is Better

### 1. **Clarity**
- Old: "Why are we removing the format prefix?"
- New: "We're matching the exact URI we created"

### 2. **Exactness**
- Old: Partial path matching in SQL statement
- New: Exact URI matching

### 3. **Maintainability**
- Old: Depends on CockroachDB's SQL statement format
- New: Depends on sink URI format (what we control)

### 4. **Simplicity**
- Old: 2 lines of setup (remove prefix, build pattern)
- New: 1 line of setup (build pattern)

### 5. **Format Support**
- Old: Had to remove format prefix (hardcoded behavior)
- New: Format stays in pattern (naturally supports any format)

---

## Example: How It Matches

Given:
- `container_name` = `"cockroachcdc"`
- `path` = `"parquet/defaultdb/public/usertable/usertable_cdc"`

**Pattern**: `"%cockroachcdc/parquet/defaultdb/public/usertable/usertable_cdc%"`

**Matches**:
```
azure://cockroachcdc/parquet/defaultdb/public/usertable/usertable_cdc?AZURE_ACCOUNT_NAME=xxx&AZURE_ACCOUNT_KEY=yyy
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                            This part matches
```

**Doesn't Match**:
```
azure://other-container/parquet/defaultdb/public/usertable/usertable_cdc?...
        ^^^^^^^^^^^^^^^
        Different container - no match!

azure://cockroachcdc/json/defaultdb/public/usertable/usertable_cdc?...
                      ^^^^
                      Different format - no match!

azure://cockroachcdc/parquet/defaultdb/public/other_table/other_table_cdc?...
                                              ^^^^^^^^^^^
                                              Different table - no match!
```

---

## Additional Improvement: Handling Multiple Changefeeds

### Removed `LIMIT 1` for Duplicate Detection

**Why?**

If multiple changefeeds accidentally write to the same destination, we need to:
1. **Detect them** (Cell 9) - Warn the user about duplicates
2. **Clean them all up** (Cell 17) - Cancel ALL duplicates, not just one

**Benefits**:
- ✅ **Duplicate Detection**: Cell 9 now warns if multiple changefeeds exist
- ✅ **Complete Cleanup**: Cell 17 now cancels ALL matching changefeeds
- ✅ **Data Integrity**: Prevents multiple writers causing data corruption
- ✅ **Visibility**: Shows all conflicting changefeeds to the user

### Example Output

**Cell 9 (Detection)**:
```
✅ Changefeed(s) already exist for this source → target mapping
   Found 2 changefeed(s):
   • Job ID: 123456, Status: running
     Sink URI: azure://container/parquet/defaultdb/public/usertable/usertable_cdc?...
   • Job ID: 789012, Status: running
     Sink URI: azure://container/parquet/defaultdb/public/usertable/usertable_cdc?...

⚠️  WARNING: Multiple changefeeds detected for same destination!
   This may cause duplicate data. Consider running Cell 17 to clean up.
```

**Cell 17 (Cleanup)**:
```
🗑️  Cancelling 2 changefeed(s)...
   ✅ Cancelled Job ID: 123456
      Sink URI: azure://container/parquet/defaultdb/public/usertable/usertable_cdc?...
   ✅ Cancelled Job ID: 789012
      Sink URI: azure://container/parquet/defaultdb/public/usertable/usertable_cdc?...

⚠️  Cancelled 2 changefeeds (duplicates detected!)
```

---

## Credit

This improvement was identified by reviewing the actual CockroachDB output:

```sql
defaultdb=> select sink_uri from [show changefeed jobs];

azure://changefeed-events/json/defaultdb/public/test-json_usertable_no_split/1769098972?...
azure://changefeed-events/parquet/defaultdb/public/test-parquet_usertable_with_split/1769098972?...
```

The `sink_uri` column is exactly what we need - no parsing required!

---

## Related Documentation

- `PATH_CENTRALIZATION.md` - Single source for path structure
- `FORMAT_PARAMETERIZATION_SUMMARY.md` - Configurable format support
- `MODULE_REFACTORING_SUMMARY.md` - Overall modularization

---

Date: 2026-01-30
