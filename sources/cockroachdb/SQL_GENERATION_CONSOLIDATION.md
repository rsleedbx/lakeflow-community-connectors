# SQL Generation Consolidation

**Date:** January 7, 2026  
**Status:** ✅ Complete

## Summary

All SQL generation logic for CDC testing has been consolidated into reusable functions in `cockroachdb.py` and exposed via CLI commands in `changefeed_helper.py`. This eliminates duplicate SQL code across test scripts.

## New Functions Added

### 1. `generate_test_insert_sql()`
**Location:** `cockroachdb.py` (lines 2612-2673)  
**Purpose:** Generate INSERT SQL for adding new rows to existing tables  
**CLI Command:** `changefeed_helper.py generate-insert-sql`

**Features:**
- Supports `simple` (id-based) and `ycsb` (ycsb_key-based) schemas
- Deterministic data generation using `generate_series()`
- Configurable row count, start ID, and key prefix
- Non-conflicting keys for CDC testing

**Usage:**
```bash
# YCSB table
python3 changefeed_helper.py generate-insert-sql \
    --table test_ycsb \
    --schema-type ycsb \
    --rows 50 \
    --key-prefix newuser

# Simple table
python3 changefeed_helper.py generate-insert-sql \
    --table test_simple \
    --schema-type simple \
    --rows 50 \
    --start-id 10001
```

### 2. `generate_test_update_sql()`
**Location:** `cockroachdb.py` (lines 2676-2719)  
**Purpose:** Generate UPDATE SQL for modifying existing rows  
**CLI Command:** `changefeed_helper.py generate-update-sql`

**Features:**
- Supports `simple` and `ycsb` schemas
- Deterministic row selection using `ORDER BY` + `LIMIT`
- Updates first N rows by primary key order
- Configurable row count

**Usage:**
```bash
# YCSB table (update first 400 rows)
python3 changefeed_helper.py generate-update-sql \
    --table test_ycsb \
    --schema-type ycsb \
    --rows 400

# Simple table
python3 changefeed_helper.py generate-update-sql \
    --table test_simple \
    --schema-type simple \
    --rows 400
```

### 3. `generate_test_delete_sql()`
**Location:** `cockroachdb.py` (lines 2722-2769)  
**Purpose:** Generate DELETE SQL for removing rows  
**CLI Command:** `changefeed_helper.py generate-delete-sql`

**Features:**
- Supports `simple` and `ycsb` schemas
- Deterministic row selection using `ORDER BY` + `LIMIT`
- Delete from end (DESC) or beginning (ASC) of table
- Configurable row count

**Usage:**
```bash
# YCSB table (delete last 100 rows)
python3 changefeed_helper.py generate-delete-sql \
    --table test_ycsb \
    --schema-type ycsb \
    --rows 100 \
    --from-end

# Simple table
python3 changefeed_helper.py generate-delete-sql \
    --table test_simple \
    --schema-type simple \
    --rows 100 \
    --from-end
```

## Code Eliminated

### Before (test_cdc_matrix.sh)

**Duplicate SQL for usertable:**
```bash
# UPDATE (lines 526)
UPDATE $table SET field0 = field0 || '_updated' 
WHERE ycsb_key IN (SELECT ycsb_key FROM $table ORDER BY ycsb_key LIMIT 400);

# DELETE (lines 531)
DELETE FROM $table 
WHERE ycsb_key IN (SELECT ycsb_key FROM $table ORDER BY ycsb_key DESC LIMIT 100);

# INSERT (lines 536-549)
INSERT INTO $table (ycsb_key, field0, field1, ...)
SELECT 'newuser' || LPAD(i::TEXT, 10, '0'), ...
FROM generate_series(1, 50) AS i;
```

**Duplicate SQL for simple_test:**
```bash
# UPDATE (lines 548)
UPDATE $table SET value = value + 1, updated_at = now() WHERE id <= 400;

# DELETE (lines 553)
DELETE FROM $table WHERE id > 900;

# INSERT (lines 558-560)
INSERT INTO $table (id, name, value)
SELECT 10000 + i, 'inserted_' || i, i * 200
FROM generate_series(1, 50) AS i;
```

### After (test_cdc_matrix.sh)

**All SQL generation delegated to Python functions:**
```bash
# UPDATE
update_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-update-sql \
    --table "$table" \
    --schema-type ycsb \
    --rows 400)

# DELETE
delete_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-delete-sql \
    --table "$table" \
    --schema-type ycsb \
    --rows 100 \
    --from-end)

# INSERT
insert_sql=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-insert-sql \
    --table "$table" \
    --schema-type ycsb \
    --rows 50 \
    --key-prefix newuser)
```

## Benefits

### 1. **Code Reusability**
- All SQL generation logic is now in one place (`cockroachdb.py`)
- Can be used by multiple test scripts, notebooks, and automation tools
- Easy to import and use programmatically:
  ```python
  from cockroachdb import generate_test_update_sql
  sql = generate_test_update_sql('mytable', 'ycsb', 400)
  ```

### 2. **Consistency**
- All tests use the same SQL generation logic
- Deterministic behavior across all test scenarios
- No more copy-paste errors or drift between scripts

### 3. **Maintainability**
- SQL logic changes in ONE place
- Easy to add new features (e.g., support for new schemas)
- Centralized documentation and examples

### 4. **Testability**
- SQL generators can be unit tested independently
- Easier to verify correctness of generated SQL
- Can add validation and error checking in one place

## Complete SQL Generator Suite

| Function | Purpose | Schemas | Key Features |
|----------|---------|---------|--------------|
| `generate_test_table_sql()` | CREATE + INSERT for initial snapshot | simple, ycsb | Column families support, deterministic data |
| `generate_test_insert_sql()` | INSERT new rows (CDC testing) | simple, ycsb | Non-conflicting keys, configurable prefix/start |
| `generate_test_update_sql()` | UPDATE existing rows (CDC testing) | simple, ycsb | Deterministic ORDER BY + LIMIT selection |
| `generate_test_delete_sql()` | DELETE rows (CDC testing) | simple, ycsb | From-end or from-beginning deletion |

## Files Modified

1. **`sources/cockroachdb/cockroachdb.py`**
   - Added `generate_test_insert_sql()` (lines 2612-2673)
   - Added `generate_test_update_sql()` (lines 2676-2719)
   - Added `generate_test_delete_sql()` (lines 2722-2769)

2. **`sources/cockroachdb/scripts/changefeed_helper.py`**
   - Added CLI command: `generate-insert-sql`
   - Added CLI command: `generate-update-sql`
   - Added CLI command: `generate-delete-sql`
   - Updated imports and command handlers

3. **`sources/cockroachdb/scripts/test_cdc_matrix.sh`**
   - Replaced all hardcoded UPDATE SQL with generator call
   - Replaced all hardcoded DELETE SQL with generator call
   - Replaced all hardcoded INSERT SQL with generator call
   - Applied to both `usertable` and `simple_test` workloads

## Testing

To verify the consolidation works correctly:

```bash
cd sources/cockroachdb/scripts

# Run CDC test matrix with new generators
./test_cdc_matrix.sh parquet

# Expected: All tests pass with same results as before
# - Snapshot: correct count
# - Insert: 50 new rows
# - Update: 400 rows modified
# - Delete: 100 rows removed
# - Unique keys: matches final table count
```

## Next Steps

### Potential Future Enhancements

1. **Add more schema types**
   - Support for custom table schemas
   - Schema auto-detection from CockroachDB

2. **Add data generation options**
   - Random vs deterministic data
   - Custom data patterns (e.g., realistic names, emails)
   - Configurable data size for performance testing

3. **Add validation**
   - Verify table exists before generating SQL
   - Check primary key compatibility
   - Validate row counts don't exceed table size

4. **Add transaction support**
   - Generate multi-statement transactions
   - Support for SAVEPOINT and ROLLBACK testing

## Related Documentation

- `sources/cockroachdb/SCHEMA_MANAGEMENT.md` - Schema file format and management
- `sources/cockroachdb/REFACTORING_SUMMARY.md` - Overall refactoring summary
- `sources/cockroachdb/CODE_REVIEW_FINDINGS.md` - Code duplication analysis

---

**Status:** ✅ All SQL generation is now consolidated and reusable!


