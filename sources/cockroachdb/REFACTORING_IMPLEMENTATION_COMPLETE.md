# Refactoring Implementation Complete: SQL Moved to Python

## ✅ Issues Fixed

### Issue #1: Primary Key Verification Duplication
**Status: FIXED**

**Before:**
```bash
# In test_cdc_matrix.sh (lines 178-195)
verify_primary_key() {
    local table=$1
    local pk_columns
    
    pk_columns=$(psql "${crdb_creds[cockroachdb_url]}" -t -c "
        SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
        FROM information_schema.key_column_usage
        WHERE table_name = '$table'
        AND constraint_name LIKE '%_pkey';" 2>/dev/null | tr -d ' ')
    
    # ... error handling ...
}
```

**After:**
```bash
# In test_cdc_matrix.sh - Now calls Python
verify_primary_key() {
    local table=$1
    local pk_columns
    
    pk_columns=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" get-primary-keys \
        --table "$table" \
        --json "$CRDB_JSON" \
        --catalog defaultdb \
        --schema public 2>&1)
    
    # ... error handling ...
}
```

**New Python Methods:**
- `cockroachdb.py`: Added `get_primary_keys()` utility function
- `changefeed_helper.py`: Added `get-primary-keys` CLI command

---

### Issue #2: SQL Generation Duplication & Base Table Dependency
**Status: FIXED**

**Before:**
- `simple_test`: Used `generate_series` ✅ (good but not reusable)
- `usertable`: Copied from base table ❌ (bad - non-deterministic, requires base table)

**After:**
- **Both** use `generate_series` via reusable Python method ✅
- **No base table dependency** ✅
- **100% deterministic** ✅
- **Fully reusable** ✅

**New SQL Generation:**
```bash
# simple_test
sql_commands=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-test-sql \
    --table "$table" \
    --schema-type simple \
    --rows 1000)

# usertable (YCSB schema) - with optional column families
sql_commands=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" generate-test-sql \
    --table "$table" \
    --schema-type ycsb \
    --rows 10000 \
    --families)  # Only for with_split tests
```

**New Python Methods:**
- `cockroachdb.py`: Added `generate_test_table_sql()` function
- `changefeed_helper.py`: Added `generate-test-sql` CLI command

---

## 📊 Changes Summary

### `cockroachdb.py` - New Utility Functions

**1. `get_primary_keys(crdb_config, table_name, catalog, schema)` → List[str]**
   - Reusable function to get primary key columns
   - Returns: `['id']` or `['ycsb_key']`
   - Eliminates SQL duplication in shell scripts

**2. `generate_test_table_sql(table_name, schema, row_count, include_column_families)` → str**
   - Generates deterministic test data with `generate_series()`
   - Schemas:
     - `'simple'`: INT PK (id), name, value, updated_at
     - `'ycsb'`: VARCHAR PK (ycsb_key), field0-field9 (TEXT)
   - Column families: Optional for YCSB schema
   - Returns complete SQL: DROP + CREATE + INSERT

---

### `changefeed_helper.py` - New CLI Commands

**1. `get-primary-keys`**
```bash
python3 changefeed_helper.py get-primary-keys \
  --table usertable \
  --json cockroachdb_credentials.json \
  --catalog defaultdb \
  --schema public

# Output: ycsb_key
# (comma-separated for multiple PKs)
```

**2. `generate-test-sql`**
```bash
# Simple table
python3 changefeed_helper.py generate-test-sql \
  --table test_simple \
  --schema-type simple \
  --rows 1000

# YCSB table with column families
python3 changefeed_helper.py generate-test-sql \
  --table test_ycsb \
  --schema-type ycsb \
  --rows 10000 \
  --families

# Output: Full SQL ready to execute
```

---

### `test_cdc_matrix.sh` - Refactored to Use Python

**Changes:**
1. ✅ `verify_primary_key()` now calls Python (no inline SQL)
2. ✅ `simple_test` creation uses Python SQL generator
3. ✅ `usertable` creation uses Python SQL generator (no base table dependency!)
4. ✅ Removed base table verification (no longer needed)

**Key Improvement:**
```bash
# BEFORE: Non-deterministic, base table dependency
base_row_count=$(psql ... -c "SELECT count(*) FROM usertable;" ...)
target_rows=$((base_row_count < 10000 ? base_row_count : 10000))
INSERT INTO $table SELECT * FROM usertable ORDER BY ycsb_key LIMIT $target_rows;
# Result: 9594 rows (depends on base table state)

# AFTER: Deterministic, self-contained
sql_commands=$(python3 ... generate-test-sql --rows 10000 ...)
psql ... <<EOF
$sql_commands
EOF
# Result: ALWAYS 10000 rows (deterministic!)
```

---

## 🎯 Benefits

### 1. No Duplication ✅
- Single source of truth for PK queries
- Single source of truth for test table SQL
- Easier to maintain and update

### 2. Better Testing ✅
- **Deterministic**: Always same data every run
- **Self-contained**: No base table dependencies
- **Fast**: `generate_series()` is faster than SELECT+INSERT

### 3. Reusable ✅
- All SQL logic in Python for future script conversion
- CLI commands work from any shell script
- Easy to call from notebooks or other Python code

### 4. Correct Counts ✅
- **Issue #3 Resolution**: Tests will now show correct snapshot counts
  - `simple_test`: 1000 rows (was always correct)
  - `usertable`: 10000 rows (was 9594 due to base table dependency)

---

## 🧪 Testing

### Expected Behavior After Refactoring

**simple_test:**
- Rows created: **1000** (unchanged)
- Primary key: `id`
- Deterministic: ✅

**usertable (YCSB):**
- Rows created: **10000** (was 9594 - FIXED!)
- Primary key: `ycsb_key`
- Deterministic: ✅ (was non-deterministic - FIXED!)
- Column families: ✅ Auto-generated when `--families` flag used

**Expected Results:**
```
Test 1/4: parquet_usertable_with_split
  - Initial rows: 10000
  - Snapshot: 10000 (was 9594 - FIXED!)
  - Update: 400
  - Delete: 100
  - Insert: 50
  - Final: 9950 rows

Test 2/4: parquet_usertable_no_split
  - Initial rows: 10000
  - Snapshot: 10000 (was 9594 - FIXED!)
  - Update: 400
  - Delete: 100
  - Insert: 50
  - Final: 9950 rows

Test 3/4: parquet_simple_test_with_split
  - Initial rows: 1000
  - Snapshot: 1000
  - Update: 400
  - Delete: 100
  - Insert: 50
  - Final: 950 rows

Test 4/4: parquet_simple_test_no_split
  - Initial rows: 1000
  - Snapshot: 1000
  - Update: 400
  - Delete: 100
  - Insert: 50
  - Final: 950 rows
```

### Enhanced CDC Testing

**NEW: Complete CDC Operation Coverage** ✅

The workload now tests ALL CDC operations:
- **UPDATE**: 400 rows modified
- **DELETE**: 100 rows removed
- **INSERT**: 50 new rows added

**Workload Flow:**
```
simple_test:
  Start: 1000 rows (id 1-1000)
  → Update 400 rows (id 1-400)
  → Delete 100 rows (id 901-1000)
  → Insert 50 rows (id 10001-10050)
  Final: 950 rows

usertable (YCSB):
  Start: 10000 rows (user0000000001-user0000010000)
  → Update 400 rows (first 400 by key order)
  → Delete 100 rows (last 100 by key order)
  → Insert 50 rows (newuser0000000001-newuser0000000050)
  Final: 9950 rows
```

**CDC Analysis Verifies:**
- Snapshot events: Initial table state
- Insert events: New rows from Step 3
- Update events: Modified rows from Step 1
- Delete events: Removed rows from Step 2
- Unique keys: Deduplicated final state

### How to Verify

Run the test again with the new code:
```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh parquet 2>&1 | tee /tmp/test_refactored_$(date +%Y%m%d_%H%M%S).log

# Check for:
# ✅ test_parquet_usertable_* created: 10,000 rows  (not 9594!)
# ✅ Workload complete (400 UPDATEs + 100 DELETEs + 50 INSERTs)
# ✅ Snapshot rows: 10000  (not 9594!)
# ✅ Insert rows: 50  (NEW!)
# ✅ Update rows: 400
# ✅ Delete rows: 100
# ✅ Unique keys: 9950  (10000 - 100 + 50)
```

---

## 📝 Migration Notes

### For Future Python Conversion

All SQL is now in `cockroachdb.py`, making it easy to convert `test_cdc_matrix.sh` to pure Python:

```python
# Future Python version of test_cdc_matrix.sh
from cockroachdb import (
    create_connector,
    get_primary_keys,
    generate_test_table_sql,
    load_crdb_config
)

# Get primary keys
pk_columns = get_primary_keys(crdb_config, 'usertable')

# Generate and execute SQL
sql = generate_test_table_sql('test_ycsb', 'ycsb', 10000, include_column_families=True)
connector.execute_sql(sql, commit=True)
```

No SQL needs to be rewritten - just call the existing functions!

---

## ✅ Completion Checklist

- [x] Added `get_primary_keys()` to cockroachdb.py
- [x] Added `generate_test_table_sql()` to cockroachdb.py
- [x] Added `get-primary-keys` CLI command to changefeed_helper.py
- [x] Added `generate-test-sql` CLI command to changefeed_helper.py
- [x] Updated `verify_primary_key()` in test_cdc_matrix.sh
- [x] Updated `simple_test` creation in test_cdc_matrix.sh
- [x] Updated `usertable` creation in test_cdc_matrix.sh (no base table!)
- [x] Removed base table verification (no longer needed)
- [ ] Test with fresh run to verify correct counts

---

## 🔄 Next Steps

1. **Run Fresh Test**: Execute `./test_cdc_matrix.sh parquet` to verify new code works
2. **Verify Counts**: Confirm snapshot counts are now 10000 (not 9594)
3. **Update Documentation**: Add examples to `CODE_REVIEW_FINDINGS.md`
4. **Consider Python Conversion**: `test_cdc_matrix.sh` → `test_cdc_matrix.py` (all SQL is ready!)

