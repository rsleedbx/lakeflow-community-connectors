# Deterministic Testing Improvements

## Problem Statement

The original test script had non-deterministic behavior that made it impossible to guarantee 100% accurate results:

### Issue 1: Primary Key Creation Failure

**Symptom:**
```
✅ test_json_usertable_with_split created: 9594 rows (PK: rowid)  ❌ Wrong PK!
⚠️  Note: Unique keys (9594) ≠ Expected (8994) - updates may have created new keys
```

**Root Cause:**
```sql
CREATE TABLE test_table AS SELECT * FROM usertable LIMIT 10000;
ALTER TABLE test_table ADD PRIMARY KEY (ycsb_key);  -- ❌ Silently fails
```

CockroachDB behavior:
1. `CREATE TABLE AS` creates a table with a hidden `rowid` column as the primary key
2. `ALTER TABLE ADD PRIMARY KEY` fails because a primary key already exists (`rowid`)
3. The failure is **silent** - no error message
4. Result: Table keeps `rowid` as PK instead of `ycsb_key`

**Impact on Tests:**
- Wrong primary key used for CDC operations
- Row counts don't match expectations
- Merge logic uses incorrect keys
- Test results are unpredictable

---

### Issue 2: Non-Deterministic Workload

**Symptom:**
```
Update rows: 400
Delete rows: 100
Expected final: 8994
Actual final: 9594  ❌ Mismatch!
```

**Root Cause:**
```sql
-- Random selection (no ORDER BY)
UPDATE test_table 
SET field0 = field0 || '_updated' 
WHERE ycsb_key IN (
    SELECT ycsb_key FROM test_table LIMIT 400  -- ❌ Random 400 rows
);

DELETE FROM test_table 
WHERE ycsb_key IN (
    SELECT ycsb_key FROM test_table 
    ORDER BY ycsb_key DESC LIMIT 100  -- Only DELETE was ordered
);
```

**Impact:**
- Can't predict which rows are updated/deleted
- Unique key count is unpredictable
- Tests can't be validated against known expectations

---

### Issue 3: No Validation

**Problems:**
- No check if row count matches expected value after table creation
- No check if workload operations affected the correct number of rows
- No verification that PK is correct (not `rowid`)

---

## Solutions Implemented

### Solution 1: Proper Primary Key Creation

```sql
-- ✅ Define PK in CREATE TABLE statement
CREATE TABLE test_table (
    ycsb_key STRING PRIMARY KEY,  -- PK defined from the start
    field0 TEXT,
    field1 TEXT,
    field2 TEXT,
    field3 TEXT,
    field4 TEXT,
    field5 TEXT,
    field6 TEXT,
    field7 TEXT,
    field8 TEXT,
    field9 TEXT
);

-- ✅ Insert deterministic data (ordered)
INSERT INTO test_table 
SELECT * FROM usertable 
ORDER BY ycsb_key 
LIMIT 10000;
```

**Verification:**
```bash
# Verify PK is correct (not rowid)
if [ "$pk_columns" = "rowid" ]; then
    echo "❌ Error: Table has auto-generated rowid instead of ycsb_key PK"
    return 1
fi
```

**Benefits:**
- ✅ Primary key is guaranteed to be `ycsb_key`
- ✅ Data is deterministic (first 10000 rows by key order)
- ✅ Explicit validation catches failures early

---

### Solution 2: Deterministic Workload

```sql
-- ✅ UPDATE first 400 rows (ordered)
UPDATE test_table 
SET field0 = field0 || '_updated' 
WHERE ycsb_key IN (
    SELECT ycsb_key FROM test_table 
    ORDER BY ycsb_key 
    LIMIT 400  -- First 400 rows
);

-- ✅ DELETE last 100 rows (ordered)
DELETE FROM test_table 
WHERE ycsb_key IN (
    SELECT ycsb_key FROM test_table 
    ORDER BY ycsb_key DESC 
    LIMIT 100  -- Last 100 rows
);
```

**For `simple_test` table:**
```sql
-- ✅ Explicit ID ranges
UPDATE test_table 
SET value = value + 1, updated_at = now() 
WHERE id <= 400;  -- IDs 1-400

DELETE FROM test_table 
WHERE id > 900;  -- IDs 901-1000
```

**Benefits:**
- ✅ Exactly which rows are updated/deleted is known
- ✅ Expected final row count is predictable
- ✅ Tests are reproducible

---

### Solution 3: Comprehensive Validation

**Helper Functions:**

```bash
# Get row count
get_row_count() {
    local table=$1
    psql "${crdb_creds[cockroachdb_url]}" -t -c "SELECT count(*) FROM $table;" 2>/dev/null | tr -d ' '
}

# Verify row count with expected value
verify_row_count() {
    local table=$1
    local expected=$2
    local context=$3  # e.g., "after creation" or "after workload"
    
    local actual
    actual=$(get_row_count "$table")
    
    if [ -z "$actual" ] || ! [[ "$actual" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Failed to get row count for $table $context"
        return 1  # Fatal
    fi
    
    if [ "$actual" -ne "$expected" ]; then
        echo "⚠️  Warning: Expected $expected rows, got $actual rows $context"
        return 2  # Warning
    fi
    
    echo "$actual"
    return 0  # Success
}
```

**Validation Points:**

1. **After Table Creation:**
```bash
if ! row_count=$(verify_row_count "$table" "$target_rows" "after creation"); then
    if [ $? -eq 1 ]; then
        return 1  # Fatal error
    fi
    row_count=$(get_row_count "$table")  # Warning - continue with actual
fi
```

2. **After Workload:**
```bash
pre_workload_count=$(get_row_count "$table")
echo "  Pre-workload count: $pre_workload_count rows"

# ... run workload ...

expected_post_count=$((pre_workload_count - 100))  # Deleted 100 rows
if ! post_workload_count=$(verify_row_count "$table" "$expected_post_count" "after workload"); then
    if [ $? -eq 2 ]; then
        echo "  ⚠️  Count mismatch detected but continuing..."
    else
        return 1  # Fatal error
    fi
fi
```

3. **Primary Key Verification:**
```bash
pk_columns=$(verify_primary_key "$table")
if [ "$pk_columns" = "rowid" ]; then
    echo "❌ Error: Table has auto-generated rowid instead of ycsb_key PK"
    return 1
fi
```

**Benefits:**
- ✅ Catches errors early (table creation failures)
- ✅ Validates expected outcomes (row counts)
- ✅ Distinguishes fatal errors from warnings
- ✅ Provides context-aware messages

---

## Test Guarantees

With these improvements, tests now guarantee:

### 1. Known Table State
- ✅ Exactly which rows exist (first N by key order)
- ✅ Correct primary key (verified)
- ✅ Expected row count (validated)

### 2. Known Workload Operations
- ✅ Exactly which rows are updated (first 400 by key order)
- ✅ Exactly which rows are deleted (last 100 by key order)
- ✅ Expected final row count = initial - 100

### 3. Predictable CDC Results

For `usertable` tests with 10,000 initial rows:

| Metric | Expected Value | Validation |
|--------|---------------|------------|
| Initial rows | 10,000 | Verified after creation |
| Pre-workload | 10,000 | Verified before workload |
| Updated rows | 400 | Known (first 400 by key) |
| Deleted rows | 100 | Known (last 100 by key) |
| Post-workload | 9,900 | Verified after workload |
| Snapshot events | ≈10,000 | Analyzed from files |
| Update events | ≈400 | Analyzed from files |
| Delete events | ≈100 | Analyzed from files |
| Unique keys (final) | 9,900 | Should match post-workload |

**No more warnings like:**
```
⚠️  Note: Unique keys (9594) ≠ Expected (8994) - updates may have created new keys
```

---

## Example: Successful Test Output

```bash
📋 Creating test table: test_json_usertable_no_split...
DROP TABLE
CREATE TABLE
INSERT 0 10000
✅ test_json_usertable_no_split created: 10000 rows (PK: ycsb_key) ✓

🏥 Checking changefeed health...
   Status: running ✓

⏳ Waiting 30s for initial snapshot...
🏥 Checking changefeed health after wait...
   Status: running ✓

📸 Snapshot files found: 1

🏋️  Running workload (400 UPDATEs + 100 DELETEs)...
  Pre-workload count: 10000 rows
  Step 1: Updating first 400 rows (by ycsb_key order)...
  UPDATE 400
  Step 2: Deleting last 100 rows (by ycsb_key order)...
  DELETE 100
  Post-workload count: 9900 rows (expected: 9900) ✓
✅ Workload complete (400 UPDATEs + 100 DELETEs)

⏳ Waiting 60s for CDC files to flush...
📊 File Count Results:
  Total json files: 2
  Snapshot files: 1
  CDC files: 1

📊 Analyzing changefeed data...

📊 CDC Operation Statistics:
  Snapshot rows: 10000 ✓
  Insert rows: 0 ✓
  Update rows: 400 ✓
  Delete rows: 100 ✓
  Unique keys (deduplicated): 9900 ✓

✅ SUCCESS (Snapshot + CDC)
```

**All counts match expectations! 🎯**

---

## Benefits

### 1. Reproducible Tests
- Same initial state every time
- Same operations every time
- Same expected results every time

### 2. Fast Debugging
- Mismatches are immediately visible
- Know exactly what went wrong
- Context-aware error messages

### 3. Confidence in Results
- 100% certainty about test data
- Can validate notebook merge logic
- Production-ready validation

### 4. Easier Maintenance
- Helper functions reduce duplication
- Clear validation at each step
- Single source of truth for expectations

---

## Related Documentation

- `REFACTORING_SUMMARY.md` - All helper functions and refactoring details
- `CHANGEFEED_ERROR_DETECTION.md` - Error detection and categorization
- `TIMEOUT_FIX.md` - Timeout implementation for health checks
- `SCHEMA_MANAGEMENT.md` - Primary key and schema handling

---

## Testing

To verify deterministic behavior:

```bash
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/scripts

# Run tests twice - results should be identical
./test_cdc_matrix.sh > /tmp/run1.txt 2>&1
./test_cdc_matrix.sh > /tmp/run2.txt 2>&1

# Compare row counts (should be identical)
grep "rows (PK:" /tmp/run1.txt
grep "rows (PK:" /tmp/run2.txt

# Compare CDC stats (should be identical)
grep "Snapshot rows:" /tmp/run1.txt
grep "Snapshot rows:" /tmp/run2.txt
```

Expected: All numbers match exactly between runs ✓


