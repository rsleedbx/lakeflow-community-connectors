# Test Count Analysis - Suspicious Numbers Investigation

## Current Test Results

```
Test 1/8: json_usertable_with_split    - snap=19044 ins=0 upd=400 del=200
Test 2/8: json_usertable_no_split      - snap=18644 ins=0 upd=800 del=200
Test 3/8: json_simple_test_with_split  - snap=550   ins=0 upd=400 del=100
Test 4/8: json_simple_test_no_split    - snap=550   ins=0 upd=400 del=100
Test 5/8: parquet_usertable_with_split - snap=18994 ins=0 upd=450 del=200
Test 6/8: parquet_usertable_no_split   - snap=18994 ins=0 upd=450 del=200
Test 7/8: parquet_simple_test_with_split - snap=500 ins=0 upd=450 del=100
Test 8/8: parquet_simple_test_no_split - snap=500   ins=0 upd=450 del=100
```

## Expected Workload

From `test_cdc_matrix.sh`:
- **400 UPDATEs** (first 400 rows)
- **100 DELETEs** (last 100 rows)
- **50 INSERTs** (new rows)

## Issues Identified

### 1. ✅ Parquet upd=450 is CORRECT!

**Explanation**: Parquet format cannot distinguish INSERT from UPDATE!

CockroachDB Parquet changefeeds use `__crdb__event_type`:
- `'c'` = CREATE/CHANGE (ambiguous - could be snapshot, insert, OR update)
- `'d'` = DELETE

To distinguish snapshot from CDC, we use timestamps:
- `timestamp <= cutoff` → SNAPSHOT
- `timestamp > cutoff` → UPDATE (includes inserts!)

**Result**: 450 = 400 updates + 50 inserts ✅

### 2. ❌ JSON ins=0 should be 50

**Status**: Investigating with new debug output

**Hypothesis**: Snapshot cutoff detection may not be working for JSON files

**Debug Added**:
- 📍 Total files being scanned
- 📍 First snapshot file found
- 📍 Snapshot cutoff timestamp
- 📍 First INSERT detection

### 3. ❌ usertable del=200 should be 100 (ALL formats!)

**Pattern**:
```
usertable (has column families):
  - ALL tests show del=200 (2× expected)

simple_test (no column families):
  - ALL tests show del=100 (correct)
```

**Hypothesis**: CockroachDB emits TWO DELETE events per row for tables with column families:
1. DELETE for primary column family
2. DELETE for data column family

**Debug Added**:
- 📍 Event count before coalescing
- 📍 Event count after coalescing  
- 📍 Sample primary keys for each operation type

### 4. ❌ json_usertable_no_split upd=800 should be 400

**Pattern**: Only `json_usertable_no_split` shows this issue

**Hypothesis**: Without `split_column_families`, JSON might be emitting duplicate UPDATE events

## Debug Output to Watch For

Run `./test_cdc_matrix.sh` and look for:

### JSON Snapshot Cutoff Detection:
```
📍 Determining snapshot cutoff from JSON files...
📍 Total files to scan: X
📍 First snapshot file: ...
📍 Scanned X snapshot files
✅ JSON snapshot cutoff timestamp: 1767754504050846623.0000000000
```

### INSERT Detection:
```
📍 First INSERT detected: timestamp=X > cutoff=Y
```

### Coalescing (Deduplication):
```
📍 Before coalescing: 20,000 events
📍 After coalescing: 10,000 events
📍 Operation samples: {'SNAPSHOT': [...], 'DELETE': [...], ...}
```

## Next Steps

1. **Run test with new debug output**
2. **Check if snapshot cutoff is being detected**
3. **Verify coalescing is working (before/after counts)**
4. **Examine DELETE event samples for usertable**
5. **Check if DELETE events have different column family markers**

## Related Files

- `cockroachdb.py` - Lines 3239-3420 (JSON analysis with new debug)
- `test_cdc_matrix.sh` - Test orchestration
- `changefeed_helper.py` - CLI wrapper for analysis

## Expected Resolution

After fixes:
```
JSON tests:
  - simple_test: snap=1000, ins=50, upd=400, del=100 ✅
  - usertable:   snap=10000, ins=50, upd=400, del=100 ✅

Parquet tests:
  - simple_test: snap=1000, ins=0, upd=450, del=100 ✅ (450=400+50)
  - usertable:   snap=10000, ins=0, upd=450, del=100 ✅ (450=400+50)
```

Note: Parquet `ins=0, upd=450` is CORRECT and expected behavior!


