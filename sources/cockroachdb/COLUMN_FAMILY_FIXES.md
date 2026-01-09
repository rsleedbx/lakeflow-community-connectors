# Column Family DELETE/UPDATE Issues - Diagnostic and Fixes

**Date:** Jan 7, 2026  
**Status:** In Progress

---

## 🔍 Issues Identified

### Issue #1: DELETE Event Doubling (4/8 tests affected)

**Affected Tests:**
- Test 1: json_usertable_with_split - `del=200` (expected 100)
- Test 2: json_usertable_no_split - `del=200` (expected 100)
- Test 5: parquet_usertable_with_split - `del=200` (expected 100)
- Test 6: parquet_usertable_no_split - `del=200` (expected 100)

**Pattern:** ONLY tables WITH column families show this issue

**Observation:** Coalescing IS working (101K → 19K events), but DELETE count remains 2×

---

### Issue #2: UPDATE Event Doubling (1/8 tests affected)

**Affected Test:**
- Test 2: json_usertable_no_split - `upd=800` (expected 400)

**Pattern:** ONLY json + usertable + no_split shows this issue

**Correlation:**
- with_split: snap=18,994, upd=400 ✅
- no_split:  snap=18,594, upd=800 ⚠️
- Difference: -400 snapshot, +400 update

---

## 📊 Diagnostic Output Added

### Code Changes (cockroachdb.py ~line 3387)

Added operation breakdown BEFORE and AFTER coalescing:

```python
if debug:
    print(f"   📍 Before coalescing: {len(all_events):,} events")
    # Show operation breakdown BEFORE coalescing
    ops_before = {'SNAPSHOT': 0, 'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
    delete_sample_keys = []
    for e in all_events:
        op = e.get('_cdc_operation', 'UNKNOWN')
        if op in ops_before:
            ops_before[op] += 1
        if op == 'DELETE' and len(delete_sample_keys) < 3:
            delete_sample_keys.append(e.get('_cdc_key', []))
    print(f"      Operations before: snap={ops_before['SNAPSHOT']}, ins={ops_before['INSERT']}, upd={ops_before['UPDATE']}, del={ops_before['DELETE']}")
    if delete_sample_keys:
        print(f"      Sample DELETE _cdc_keys: {delete_sample_keys}")

coalesced_events = connector._coalesce_events_by_key(all_events)

if debug:
    print(f"   📍 After coalescing: {len(coalesced_events):,} events")
    ops_after = {'SNAPSHOT': 0, 'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
    for e in coalesced_events:
        op = e.get('_cdc_operation', 'UNKNOWN')
        if op in ops_after:
            ops_after[op] += 1
    print(f"      Operations after: snap={ops_after['SNAPSHOT']}, ins={ops_after['INSERT']}, upd={ops_after['UPDATE']}, del={ops_after['DELETE']}")
```

**Expected Output:** This will show if DELETE count changes during coalescing

---

## 🔬 Root Cause Analysis

### Hypothesis 1: DELETE events have different _cdc_key values

**Theory:** Column family DELETE events might have slightly different PK values or structure

**Test:** Run test_cdc_matrix.sh and examine "Sample DELETE _cdc_keys" output

**If confirmed:** Fix `_cdc_key` extraction logic to ensure consistent keys

---

### Hypothesis 2: DELETE events are being duplicated AFTER coalescing

**Theory:** Something in the counting logic is duplicating DELETE events

**Test:** Check if `ops_before['DELETE']` equals `ops_after['DELETE']`

**If confirmed:** Fix counting logic or coalescing merge logic

---

### Hypothesis 3: Coalescing merge logic doesn't preserve operation correctly

**Theory:** When merging events with `_cdc_operation='DELETE'`, the field might be overwritten

**Test:** Add debug to print merged event's `_cdc_operation` field

**If confirmed:** Fix merge logic at lines 850-855 to handle `_cdc_operation` specially

---

## 💡 Proposed Solutions

### Solution #1: Fix Coalescing for DELETE Events

**Problem:** Current coalescing uses "last-non-null" semantics which might not be appropriate for all fields

**Fix:** Modify `_coalesce_events_by_key()` to handle certain fields specially:

```python
def _coalesce_events_by_key(self, events: List[Dict]) -> List[Dict]:
    """... existing docstring ..."""
    from collections import defaultdict
    
    # ... existing grouping logic ...
    
    # Merge events for each key
    coalesced = []
    for key_tuple, key_events in key_to_events.items():
        merged = {}
        
        # Special handling for CDC operation field
        # Take the LAST operation (chronologically) based on timestamp
        last_operation = None
        last_timestamp = None
        
        # Take last non-null value for each field
        for event in key_events:
            # Track CDC operation by timestamp
            event_timestamp = event.get('_cdc_updated')
            event_operation = event.get('_cdc_operation')
            if event_operation and (last_timestamp is None or event_timestamp > last_timestamp):
                last_operation = event_operation
                last_timestamp = event_timestamp
            
            for field, value in event.items():
                if value is not None:
                    merged[field] = value
        
        # Ensure we use the chronologically last operation
        if last_operation:
            merged['_cdc_operation'] = last_operation
        
        coalesced.append(merged)
    
    return coalesced
```

**Rationale:** DELETE should win over other operations if it's the last chronological event

---

### Solution #2: Add Operation Priority Logic

**Problem:** When merging events from different column families, need to determine which operation "wins"

**Fix:** Add priority logic:

```python
# Operation priority: DELETE > UPDATE > INSERT > SNAPSHOT
operation_priority = {'DELETE': 4, 'UPDATE': 3, 'INSERT': 2, 'SNAPSHOT': 1, 'UNKNOWN': 0}

# In merge loop:
for event in key_events:
    event_operation = event.get('_cdc_operation')
    current_priority = operation_priority.get(event_operation, 0)
    best_priority = operation_priority.get(merged.get('_cdc_operation'), 0)
    
    if current_priority > best_priority:
        merged['_cdc_operation'] = event_operation
```

**Rationale:** DELETE is the "final" operation and should override earlier operations

---

### Solution #3: Debug and Validate _cdc_key Construction

**Problem:** Need to verify that _cdc_key is built consistently

**Fix:** Add validation in event building:

```python
# After building cdc_key_pairs
if cdc_operation == 'DELETE' and debug:
    print(f"   🔍 DELETE event: key={cdc_key_pairs}, timestamp={event_timestamp}, source={blob_name}")

# Validate key consistency
if len(cdc_key_pairs) < len(primary_key_columns):
    if debug:
        print(f"   ⚠️  Incomplete PK for {cdc_operation}: {cdc_key_pairs} (expected {len(primary_key_columns)} columns)")
    file_skipped += 1
    continue
```

---

## 🔧 Implementation Plan

### Step 1: Run Diagnostic (5 minutes)

Run test_cdc_matrix.sh with new debug output:

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

Look for:
- "Operations before" vs "Operations after" - do DELETE counts change?
- "Sample DELETE _cdc_keys" - are they identical or different?

---

### Step 2: Analyze Results (10 minutes)

Based on diagnostic output, determine:
1. Are DELETE events being merged? (count before vs after)
2. Do DELETE events have consistent _cdc_keys?
3. Is the issue in coalescing or counting?

---

### Step 3: Implement Fix (30 minutes)

Based on Step 2, apply appropriate solution:
- If keys are different → Fix _cdc_key construction
- If keys are same but not merging → Fix coalescing grouping logic
- If merging but operation lost → Fix merge logic (Solution #1 or #2)

---

### Step 4: Validate Fix (30 minutes)

Run test_cdc_matrix.sh again and verify:
- simple_test tests still show del=100 ✅
- usertable tests now show del=100 (not 200) ✅
- json_usertable_no_split shows upd=400 (not 800) ✅

---

## 📝 Expected Test Results After Fix

```
Test 1/8: json_usertable_with_split
  Snapshot rows: ~10000
  Insert rows: 50
  Update rows: 400
  Delete rows: 100  ← Fixed from 200

Test 2/8: json_usertable_no_split
  Snapshot rows: ~10000  
  Insert rows: 50
  Update rows: 400  ← Fixed from 800
  Delete rows: 100  ← Fixed from 200

Tests 3-8: Should remain unchanged (already correct)
```

---

## 🎓 Key Learnings

### About Column Families

1. **CockroachDB emits separate events per column family** when `split_column_families=true`
2. **Each event contains:** PK + columns from that specific family
3. **For DELETE:** Both column families emit DELETE events with the same PK
4. **For UPDATE:** Both column families emit UPDATE events

### About Coalescing

1. **Purpose:** Merge fragmented events by primary key
2. **Method:** Group by `_cdc_key` (PK only), merge with last-non-null semantics
3. **Challenge:** Need to preserve correct `_cdc_operation` when merging
4. **Solution:** Use timestamp or priority to determine final operation

---

## 📚 Related Files

- `cockroachdb.py` - Lines 793-858 (coalescing logic)
- `cockroachdb.py` - Lines 3387-3420 (JSON analysis with new debug)
- `cockroachdb.py` - Lines 3170-3205 (Parquet analysis)
- `test_cdc_matrix.sh` - Test execution script
- `TEST_RESULTS_FINAL.md` - Complete test results analysis

---

## ✅ Next Steps

1. ⏳ **Run diagnostic test** to see detailed operation counts
2. 🔍 **Analyze output** to determine root cause  
3. 🔧 **Implement appropriate fix** based on findings
4. ✅ **Validate** all 8 tests show correct counts
5. 📝 **Document** final solution and update strategy

---

**Status:** Diagnostic output added, awaiting test run to determine exact root cause and implement fix.

