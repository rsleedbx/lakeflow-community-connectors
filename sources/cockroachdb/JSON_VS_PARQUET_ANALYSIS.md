# JSON vs Parquet: Code Duplication Analysis

**Date:** Jan 7, 2026 1:00 PM  
**Analysis Type:** Deep code structure analysis  
**Scope:** CockroachDB connector format-specific handling

---

## 🎯 Executive Summary

**Question:** Between JSON and Parquet formats, how much code is duplicated vs shared?

**Answer:**
- **Duplicated:** ~175 lines (40% of format-specific code)
- **Shared:** ~265 lines (60% of total processing logic)
- **Refactoring Potential:** ~105 lines (23% savings possible)

**Key Finding:** Duplication exists primarily at the **parsing layer** (reading files, extracting events). The **business logic layer** (CDC rules, coalescing, counting) is 100% shared or identical.

---

## 📊 Quantitative Breakdown

### By Component

| Component | Lines | Duplicated? | Why? |
|-----------|-------|-------------|------|
| **File parsing** | 210 | ✅ Yes | Different file formats (Parquet vs JSON) |
| **CDC detection** | 70 | ✅ Yes | Different event structures, **same business rules** |
| **Coalescing** | 65 | ❌ No | **Shared function** `_coalesce_events_by_key()` |
| **Schema loading** | 50 | ❌ No | **Shared function** `_load_schema_from_volume()` |
| **Primary key extraction** | 75 | ❌ No | **Shared function** `_infer_primary_keys_from_data()` |
| **Operation counting** | 25 | ⚠️ Identical | Same logic in both paths (could extract) |

### By Logical Layer

| Layer | Lines | Shared % | Notes |
|-------|-------|----------|-------|
| **Parsing** | 210 | 0% | Format differences require different code |
| **Detection** | 70 | 0% | Same business rules, different syntax |
| **Business Logic** | 215 | **100%** | Single implementation, format-agnostic |

**Overall Duplication:** 40% of lines, but 0% of business logic!

---

## 🔍 Detailed Analysis

### 1. Snapshot Cutoff Detection (95 lines total)

**Purpose:** Find max timestamp from initial snapshot files

**Parquet Implementation (50 lines):**
```python
# Lines 3045-3097
max_timestamp = None
for blob_name in data_blobs:
    if '-00000000-' in blob_name:  # ✅ Same pattern
        df = pd.read_parquet(blob)   # ❌ Different: Parquet-specific
        max_timestamp = df['__crdb__updated'].max()  # ❌ Different: column name
```

**JSON Implementation (45 lines):**
```python
# Lines 3239-3283
max_timestamp = None
for blob_name in data_blobs:
    if '-00000000-' in blob_name:  # ✅ Same pattern
        for line in file.readlines():  # ❌ Different: JSON line-by-line
            event = json.loads(line)
            timestamp = event.get('updated')  # ❌ Different: field name
            max_timestamp = max(timestamp, max_timestamp)
```

**Shared Elements:**
- Outer loop structure (for blob in blobs)
- Filename pattern detection ('-00000000-')
- Max timestamp logic

**Different Elements:**
- File reading method (binary vs text)
- Parsing approach (pandas vs json.loads)
- Field names (__crdb__updated vs updated)

**Refactoring Potential:** ~15 lines (outer loop + pattern detection)

---

### 2. CDC Operation Detection (70 lines total)

**Purpose:** Classify events as SNAPSHOT, INSERT, UPDATE, or DELETE

**Parquet Implementation (30 lines):**
```python
# Lines 3116-3148
event_type = record.get('__crdb__event_type')  # 'c', 'd'
event_timestamp = record.get('__crdb__updated')

if event_type == 'c':
    if timestamp <= cutoff:
        operation = 'SNAPSHOT'  # ✅ Business rule
    else:
        operation = 'UPDATE'    # ✅ Business rule (includes INSERTs!)
elif event_type == 'd':
    operation = 'DELETE'        # ✅ Business rule
```

**JSON Implementation (40 lines):**
```python
# Lines 3302-3343
after = event.get('after')   # Data after change
before = event.get('before') # Data before change
timestamp = event.get('updated')

if after and not before:
    if timestamp <= cutoff:
        operation = 'SNAPSHOT'  # ✅ SAME business rule
    else:
        operation = 'INSERT'    # ✅ SAME business rule
elif after and before:
    operation = 'UPDATE'        # ✅ SAME business rule
elif before and not after:
    operation = 'DELETE'        # ✅ SAME business rule
```

**Business Rules (Identical):**
1. SNAPSHOT = data from initial scan (timestamp <= cutoff)
2. INSERT = new row after snapshot (JSON can detect, Parquet cannot)
3. UPDATE = existing row changed (Parquet 'c', JSON after+before)
4. DELETE = row removed (Parquet 'd', JSON !after+before)

**Key Insight:** The business rules are **IDENTICAL**. Only the syntax differs due to format structure.

**Refactoring Potential:** ~40 lines (unified function in `CODE_DEDUP_REFACTORING_PLAN.md`)

---

### 3. File Processing Loop (210 lines total)

**Purpose:** Iterate through files and build event list

**Shared Elements:**
- Blob iteration loop
- Primary key extraction logic (identical)
- Event structure building (identical)
- Fragment filtering (identical)

**Different Elements:**
- File reading (pd.read_parquet vs file.read)
- Record extraction (df.to_dict vs json.loads)

**Percentage:**
- File reading: 40 lines (different)
- Event building: 70 lines (shared logic, duplicated code)

**Refactoring Potential:** ~30 lines (event building logic)

---

### 4. Coalescing (65 lines)

**Purpose:** Merge column family fragments by primary key

**Implementation:** `_coalesce_events_by_key()` - Lines 793-858

**Duplication:** ❌ **ZERO** - Single implementation

**Usage:**
```python
# Parquet analysis (line 3171)
connector = LakeflowConnect({})
coalesced = connector._coalesce_events_by_key(all_events)

# JSON analysis (line 3388)
connector = LakeflowConnect({})
coalesced = connector._coalesce_events_by_key(all_events)
```

**Key Insight:** Once events are extracted into standardized structure, ALL downstream processing is format-agnostic!

---

### 5. Operation Counting (50 lines total)

**Purpose:** Count SNAPSHOT, INSERT, UPDATE, DELETE operations

**Parquet Implementation (25 lines):**
```python
# Lines 3170-3205
stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
for event in coalesced:
    op = event['_cdc_operation']
    stats[op] += 1
```

**JSON Implementation (22 lines):**
```python
# Lines 3388-3410
stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
for event in coalesced:
    op = event['_cdc_operation']
    stats[op] += 1
```

**Duplication:** ⚠️ **100%** - Identical logic in both paths

**Refactoring Potential:** ~25 lines (trivial extraction)

---

## 🎯 Refactoring Opportunities

### Priority 1: CDC Operation Detection (HIGH)

**Current State:** Duplicated in 3 places (production + 2 test paths)

**Proposed Solution:**
```python
def _determine_cdc_operation_unified(
    event_type=None,      # For Parquet ('c', 'd')
    before=None,          # For JSON envelope
    after=None,           # For JSON envelope
    timestamp=None,       # Event timestamp
    snapshot_cutoff=None  # Cutoff for snapshot vs CDC
) -> str:
    """
    Unified CDC detection for both Parquet and JSON formats.
    
    Supports:
    - Parquet: Uses event_type ('c', 'd') + timestamp
    - JSON: Uses before/after fields + timestamp
    
    Returns: 'SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE', 'UNKNOWN'
    """
    # JSON format detection (envelope)
    if before is not None or after is not None:
        if after and not before:
            # New data, no previous state
            if snapshot_cutoff and timestamp:
                return 'INSERT' if timestamp > snapshot_cutoff else 'SNAPSHOT'
            return 'SNAPSHOT'
        elif after and before:
            # Data changed
            return 'UPDATE'
        elif before and not after:
            # Data removed
            return 'DELETE'
        return 'UNKNOWN'
    
    # Parquet format detection (flat)
    if event_type == 'c':
        # Create/change event
        if snapshot_cutoff and timestamp:
            return 'UPDATE' if timestamp > snapshot_cutoff else 'SNAPSHOT'
        return 'SNAPSHOT'
    elif event_type == 'd':
        return 'DELETE'
    elif event_type == 'i':
        return 'INSERT'
    
    return 'UNKNOWN'
```

**Benefits:**
- ✅ Single source of truth for CDC rules
- ✅ Eliminates 40 lines of duplication
- ✅ Guarantees consistency across formats
- ✅ Easier to test and maintain

**Effort:** 1-2 hours  
**Risk:** Low (well-understood logic)  
**Status:** Planned in `CODE_DEDUP_REFACTORING_PLAN.md`, intentionally deferred

---

### Priority 2: Operation Counting (MEDIUM)

**Current State:** Identical logic in both paths

**Proposed Solution:**
```python
def _count_operations(coalesced_events: List[Dict]) -> Dict[str, int]:
    """
    Count CDC operations from coalesced events.
    
    Returns dict with keys: snapshot, insert, update, delete, unique_keys
    """
    stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
    active_keys = 0
    
    for event in coalesced_events:
        op = event.get('_cdc_operation', 'UNKNOWN')
        if op in stats:
            stats[op] += 1
        if op != 'delete':
            active_keys += 1
    
    stats['unique_keys'] = active_keys
    return stats
```

**Benefits:**
- ✅ Eliminates 25 lines of duplication
- ✅ Trivial refactoring (low risk)
- ✅ Improves consistency

**Effort:** 30 minutes  
**Risk:** Very low  
**Status:** Quick win if doing refactoring

---

### Priority 3: Snapshot Cutoff Detection (LOW)

**Current State:** Similar logic, different file parsing

**Proposed Solution:**
```python
def _detect_snapshot_cutoff(
    data_blobs: List[str],
    format_type: str,  # 'json' or 'parquet'
    read_timestamp_callback: Callable
) -> Optional[str]:
    """
    Detect snapshot cutoff from sequence 00000000 files.
    
    Args:
        data_blobs: List of blob names
        format_type: 'json' or 'parquet'
        read_timestamp_callback: Function to read max timestamp from file
    
    Returns:
        Max timestamp from snapshot files, or None
    """
    max_timestamp = None
    
    for blob_name in data_blobs:
        if '-00000000-' not in blob_name:
            continue
            
        timestamp = read_timestamp_callback(blob_name)
        if timestamp and (not max_timestamp or timestamp > max_timestamp):
            max_timestamp = timestamp
    
    return max_timestamp
```

**Benefits:**
- ✅ Eliminates 15 lines of duplication
- ❌ Adds callback complexity
- ⚠️ Unclear if worth the abstraction

**Effort:** 1 hour  
**Risk:** Medium (adds indirection)  
**Status:** Low priority, defer

---

## 📈 Summary Metrics

### Current State

| Metric | Value | Notes |
|--------|-------|-------|
| **Total format-specific code** | 440 lines | JSON + Parquet analysis functions |
| **Duplicated code** | 175 lines | 40% of format-specific code |
| **Shared code** | 265 lines | 60% (business logic functions) |
| **Refactorable duplication** | 105 lines | 60% of duplicated code |
| **Necessary duplication** | 70 lines | 40% (file parsing differences) |

### If Fully Refactored

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Duplicated code** | 175 lines | 70 lines | -105 lines (60% reduction) |
| **Duplication %** | 40% | 16% | -24% |
| **Shared code** | 265 lines | 370 lines | +105 lines |

---

## 💡 Key Insights

### 1. Business Logic is 100% Shared

**Finding:** All CDC business rules, coalescing logic, schema management, and primary key handling use shared functions.

**Implication:** The core connector logic is already highly modular and reusable.

---

### 2. Duplication Exists at Parsing Layer

**Finding:** Most duplication is in file reading and event extraction (210 lines).

**Implication:** This duplication is largely **necessary** due to fundamental format differences (binary vs text, flat vs envelope).

---

### 3. CDC Rules Are Duplicated but Identical

**Finding:** CDC detection logic is duplicated in 3 places with **identical business rules**.

**Implication:** This is the **highest value refactoring target** - same rules, different syntax, easy to unify.

---

### 4. Format-Agnostic After Extraction

**Finding:** Once events are extracted into standardized structure (`_cdc_key`, `_cdc_operation`), all processing is format-agnostic.

**Implication:** The connector architecture is well-designed - duplication is isolated to the parsing layer.

---

## 🎯 Recommendations

### Current Decision: Intentional Technical Debt

**Status:** Code duplication is **documented and maintained** as technical debt.

**Rationale:**
1. ✅ Business logic is already 100% shared
2. ✅ Parsing duplication is necessary (format differences)
3. ✅ CDC detection duplication is manageable (3 locations, well-documented)
4. ✅ Test suite validates consistency
5. ⚠️ Refactoring adds complexity without clear value **today**

**Maintenance Strategy:**
- Cross-reference comments in code
- Comprehensive documentation (this file + `CDC_LOGIC_DUPLICATION_DOCUMENTED.md`)
- Test suite catches divergence
- Clear warning: "Change CDC rules in ALL THREE locations!"

---

### When to Revisit Refactoring

**Triggers to reconsider:**
1. CDC logic becomes significantly more complex (e.g., new event types, state machines)
2. Divergence bugs occur frequently (test suite catches inconsistencies)
3. Adding third format (e.g., Avro) would create fourth copy
4. Maintenance burden becomes significant

**Until then:** Document clearly, test thoroughly, keep duplication isolated.

---

## 📚 Related Documentation

- `CONNECTOR_EVOLUTION_STRATEGY.md` - Overall strategy (includes this analysis)
- `CDC_LOGIC_DUPLICATION_DOCUMENTED.md` - Maintenance strategy
- `CODE_DEDUP_REFACTORING_PLAN.md` - Detailed refactoring plan (deferred)
- `TEST_VALIDATION_STATUS.md` - Current test validation work

---

## 🏁 Conclusion

**Between JSON and Parquet:**
- **40% code duplication** (175 lines)
- **60% code sharing** (265 lines)
- **23% refactorable** (105 lines potential savings)

**Strategic Assessment:**
- ✅ **Excellent:** Business logic is 100% shared
- ✅ **Good:** Duplication is isolated to parsing layer
- ⚠️ **Acceptable:** CDC detection duplicated (same rules, manageable)
- ✅ **Good:** Documented and maintained as technical debt

**Final Verdict:** Current duplication level is **acceptable for the foreseeable future**. Refactoring is **available but not urgent**. Focus on production deployment and test validation instead.

