# Two-Variable Design: Separation of Concerns

## Overview

The notebook now uses **two independent variables** to control CDC behavior, providing a cleaner separation of concerns:

1. **`cdc_mode`**: Controls **CDC processing logic** (what to do with events)
2. **`column_family_mode`**: Controls **table structure** (how data is organized)

This design is more flexible and easier to understand than the previous single-variable approach.

---

## Why Two Variables?

### **Problem with Single Variable**

**Before (3 modes):**
- `append-only` → Append-only CDC, single family
- `update-delete` → MERGE CDC, single family
- `column-family` → MERGE CDC, multiple families ❌ Mixed concerns!

**Issues:**
- `column-family` implied MERGE (coupling two orthogonal concerns)
- No way to do append-only WITH column families
- Confusing: "column-family" sounded like a table structure, but also meant MERGE

### **Solution: Separate the Concerns**

**After (2 × 2 = 4 combinations):**

| `cdc_mode` | `column_family_mode` | Function | Use Case |
|------------|---------------------|----------|----------|
| `append-only` | `single_cf` | `ingest_cdc_append_only_single_family()` | Audit log, standard table |
| `append-only` | `multi_cf` | `ingest_cdc_append_only_multi_family()` | Audit log, wide table |
| `update-delete` | `single_cf` | `ingest_cdc_with_merge_single_family()` | Current state, standard table |
| `update-delete` | `multi_cf` | `ingest_cdc_with_merge_multi_family()` | Current state, wide table |

**Benefits:**
- ✅ Orthogonal concerns (CDC logic vs. table structure)
- ✅ All 4 combinations available
- ✅ Clear naming: each variable controls one aspect
- ✅ Easier to understand and test

---

## Variable 1: `cdc_mode`

**What it controls:** **CDC event processing logic**

### **Values:**

#### **`"append-only"`**
- **Behavior**: Store ALL CDC events as rows
- **DELETE events**: Stored as rows with `_cdc_operation='DELETE'`
- **UPDATE events**: Stored as multiple rows (history preserved)
- **Target table**: Growing over time (append-only)
- **Use case**: Audit logs, time-series, full history

#### **`"update-delete"`**
- **Behavior**: Apply MERGE logic to target table
- **DELETE events**: Remove rows from target
- **UPDATE events**: Modify existing rows
- **Target table**: Current state (deduplicated)
- **Use case**: Production replication, current state sync

---

## Variable 2: `column_family_mode`

**What it controls:** **Table structure and changefeed configuration**

### **Values:**

#### **`"single_cf"`**
- **Table structure**: 1 column family (default)
- **Changefeed**: `split_column_families=false` (or omitted)
- **Files generated**: 1 Parquet file per CDC event
- **Merging needed**: No
- **Performance**: Faster (no fragment merging overhead)
- **Use case**: Most tables (< 50 columns)

#### **`"multi_cf"`**
- **Table structure**: 3 column families (for this tutorial)
- **Changefeed**: `split_column_families=true`
- **Files generated**: Multiple Parquet files per CDC event (fragments)
- **Merging needed**: Yes (by primary key + timestamp + operation)
- **Performance**: Slower (~10-20% overhead for merging)
- **Use case**: Wide tables (50+ columns) with selective access patterns

---

## Configuration (Cell 1)

```python
# ============================================================================
# CDC MODE SELECTION
# ============================================================================

# 1. CDC Processing Mode (how to process CDC events)
cdc_mode = "append-only"  # or "update-delete"

# 2. Column Family Mode (table structure)
column_family_mode = "single_cf"  # or "multi_cf"

# ============================================================================
# FUNCTION SELECTION MATRIX
# ============================================================================
# | cdc_mode      | column_family_mode | Function Called                         |
# |---------------|--------------------|-----------------------------------------|
# | append-only   | single_cf          | ingest_cdc_append_only_single_family()  |
# | append-only   | multi_cf           | ingest_cdc_append_only_multi_family()   |
# | update-delete | single_cf          | ingest_cdc_with_merge_single_family()   |
# | update-delete | multi_cf           | ingest_cdc_with_merge_multi_family()    |
# ============================================================================
```

---

## Function Selection (Cell 11)

Cell 11 now uses **both variables** to select the appropriate function:

```python
if cdc_mode == "append-only" and column_family_mode == "single_cf":
    query = ingest_cdc_append_only_single_family(...)
    
elif cdc_mode == "append-only" and column_family_mode == "multi_cf":
    query = ingest_cdc_append_only_multi_family(...)
    
elif cdc_mode == "update-delete" and column_family_mode == "single_cf":
    result = ingest_cdc_with_merge_single_family(...)
    query = result["query"]
    
elif cdc_mode == "update-delete" and column_family_mode == "multi_cf":
    result = ingest_cdc_with_merge_multi_family(...)
    query = result["query"]
    
else:
    raise ValueError(f"Invalid combination: {cdc_mode} + {column_family_mode}")
```

---

## Table Creation (Cell 6)

Cell 6 uses **`column_family_mode`** to determine table structure:

```python
if column_family_mode == "multi_cf":
    # Create table with 3 column families
    CREATE TABLE usertable (
        ycsb_key INT PRIMARY KEY,
        field0 TEXT, field1 TEXT, field2 TEXT,
        FAMILY frequently_read (ycsb_key, field0, field1, field2),
        ...
    )
else:
    # Create table with 1 column family (default)
    CREATE TABLE usertable (
        ycsb_key INT PRIMARY KEY,
        field0 TEXT,
        ...
    )
```

---

## Changefeed Creation (Cell 8)

Cell 8 uses **`column_family_mode`** for changefeed options:

```python
if column_family_mode == "multi_cf":
    changefeed_options = """
        format='parquet',
        updated,
        resolved='10s',
        split_column_families=true
    """
else:
    changefeed_options = """
        format='parquet',
        updated,
        resolved='10s'
    """
```

---

## Target Table Naming

Target tables now reflect **both variables**:

```python
source_table = "usertable"  # Shared across all modes
target_table = f"usertable_{cdc_mode}_{column_family_mode}"
```

### **Examples:**

| `cdc_mode` | `column_family_mode` | Target Table Name |
|------------|---------------------|-------------------|
| `append-only` | `single_cf` | `usertable_append-only_single_cf` |
| `append-only` | `multi_cf` | `usertable_append-only_multi_cf` |
| `update-delete` | `single_cf` | `usertable_update-delete_single_cf` |
| `update-delete` | `multi_cf` | `usertable_update-delete_multi_cf` |

This allows you to test all 4 combinations side-by-side in the same Databricks workspace!

---

## Testing All 4 Combinations

### **Test 1: Append-Only + Single Family** (Most Common)

```python
# Cell 1
cdc_mode = "append-only"
column_family_mode = "single_cf"

# Cell 6: Creates table with 1 column family
# Cell 8: Changefeed without split_column_families
# Cell 11: Calls ingest_cdc_append_only_single_family()
# Result: All events stored, no fragment merging
```

**Use Case:** Audit logs for standard tables

---

### **Test 2: Append-Only + Multi Family** (NEW!)

```python
# Cell 1
cdc_mode = "append-only"
column_family_mode = "multi_cf"

# Cell 6: Creates table with 3 column families
# Cell 8: Changefeed with split_column_families=true
# Cell 11: Calls ingest_cdc_append_only_multi_family()
# Result: All events stored, fragments merged
```

**Use Case:** Audit logs for wide tables with column families

---

### **Test 3: Update-Delete + Single Family** (Production Default)

```python
# Cell 1
cdc_mode = "update-delete"
column_family_mode = "single_cf"

# Cell 6: Creates table with 1 column family
# Cell 8: Changefeed without split_column_families
# Cell 11: Calls ingest_cdc_with_merge_single_family()
# Result: Current state replicated, no fragment merging
```

**Use Case:** Production CDC for standard tables

---

### **Test 4: Update-Delete + Multi Family** (Wide Table Replication)

```python
# Cell 1
cdc_mode = "update-delete"
column_family_mode = "multi_cf"

# Cell 6: Creates table with 3 column families
# Cell 8: Changefeed with split_column_families=true
# Cell 11: Calls ingest_cdc_with_merge_multi_family()
# Result: Current state replicated, fragments merged
```

**Use Case:** Production CDC for wide tables with column families

---

## Decision Tree

```
┌─────────────────────────────────┐
│ Do you need full history?      │
└────────┬────────────────────────┘
         │
    ┌────┴────┐
   YES       NO
    │         │
    │         │
cdc_mode=     cdc_mode=
append-only   update-delete
    │         │
    └────┬────┘
         │
         ▼
┌─────────────────────────────────┐
│ Does table have column families?│
│ (or is it a wide table >50 cols)│
└────────┬────────────────────────┘
         │
    ┌────┴────┐
   YES       NO
    │         │
    │         │
column_       column_
family_mode=  family_mode=
multi_cf      single_cf
```

---

## Migration Guide

### **From Old Single-Variable Design**

**Old Code:**
```python
cdc_mode = "column-family"  # Mixed concerns
```

**New Code:**
```python
cdc_mode = "update-delete"      # CDC processing
column_family_mode = "multi_cf"  # Table structure
```

### **Mapping:**

| Old `cdc_mode` | New `cdc_mode` | New `column_family_mode` |
|----------------|----------------|--------------------------|
| `append-only` | `append-only` | `single_cf` |
| `update-delete` | `update-delete` | `single_cf` |
| `column-family` | `update-delete` | `multi_cf` |

---

## Benefits Summary

### **1. Orthogonal Concerns**
- CDC processing ⊥ Table structure
- Each variable controls one aspect
- Clear responsibilities

### **2. Complete Coverage**
- All 4 combinations available
- Previously impossible: append-only + multi_cf
- Flexible for any use case

### **3. Better Naming**
- `cdc_mode`: Clearly about CDC processing
- `column_family_mode`: Clearly about table structure
- No ambiguity

### **4. Easier Testing**
- Test each dimension independently
- Side-by-side comparison (4 target tables)
- Clear function names match variables

### **5. Scalable Design**
- Easy to add more CDC modes (e.g., "snapshot-only")
- Easy to add more column family patterns
- 2D matrix scales well

---

## Summary

| Aspect | Old Design (1 variable) | New Design (2 variables) |
|--------|------------------------|-------------------------|
| **Variables** | `cdc_mode` (3 values) | `cdc_mode` + `column_family_mode` |
| **Combinations** | 3 (hardcoded) | 4 (2 × 2 matrix) |
| **Append-only + Multi CF** | ❌ Not possible | ✅ Available |
| **Naming** | Ambiguous ("column-family") | Clear (separate concerns) |
| **Flexibility** | Limited | High |
| **Maintainability** | Mixed concerns | Separated concerns |

**Bottom Line:** The two-variable design is more flexible, clearer, and follows the Single Responsibility Principle! 🎉
