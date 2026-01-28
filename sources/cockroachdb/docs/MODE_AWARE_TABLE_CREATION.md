# Mode-Aware Table Creation

## Overview

Cells 6 and 8 are now **mode-aware**: they automatically create the appropriate table structure and changefeed configuration based on the `cdc_mode` selected in Cell 1.

---

## What Changed

### **Cell 6: Create Test Table**

#### **Before (Static)**
```python
# Always created single column family
CREATE TABLE IF NOT EXISTS usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    ...
)
```

#### **After (Mode-Aware)**
```python
if cdc_mode == "column-family":
    # Create table with MULTIPLE column families
    CREATE TABLE IF NOT EXISTS usertable (
        ycsb_key INT PRIMARY KEY,
        field0 TEXT, field1 TEXT, field2 TEXT,
        FAMILY frequently_read (ycsb_key, field0, field1, field2),
        
        field3 TEXT, field4 TEXT, field5 TEXT,
        FAMILY medium_read (field3, field4, field5),
        
        field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT,
        FAMILY rarely_read (field6, field7, field8, field9)
    )
else:
    # Create table with SINGLE column family (default)
    CREATE TABLE IF NOT EXISTS usertable (
        ycsb_key INT PRIMARY KEY,
        field0 TEXT,
        field1 TEXT,
        ...
    )
```

**Output:**
```
✅ Table 'usertable' created (or already exists)
   Mode: column-family
   Column families: 3 column families (frequently_read, medium_read, rarely_read)
```

---

### **Cell 8: Create Changefeed**

#### **Before (Static)**
```python
# Always used single-family settings
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://...'
WITH 
    format='parquet',
    updated,
    resolved='10s'
```

#### **After (Mode-Aware)**
```python
if cdc_mode == "column-family":
    # Add split_column_families for multi-family mode
    changefeed_options = """
    format='parquet',
    updated,
    resolved='10s',
    split_column_families=true
"""
else:
    # Standard options for single-family mode
    changefeed_options = """
    format='parquet',
    updated,
    resolved='10s'
"""

CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://...'
WITH {changefeed_options}
```

**Output (Column-Family Mode):**
```
✅ Changefeed created
   Job ID: 987654321
   Source: defaultdb.public.usertable
   Target path: .../usertable/usertable_column_family/
   Format: Parquet
   Split column families: TRUE (fragments will be generated)
   Destination: Azure Blob Storage
```

**Output (Single-Family Mode):**
```
✅ Changefeed created
   Job ID: 123456789
   Source: defaultdb.public.usertable
   Target path: .../usertable/usertable_update_delete/
   Format: Parquet
   Split column families: FALSE (single file per event)
   Destination: Azure Blob Storage
```

---

## Column Family Design

### **Family 1: frequently_read**
- **Columns**: `ycsb_key` (PK), `field0`, `field1`, `field2`
- **Use Case**: Hot data, accessed on every query
- **Example**: User ID, name, email

### **Family 2: medium_read**
- **Columns**: `field3`, `field4`, `field5`
- **Use Case**: Warm data, accessed occasionally
- **Example**: Phone number, address, birthdate

### **Family 3: rarely_read**
- **Columns**: `field6`, `field7`, `field8`, `field9`
- **Use Case**: Cold data, rarely accessed
- **Example**: Profile metadata, large TEXT/JSONB columns

---

## How Split Column Families Work

### **Single Family Mode** (`split_column_families=false`)

**One UPDATE to row 123:**
```
CockroachDB → 1 Parquet file
```

**File: `usertable-123456.parquet`**
```
ycsb_key | field0 | field1 | field2 | field3 | ... | field9
---------|--------|--------|--------|--------|-----|-------
123      | v0     | v1     | v2     | v3     | ... | v9
```

---

### **Multi Family Mode** (`split_column_families=true`)

**One UPDATE to row 123:**
```
CockroachDB → 3 Parquet files (one per column family)
```

**File 1: `usertable-123456-family1.parquet`** (frequently_read)
```
ycsb_key | field0 | field1 | field2 | field3 | ... | field9
---------|--------|--------|--------|--------|-----|-------
123      | v0     | v1     | v2     | NULL   | ... | NULL
```

**File 2: `usertable-123457-family2.parquet`** (medium_read)
```
ycsb_key | field0 | field1 | field2 | field3 | ... | field9
---------|--------|--------|--------|--------|-----|-------
123      | NULL   | NULL   | NULL   | v3     | ... | NULL
```

**File 3: `usertable-123458-family3.parquet`** (rarely_read)
```
ycsb_key | field0 | field1 | field2 | field3 | ... | field9
---------|--------|--------|--------|--------|-----|-------
123      | NULL   | NULL   | NULL   | NULL   | ... | v9
```

**After Merging (by Cell 11):**
```
ycsb_key | field0 | field1 | field2 | field3 | ... | field9
---------|--------|--------|--------|--------|-----|-------
123      | v0     | v1     | v2     | v3     | ... | v9  ← Complete row!
```

---

## Testing Different Modes

### **Test 1: Append-Only (No Column Families)**

```python
# Cell 1
cdc_mode = "append-only"

# Cell 6 creates: Single column family table
# Cell 8 creates: Changefeed without split_column_families
# Cell 11 uses: ingest_cdc_append_only_single_family()
```

**Expected Output:**
```
✅ Table 'usertable' created
   Mode: append-only
   Column families: 1 column family (default primary)

✅ Changefeed created
   Split column families: FALSE (single file per event)
```

---

### **Test 2: Update-Delete (No Column Families)**

```python
# Cell 1
cdc_mode = "update-delete"

# Cell 6 creates: Single column family table
# Cell 8 creates: Changefeed without split_column_families
# Cell 11 uses: ingest_cdc_with_merge_single_family()
```

**Expected Output:**
```
✅ Table 'usertable' created
   Mode: update-delete
   Column families: 1 column family (default primary)

✅ Changefeed created
   Split column families: FALSE (single file per event)
```

---

### **Test 3: Column-Family Mode**

```python
# Cell 1
cdc_mode = "column-family"

# Cell 6 creates: 3 column family table
# Cell 8 creates: Changefeed WITH split_column_families=true
# Cell 11 uses: ingest_cdc_with_merge_multi_family()
```

**Expected Output:**
```
✅ Table 'usertable' created
   Mode: column-family
   Column families: 3 column families (frequently_read, medium_read, rarely_read)

✅ Changefeed created
   Split column families: TRUE (fragments will be generated)

🔧 Merging column family fragments...
   Grouping by: ['ycsb_key'] + _cdc_timestamp + _cdc_operation
✅ Column family merge configured
```

---

## Benefits

### **1. Automatic Configuration**
- No manual SQL editing required
- Just change `cdc_mode` in Cell 1
- Table and changefeed auto-configure

### **2. Testing Flexibility**
- Test single-family CDC: Use "append-only" or "update-delete"
- Test multi-family CDC: Use "column-family"
- Switch modes without recreating infrastructure

### **3. Production Readiness**
- Same notebook works for both scenarios
- Mode selection documents intent
- Clear output shows configuration

### **4. Educational Value**
- See how column families affect CDC
- Compare fragment count (1 vs 3 files per event)
- Understand merge logic necessity

---

## Verification

### **Check Table Structure**

```sql
-- In CockroachDB
SHOW CREATE TABLE usertable;
```

**Single Family Output:**
```sql
CREATE TABLE usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    ...
)
```

**Multi Family Output:**
```sql
CREATE TABLE usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT, field1 TEXT, field2 TEXT,
    FAMILY frequently_read (ycsb_key, field0, field1, field2),
    field3 TEXT, field4 TEXT, field5 TEXT,
    FAMILY medium_read (field3, field4, field5),
    ...
)
```

### **Check Changefeed Configuration**

```sql
-- In CockroachDB
SELECT job_id, description 
FROM [SHOW CHANGEFEED JOBS] 
WHERE description LIKE '%usertable%'
ORDER BY created DESC 
LIMIT 1;
```

**Look for:**
- Single family: No mention of `split_column_families`
- Multi family: `split_column_families = true` in description

### **Check File Count in Azure (Cell 10)**

**Single Family Mode:**
```
📊 Azure File Summary
   Data files: 124 files  ← One file per CDC event
   Resolved files: 12 files
```

**Multi Family Mode:**
```
📊 Azure File Summary
   Data files: 372 files  ← Three files per CDC event (124 × 3)
   Resolved files: 12 files
```

---

## Troubleshooting

### Issue: "Expected 3 files per event, seeing 1"

**Cause:** Table was created in single-family mode earlier

**Solution:**
```python
# Drop and recreate table
# Cell 17 (Cleanup Section)
spark.sql(f"DROP TABLE IF EXISTS {source_table}")

# Then re-run:
# Cell 6 (Create Table) - will use column families now
# Cell 8 (Create Changefeed) - will use split_column_families=true
```

---

### Issue: "Merge logic not triggered"

**Cause:** Changefeed created without `split_column_families=true`

**Solution:**
```python
# Cancel old changefeed
# Cell 16 (Cleanup: Cancel Changefeed)

# Clear Azure data
# Cell 18 (Clear Azure Changefeed Data)

# Re-run Cell 8 with cdc_mode="column-family"
```

---

## Summary

| Cell | What It Does | Mode-Aware Behavior |
|------|--------------|---------------------|
| **Cell 6** | Create table | Single family (default) or 3 families |
| **Cell 8** | Create changefeed | `split_column_families=false` or `true` |
| **Cell 11** | Ingest CDC | Selects function based on mode |
| **Cell 12** | Query results | Shows merged or fragmented data |

**Key Takeaway:** Just set `cdc_mode` in Cell 1, and the entire notebook adapts automatically! 🎉
