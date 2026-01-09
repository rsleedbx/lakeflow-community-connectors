# Test Results Analysis - Critical Issues Found

**Date:** January 6, 2026  
**Test Run:** All 8 tests completed, but results show systematic errors

---

## 📊 **Test Results Summary**

| Test | Format | Split | Table Created | Snapshot | Deletes | Unique Keys | Status |
|------|--------|-------|---------------|----------|---------|-------------|--------|
| 1 | JSON | Yes | 9,594 | **9,094** ❌ | **501** ❌ | 9,995 | WRONG |
| 2 | JSON | No | 9,594 | **9,094** ❌ | **500** ❌ | 9,994 | WRONG |
| 3 | JSON | Yes | 1,000 | **500** ❌ | 100 ✅ | 1,000 | WRONG |
| 4 | JSON | No | 1,000 | **500** ❌ | 100 ✅ | 1,000 | WRONG |
| 5 | Parquet | Yes | 9,594 | **0** ❌ | **401** ❌ | 9,995 | WRONG |
| 6 | Parquet | No | 9,594 | **0** ❌ | **401** ❌ | 9,995 | WRONG |
| 7 | Parquet | Yes | 1,000 | **0** ❌ | 100 ✅ | 1,000 | WRONG |
| 8 | Parquet | No | 1,000 | **0** ❌ | 100 ✅ | 1,000 | WRONG |

---

## 🐛 **Critical Issue #1: JSON Snapshot Undercounting**

### **Observed:**
- All JSON tests show ~50% of snapshot rows missing
- usertable: 9,094 / 9,594 (500 missing)
- simple_test: 500 / 1,000 (500 missing)

### **Root Cause:**
My "skip incomplete fragments" fix (line 2733-2736 in `cockroachdb.py`) is **too aggressive**.

Even in "no_split" tests, the files show:
```bash
# Test 2 (json_usertable_no_split) - lines 214-215
usertable+fam_1_field0-4.ndjson    ← No PK!
usertable+fam_0_ycsb_key-4.ndjson  ← Has PK
```

**Why?** The base `usertable` table has multiple column families by design (YCSB benchmark). When we do `CREATE TABLE test_xxx AS SELECT * FROM usertable`, CockroachDB **preserves the column family structure**.

**Result:** Even without `split_column_families` in the changefeed, CockroachDB emits separate files per column family.

**My fix skips fam_1 events** because they don't have `ycsb_key`, so we only count half the logical rows.

---

## 🐛 **Critical Issue #2: Parquet Snapshot = 0**

### **Observed:**
- ALL Parquet tests show **0 snapshot rows**
- But unique keys are correct (9,995 or 1,000)
- Delete counts are inflated (401 instead of 100)

### **Root Cause:**
The Parquet analysis logic is **miscategorizing ALL snapshot events**.

Looking at Test 5 results:
```
Snapshot rows: 0      ❌ (all 9,594 missing)
Delete rows: 401      ❌ (should be 100)
Unique keys: 9,995    ✅ (correct after dedup)
```

**Theory:** The snapshot detection logic based on `__crdb__event_type == 'c'` or timestamp comparison is broken, causing:
1. All snapshot events to be classified as something else
2. Inflated delete counts suggest snapshot events are being misclassified as deletes

---

## 🔍 **Why This Matters**

1. **JSON Analysis is Unreliable** for tables with column families
2. **Parquet Analysis is Completely Broken** for snapshots
3. **The test matrix results cannot be trusted** for validation
4. **Notebooks using these statistics will have wrong expectations**

---

## ✅ **Fixes Required**

### **Fix 1: JSON - Better Fragment Handling** 

**Option A: Accept Undercounting** ✅ **RECOMMENDED**
- Document that JSON analysis with column families only counts PK-containing fragments
- This is technically correct (one PK fragment = one logical row)
- But the ISSUE is we're getting ~50% instead of 100%, which suggests:
  - We're looking for the wrong PK, OR
  - The table structure is different than expected

**Option B: Use Filename Pattern**
- If fragment filenames include identifiers, use those for linking
- Complex and fragile

### **Fix 2: Parquet - Fix Snapshot Detection**

**Investigation needed:**
1. Check how `_process_parquet_records` determines snapshot vs CDC
2. Verify `__crdb__event_type` values in actual files
3. Check if timestamp-based logic is working

---

## 🚨 **Immediate Action Items**

### **1. Investigate Primary Key Mismatch**

Check if the test tables actually have `ycsb_key` as PK:
```sql
SHOW CREATE TABLE test_json_usertable_with_split;
```

**Hypothesis:** Line 77 shows:
```
NOTICE: ... the new table has a hidden rowid primary key column
```

This means `CREATE TABLE AS SELECT *` created a table **WITHOUT** `ycsb_key` as PK!

**If true:** We're analyzing with the wrong primary key, which explains everything.

### **2. Fix Table Creation**

Change from:
```sql
CREATE TABLE test_xxx AS SELECT * FROM usertable LIMIT 10000;
ALTER TABLE test_xxx ADD PRIMARY KEY (ycsb_key);
```

To ensure PK is properly set.

### **3. Fix Parquet Snapshot Detection**

Debug the `_process_parquet_records` function to see why all events show as non-snapshot.

---

## 📝 **Next Steps**

1. **Verify PK Issue:** Check actual table schema
2. **Fix Table Creation:** Ensure PK is correctly set
3. **Re-run Tests:** With proper PKs, results should be correct
4. **Fix Parquet Analysis:** If PK fix doesn't resolve it, debug snapshot detection

---

## 🎯 **Expected Results After Fixes**

| Test | Snapshot | Deletes | Unique Keys |
|------|----------|---------|-------------|
| 1-2 (JSON usertable) | 9,594 ✅ | 100 ✅ | 9,494 ✅ |
| 3-4 (JSON simple_test) | 1,000 ✅ | 100 ✅ | 900 ✅ |
| 5-6 (Parquet usertable) | 9,594 ✅ | 100 ✅ | 9,494 ✅ |
| 7-8 (Parquet simple_test) | 1,000 ✅ | 100 ✅ | 900 ✅ |


