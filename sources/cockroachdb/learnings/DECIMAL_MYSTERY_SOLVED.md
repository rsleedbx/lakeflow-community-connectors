# 🎯 DECIMAL Mystery SOLVED!

**Date:** 2026-01-27  
**Status:** ✅ ROOT CAUSE CONFIRMED

---

## 🔍 The Mystery

**Question:** Why do test scenarios work without explicit schema, but the blog post notebook fails with `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`?

---

## ✅ The Answer

### Root Cause:
The DECIMAL issue is **NOT in the data files** - it's in the **`.RESOLVED` files**!

### File Breakdown:
| File Type | Contains | DECIMAL Issue? |
|-----------|----------|----------------|
| `usertable-*.parquet` | Actual CDC event data | ❌ NO - uses `StringType` |
| `*.RESOLVED` | CDC watermark timestamps | ✅ YES - uses `DECIMAL(2147483647, 0)` |

### Evidence (Cell 8 Test Results):
```
📁 2026-01-26:
   - 1 data file (usertable-*.parquet)
   - 138 .RESOLVED files

📁 2026-01-27:
   - 0 data files
   - 94 .RESOLVED files

🧪 Test Results:
   ✅ usertable-*.parquet → Reads successfully (StringType)
   ❌ *.RESOLVED → FAILS with DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION
```

---

## 💡 Why Test Scenarios Work

Test scenarios using `cockroachdb.py` **filter by file pattern**:
- Pattern: `*usertable*.parquet`
- Result: **`.RESOLVED` files are excluded**
- Outcome: ✅ No DECIMAL error!

Blog post notebook reads **entire directory**:
- Reads: ALL files in the directory
- Result: **`.RESOLVED` files are included**
- Outcome: ❌ DECIMAL error!

---

## 🎯 The Solutions (Best to Worst)

### ✅ Option 1: Filter Out .RESOLVED Files (BEST!)

**Cell 9** in the notebook demonstrates this approach.

```python
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("pathGlobFilter", "*usertable*.parquet")  # ← Excludes .RESOLVED files!
    .load(source_path)
)
```

**Why this is best:**
- ✅ No explicit schema needed
- ✅ Simpler code
- ✅ Works exactly like test scenarios
- ✅ Schema inference from data files only

---

### ✅ Option 2: Use Explicit Schema (Current Workaround)

**Cell 6** in the notebook demonstrates this approach.

```python
explicit_schema = StructType([
    # ... all fields ...
    StructField("__crdb__updated", StringType(), False),
])

raw_df = (spark.readStream
    .format("cloudFiles")
    .schema(explicit_schema)  # ← Bypasses metadata validation
    .load(source_path)
)
```

**When to use:**
- You need to process `.RESOLVED` files
- You want explicit schema control

---

### ✅ Option 3: Use JSON Format

Change the CockroachDB changefeed:

```sql
CREATE CHANGEFEED FOR TABLE usertable
  INTO 'azure-blob://...'
  WITH format=json, envelope=wrapped;
```

**Trade-offs:**
- ✅ No DECIMAL issues
- ❌ Larger files, slower reads

---

## 🔬 Technical Details

### What are .RESOLVED files?

`.RESOLVED` files are CockroachDB CDC watermark tracking files:
- Format: Parquet (despite non-.parquet extension)
- Purpose: Track changefeed progress timestamps
- Schema: Contains `resolved` column with nanosecond precision
- Encoding: `DECIMAL(2147483647, 0)` - exceeds Spark's max precision (38)

### Why does Spark read them?

- Spark's `recursiveFileLookup` scans all files
- Spark detects Parquet format regardless of extension
- During schema validation, it hits the DECIMAL precision limit
- Error occurs before any filtering can be applied

### Why doesn't PyArrow have this issue?

- **PyArrow**: Reads logical type → sees `string` ✅
- **Spark**: Validates physical metadata → sees `DECIMAL(2147483647, 0)` ❌
- Different Parquet reader implementations with different validation rules

---

## 📚 Files Updated

1. **`stream-changefeed-to-databricks-azure.ipynb`**
   - Cell 8: Root cause investigation (data files vs .RESOLVED files)
   - Cell 9: Best solution (file filtering)

2. **`verify_decimal_issue.md`**
   - Updated root cause section
   - Hypothesis G marked as DISPROVEN
   - Added file filtering as Option 1 (best solution)

---

## 🎓 Key Learnings

1. **Problem was file selection, not file encoding**
   - Data files are fine
   - .RESOLVED files have the issue

2. **Test scenarios worked by accident**
   - File pattern filtering excluded problematic files
   - Wasn't intentional DECIMAL mitigation

3. **Simpler solution exists**
   - Don't need explicit schema
   - Just filter files properly

4. **File extension doesn't matter to Spark**
   - .RESOLVED files are read as Parquet
   - Spark detects format by content, not extension

---

## ✅ Recommended Next Steps

1. **Update Cell 6** to use file filtering instead of explicit schema (simpler!)
2. **Run Cell 9** to verify file filtering works in your environment
3. **Update production code** to use `pathGlobFilter`
4. **Document** this for other CockroachDB + Databricks users

---

**Mystery Status:** 🎉 **SOLVED!**
