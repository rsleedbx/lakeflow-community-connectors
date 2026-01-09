# CockroachDB Learnings Documentation

## 📚 Master CDC Testing Documents

### Core CDC References (Current & Authoritative)

1. **📊 CDC_TEST_MATRIX_RESULTS.md** - **MASTER DOCUMENT**
   - Comprehensive CDC test results (Dec 22, 2025)
   - Tests all format/table/split combinations
   - 100% success rate (6/6 valid tests)
   - Includes Parquet vs JSON format comparison
   - **Status**: ✅ Current - Use this as primary reference

2. **⚡ CDC_QUICK_REFERENCE.md**
   - Quick summary of what works
   - Recommended configurations
   - Common mistakes and fixes
   - **Status**: ✅ Current - Quick lookup

3. **🔧 PARQUET_FORMAT_FIX.md**
   - CockroachDB native Parquet format specification
   - Explains `__crdb__event_type` column behavior
   - Why event counts = rows × column families
   - **Status**: ✅ Current - Format reference

4. **🎯 PARQUET_UPDATE_DETECTION.md**
   - Enhanced UPDATE detection for Parquet format
   - Timestamp-based logic to distinguish SNAPSHOT from UPDATE
   - Implementation details and edge cases
   - Comparison with JSON format's explicit detection
   - **Status**: ✅ Current - Technical enhancement (Dec 23, 2025)

5. **📖 TEST_AZURE_CDC_USAGE.md**
   - test_azure_cdc.sh usage guide
   - Script examples and output
   - Troubleshooting tips
   - **Status**: ✅ Current - Script documentation

---

## 🗑️ Recently Cleaned Up (Dec 23, 2025)

The following outdated/redundant documents were removed to reduce confusion:

### Deleted Documents
1. ❌ `TEST_REPORT_CDC.md` - Outdated (Dec 2024, 1 year old)
2. ❌ `WORKLOAD_TESTING_SUMMARY.md` - About workload types, not CDC behavior
3. ❌ `FAST_TESTING_UPGRADE.md` - Info integrated into current docs
4. ❌ `CDC_TESTING_GUIDE.md` - Superseded by CDC_TEST_MATRIX_RESULTS.md
5. ❌ `TEST_RESULTS.md` - About vendor removal, unrelated to CDC

### Why Cleaned Up
- **Confusion**: Too many test documents with conflicting information
- **Outdated**: Some docs from 2024 with incorrect assumptions
- **Redundant**: Information duplicated across multiple files
- **Clarity**: New users need single source of truth

---

## ✅ Key CDC Findings (From Master Document)

### What Works
- ✅ JSON format: Snapshot + CDC
- ✅ Parquet format: Snapshot + CDC
- ✅ split_column_families: Required for multi-family tables
- ✅ Both formats flush to Azure within 60 seconds

### Critical Requirements
- ⏱️ **60+ seconds** wait time for CDC files
- 📊 **500+ operations** for reliable flush
- 🔀 **split_column_families** for multi-family tables (CockroachDB requirement)

### Format Comparison

| Feature | Parquet | JSON |
|---------|---------|------|
| **File size** | Smaller (compressed) | Larger (nested) |
| **Update detection** | ⚠️ 'c' for both snapshot & update | ✅ before/after distinction |
| **Performance** | ✅ Better for analytics | ⚠️ Slower queries |
| **Debugging** | ⚠️ Requires tools | ✅ Human-readable |
| **Production** | ✅ Recommended | Use for audit trails |

### Important Parquet Behavior
⚠️ **Parquet uses `__crdb__event_type='c'` for BOTH snapshots AND updates**, making them indistinguishable by event type alone.

**Workaround**: Use timestamps or JSON format for explicit update detection.

---

## 🔍 How to Use This Documentation

### For New Users
1. Start with **CDC_QUICK_REFERENCE.md** for overview
2. Read **CDC_TEST_MATRIX_RESULTS.md** for comprehensive details
3. Use **TEST_AZURE_CDC_USAGE.md** to run your own tests

### For Troubleshooting
1. Check **CDC_QUICK_REFERENCE.md** → "Common Mistakes" section
2. Verify your setup matches tested configurations in **CDC_TEST_MATRIX_RESULTS.md**
3. Review **PARQUET_FORMAT_FIX.md** if working with Parquet format

### For Production Planning
1. Review format comparison in **CDC_TEST_MATRIX_RESULTS.md** → "Appendix"
2. Check performance metrics and timing requirements
3. Choose format based on your use case (analytics vs. audit trail)

---

## 📝 Documentation Status

**Last Cleanup**: December 23, 2025  
**Master Document**: CDC_TEST_MATRIX_RESULTS.md (Dec 22, 2025)  
**Test Coverage**: 8 combinations, 100% success rate (6/6 valid)  
**Status**: ✅ **Current and authoritative**

---

## 🎯 Single Source of Truth

**For all CDC behavior and testing questions, refer to:**
- **CDC_TEST_MATRIX_RESULTS.md** (Master)

All other CDC test documents have been archived or deleted to prevent confusion.
