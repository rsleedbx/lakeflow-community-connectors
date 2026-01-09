# CockroachDB CDC to Azure - Quick Reference

## ✅ What Actually Works

| Format | Table Type | split_column_families | Snapshot | CDC | Status |
|--------|-----------|----------------------|----------|-----|---------|
| JSON | Any | Required for multi-family | ✅ | ✅ | **WORKS** |
| JSON | Single family | Optional | ✅ | ✅ | **WORKS** |
| Parquet | Any | Required for multi-family | ✅ | ✅ | **WORKS** |
| Parquet | Single family | Optional | ✅ | ✅ | **WORKS** |

## ⏱️ Critical Timing

- **Snapshot files**: Appear within 30 seconds
- **CDC files**: Appear after 60+ seconds
- **⚠️  MUST WAIT 60 SECONDS** after workload!

## 🎯 Recommended Configurations

### For Testing (Fastest Feedback)

```sql
CREATE CHANGEFEED FOR TABLE simple_test
INTO 'azure://changefeed-events/test?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH 
  updated,
  resolved = '10s',
  format = 'json',
  envelope = 'wrapped';
```

**Then**: Wait 60s, check files, profit!

### For Production (Best Performance)

```sql
CREATE CHANGEFEED FOR TABLE your_table
INTO 'azure://changefeed-events/prod?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH 
  updated,
  resolved = '10s',
  split_column_families,  -- Add if multi-family table
  format = 'parquet',
  compression = 'gzip';
```

## 📊 Test Results Summary

**Test Matrix**: 8 combinations tested  
**Success Rate**: 100% (6/6 valid tests)  
**Key Finding**: Both JSON and Parquet produce CDC files within 60 seconds!

### Successful Combinations

1. ✅ JSON + YCSB (multi-family) + split ➔ 33 snapshot + 1 CDC
2. ✅ JSON + simple_test + split ➔ 2 snapshot + 1 CDC  
3. ✅ JSON + simple_test + no split ➔ 2 snapshot + 1 CDC
4. ✅ Parquet + YCSB (multi-family) + split ➔ 22 snapshot + 1 CDC
5. ✅ Parquet + simple_test + split ➔ 2 snapshot + 1 CDC
6. ✅ Parquet + simple_test + no split ➔ 2 snapshot + 1 CDC

### Skipped (CockroachDB Requirement)

- ⚠️  YCSB without `split_column_families` ➔ Error (required)

## 🚀 Quick Start

```bash
# 1. Run test
cd sources/cockroachdb/scripts
./test_azure_cdc.sh json  # or parquet

# 2. Script automatically:
#    - Creates changefeed
#    - Runs workload (500 ops)
#    - Waits 60s
#    - Checks for files
#    - Analyzes statistics

# 3. View results
# You'll see both snapshot AND CDC files!
```

## 🔍 Verify CDC Files

```bash
# Check files
az storage blob list \
  --account-name <account> \
  --account-key <key> \
  --container-name changefeed-events \
  --prefix <path> \
  --output table

# Analyze statistics
./analyze_changefeed_stats.py
```

## 📝 Key Takeaways

1. **⏱️  WAIT 60 SECONDS** - This is the #1 reason tests "failed"
2. **✅ Parquet works for CDC** - Previous assumption was wrong
3. **✅ JSON works for CDC** - Also within 60s
4. **🔀 split_column_families** - Required for multi-family tables, works perfectly
5. **📊 500 operations** - Enough to trigger CDC file flush

## ❌ Common Mistakes

| Mistake | Fix |
|---------|-----|
| Wait only 20-30s | ⏱️  Wait 60+ seconds |
| Assume Parquet = snapshot only | ✅ Parquet works for CDC |
| Omit split_column_families for YCSB | 🔀 Add it (required) |
| Run tiny workload (<100 ops) | 📊 Run 500+ operations |
| Check wrong path | 🎯 Check format-specific path |

## 📚 Full Documentation

### Master Documents (Current)
- **📊 CDC_TEST_MATRIX_RESULTS.md** - **MASTER** comprehensive test results and format details
- **🔧 PARQUET_FORMAT_FIX.md** - CockroachDB native Parquet format specification
- **📖 TEST_AZURE_CDC_USAGE.md** - test_azure_cdc.sh usage guide
- **⚡ CDC_QUICK_REFERENCE.md** - This document (quick reference)

### Test Scripts
- **test_azure_cdc.sh** - Main CDC test script (JSON/Parquet)
- **test_cdc_matrix.sh** - Comprehensive test matrix
- **analyze_changefeed_stats.py** - Statistics analyzer

### Key Findings
✅ Both JSON and Parquet work for CDC  
⏱️ 60 second wait time required  
🔀 split_column_families required for multi-family tables  
⚠️ Parquet uses 'c' for both snapshots AND updates

---

**Last Updated**: December 23, 2025  
**Test Status**: ✅ All configurations validated  
**Success Rate**: 100%



