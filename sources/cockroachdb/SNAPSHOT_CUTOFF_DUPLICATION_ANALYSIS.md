# Snapshot Cutoff Detection - Code Duplication Analysis

**Date:** January 8, 2026  
**Issue:** Duplicated snapshot cutoff logic led to bug in Volume path  
**Status:** ⚠️ CRITICAL - Needs refactoring

---

## 🐛 **The Bug**

**What happened:**
- Parquet (Azure): ✅ Correctly filtered for `sequence == "00000000"`
- JSON (Azure): ✅ Correctly filtered for `-00000000-` in filename
- JSON (Volume): ❌ Used `[:3]` without filtering → **BUG!**

**Root cause:** Same logic implemented 4 different times with slight variations → inconsistency led to bugs

---

## 📊 **Duplication Analysis**

### Locations Where Snapshot Cutoff is Detected

| Location | Lines | Format | Source | Implementation |
|----------|-------|--------|--------|----------------|
| **1. Azure Blob - Parquet** | 3223-3280 | Parquet | Azure | Filters by `sequence == "00000000"` ✅ |
| **2. Azure Blob - JSON** | 3446-3486 | JSON | Azure | Filters by `-00000000-` in name ✅ |
| **3. Volume - JSON** | 3765-3808 | JSON | Volume | `[:3]` without filtering ❌ **BUG** |
| **4. Streaming - JSON** | 5605-5648 | JSON | Volume | Same as #3 ❌ **BUG** |

### Code Comparison

#### **Azure Parquet (Correct)**
```python
# Lines 3223-3240
for blob_name in data_blobs:
    filename = blob_name.split('/')[-1]
    parts = filename.split('-')
    
    if len(parts) >= 5:
        sequence = parts[4]  # Parse sequence from filename
        if sequence == "00000000":  # ✅ Explicit filter!
            snapshot_files.append(blob_name)
        else:
            cdc_files.append(blob_name)
```

#### **Azure JSON (Correct)**
```python
# Lines 3456-3460
for blob_name in data_blobs:
    # Check if it's a snapshot file (sequence 00000000)
    if '-00000000-' in blob_name:  # ✅ Explicit filter!
        snapshot_file_count += 1
        # ... process
```

#### **Volume JSON (Had Bug - Now Fixed)**
```python
# Lines 3770-3779 (BEFORE fix)
snapshot_files = sorted(file_list, key=lambda x: x['name'])[:3]  # ❌ No filtering!

# Lines 3770-3779 (AFTER fix)
snapshot_files = [f for f in file_list 
                 if f['name'].endswith(('.ndjson', '.json'))
                 and '-00000000-' in f['name']]  # ✅ Now filters!
```

#### **Streaming JSON (Had Bug - Now Fixed)**
```python
# Lines 5613-5625 (BEFORE fix)
json_files = sorted([f for f in all_files if f.name.endswith(('.ndjson', '.json'))])[:3]  # ❌ No filtering!

# Lines 5617-5625 (AFTER fix)
snapshot_files = [f for f in all_files 
                 if f.name.endswith(('.ndjson', '.json')) 
                 and '-00000000-' in f.name]  # ✅ Now filters!
```

---

## 🔢 **Duplication Metrics**

| Metric | Count | Notes |
|--------|-------|-------|
| **Total implementations** | 4 | Azure Parquet, Azure JSON, Volume JSON, Streaming JSON |
| **Lines duplicated** | ~160 | Approximately 40 lines × 4 implementations |
| **Bugs introduced** | 2 | Volume JSON & Streaming JSON (same bug, 2 places) |
| **Shared code** | 0% | **ZERO code reuse!** |

---

## ❌ **Why This Violates DRY Principle**

### The Same Business Logic, Implemented 4 Times:

1. **Filter for snapshot files** (sequence `00000000`)
2. **Read files** (Spark or pandas)
3. **Extract timestamp column** (`updated` or `__crdb__updated`)
4. **Find maximum timestamp**
5. **Return as cutoff**

### Consequences of Duplication:

1. ❌ **Bugs introduced:** Volume path forgot to filter by sequence
2. ❌ **Maintenance burden:** Fix must be applied 4 times
3. ❌ **Inconsistency:** Different implementations for same logic
4. ❌ **Testing complexity:** Must test 4 different code paths
5. ❌ **Code bloat:** 160 lines could be 40 lines with reuse

---

## ✅ **Proposed Solution: Extract Shared Function**

### New Function: `detect_snapshot_cutoff()`

```python
def detect_snapshot_cutoff(
    files: List[Dict[str, str]],  # List of {name, path} dicts
    format_type: str,              # 'json' or 'parquet'
    source_type: str,              # 'azure_blob' or 'volume'
    spark = None,
    dbutils = None,
    debug: bool = False
) -> Optional[str]:
    """
    Detect snapshot cutoff timestamp from changefeed files.
    
    The snapshot cutoff is the maximum timestamp from files with sequence 00000000.
    This is used to distinguish SNAPSHOT events from INSERT/UPDATE events.
    
    Args:
        files: List of file metadata dicts with 'name' and 'path' keys
        format_type: 'json' or 'parquet'
        source_type: 'azure_blob' or 'volume' (affects how files are read)
        spark: SparkSession (required)
        dbutils: DBUtils (required for volume source)
        debug: Print debug output
        
    Returns:
        Maximum timestamp as string, or None if no snapshot files found
        
    Example:
        ```python
        files = [
            {'name': '...-00000000-table.parquet', 'path': '/path/to/file'},
            {'name': '...-00000001-table.parquet', 'path': '/path/to/file'}
        ]
        cutoff = detect_snapshot_cutoff(files, 'parquet', 'azure_blob', spark)
        # Returns: "1767823574768222906.0000000000"
        ```
    """
    # Step 1: Filter for snapshot files (sequence 00000000)
    snapshot_files = [f for f in files if '-00000000-' in f['name']]
    
    if not snapshot_files:
        if debug:
            print(f"   ⚠️  No snapshot files found (no -00000000- in filenames)")
        # Fallback: use first file only (for legacy data without sequence numbers)
        snapshot_files = [files[0]] if files else []
    
    if not snapshot_files:
        return None
    
    if debug:
        print(f"   📁 Found {len(snapshot_files)} snapshot file(s)")
        print(f"   📄 Sample: {snapshot_files[0]['name']}")
    
    # Step 2: Determine timestamp column name by format
    timestamp_col = 'updated' if format_type == 'json' else '__crdb__updated'
    
    # Step 3: Read files and extract max timestamp
    max_timestamp = None
    
    for file_info in snapshot_files:
        try:
            file_path = file_info['path']
            
            # Read file based on format
            if format_type == 'json':
                if source_type == 'azure_blob':
                    # Azure blob: download and parse JSON lines
                    from azure.storage.blob import BlobServiceClient
                    # ... (existing Azure blob reading logic)
                    pass
                else:
                    # Volume: use Spark
                    df = spark.read.json(file_path)
            else:  # parquet
                if source_type == 'azure_blob':
                    # Azure blob: download and read with pandas
                    import pandas as pd
                    from io import BytesIO
                    # ... (existing Azure blob reading logic)
                    pass
                else:
                    # Volume: use Spark
                    df = spark.read.parquet(file_path)
            
            # Extract max timestamp
            if source_type == 'azure_blob':
                # pandas DataFrame
                if timestamp_col in df.columns:
                    file_max = str(df[timestamp_col].max())
            else:
                # Spark DataFrame
                if timestamp_col in df.columns:
                    file_max = df.agg({timestamp_col: "max"}).collect()[0][0]
                    file_max = str(file_max) if file_max else None
            
            # Track maximum across all files
            if file_max and (max_timestamp is None or file_max > max_timestamp):
                max_timestamp = file_max
                
        except Exception as e:
            if debug:
                print(f"   ⚠️  Could not read {file_info['name']}: {e}")
            continue
    
    if debug and max_timestamp:
        print(f"   ✅ Snapshot cutoff: {max_timestamp}")
    
    return max_timestamp
```

### Usage in All 4 Locations:

```python
# 1. Azure Blob - Parquet (replace lines 3223-3280)
snapshot_cutoff = detect_snapshot_cutoff(
    files=blob_files,
    format_type='parquet',
    source_type='azure_blob',
    spark=None,  # Uses pandas for Azure
    debug=debug
)

# 2. Azure Blob - JSON (replace lines 3446-3486)
snapshot_cutoff = detect_snapshot_cutoff(
    files=blob_files,
    format_type='json',
    source_type='azure_blob',
    spark=None,  # Uses line-by-line JSON parsing
    debug=debug
)

# 3. Volume - JSON (replace lines 3765-3808)
snapshot_cutoff = detect_snapshot_cutoff(
    files=file_list,
    format_type='json',
    source_type='volume',
    spark=spark,
    dbutils=dbutils,
    debug=debug
)

# 4. Streaming - JSON (replace lines 5605-5648)
snapshot_cutoff = detect_snapshot_cutoff(
    files=[{'name': f.name, 'path': f.path} for f in dbutils.fs.ls(volume_path)],
    format_type='json',
    source_type='volume',
    spark=spark,
    dbutils=dbutils,
    debug=debug
)
```

---

## 📊 **Benefits of Refactoring**

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Lines of code** | ~160 | ~80 | 50% reduction |
| **Implementations** | 4 separate | 1 shared | 75% reduction |
| **Bug surface area** | 4 places | 1 place | 75% reduction |
| **Test coverage needed** | 4 code paths | 1 code path | 75% reduction |
| **Consistency** | ❌ Varied | ✅ Guaranteed | 100% improvement |

---

## 🎯 **Implementation Plan**

### Phase 1: Extract Function (Priority 1)
1. Create `detect_snapshot_cutoff()` function
2. Add comprehensive unit tests
3. Document parameters and return values

### Phase 2: Replace Duplicated Code (Priority 1)
1. Replace Azure Blob - Parquet (lines 3223-3280)
2. Replace Azure Blob - JSON (lines 3446-3486)
3. Replace Volume - JSON (lines 3765-3808)
4. Replace Streaming - JSON (lines 5605-5648)

### Phase 3: Validation (Priority 1)
1. Run `test_cdc_matrix.sh` full suite
2. Run `test_cdc_matrix.sh --validate-only`
3. Test all 8 scenarios (4 JSON + 4 Parquet)
4. Verify operation counts match expected

---

## 🔍 **Root Cause Analysis**

### Why Did This Happen?

1. **Incremental development:** Features added at different times
2. **Copy-paste coding:** Similar code copied and modified slightly
3. **No refactoring pass:** Never consolidated after patterns emerged
4. **Lack of shared utilities:** No `utils.py` or helper module

### Similar Issues in Codebase?

Need to audit for other duplicated patterns:
- [ ] Primary key extraction (already consolidated? Check!)
- [ ] File listing logic
- [ ] Timestamp comparison logic (partially consolidated - see `JSON_TYPE_COMPARISON_FIX.md`)
- [ ] CDC operation determination

---

## 📝 **Lessons Learned**

1. **DRY from the start:** Extract shared functions immediately when patterns emerge
2. **Code review discipline:** Catch duplication in review before merge
3. **Refactoring budget:** Allocate time for consolidation passes
4. **Test-driven development:** Unit tests would have caught the Volume bug
5. **Filename parsing:** Should have centralized filename convention handling

---

## ✅ **Recommendation**

**IMMEDIATE ACTION REQUIRED:**

Extract `detect_snapshot_cutoff()` as a shared function to:
1. ✅ Prevent future bugs (consistency guaranteed)
2. ✅ Reduce code by 50% (~80 lines saved)
3. ✅ Simplify testing (1 path instead of 4)
4. ✅ Improve maintainability (1 place to fix bugs)

**Priority:** 🔴 **HIGH** - This duplication led to a production bug

---

**Status:** ⚠️ Analysis Complete, Refactoring Recommended  
**Estimated Effort:** 2-3 hours (including tests)  
**Risk:** Low (existing tests will catch regressions)

