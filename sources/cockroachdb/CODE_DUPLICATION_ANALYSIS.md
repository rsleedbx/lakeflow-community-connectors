# Code Duplication Analysis: `cockroachdb.py`

**Date:** January 21, 2026  
**File:** `sources/cockroachdb/cockroachdb.py`  
**Total Lines:** 6,987  
**Analysis:** Identify duplicate code patterns and refactoring opportunities

---

## Executive Summary

**Duplication Level:** 🟡 **Moderate (20-30%)**

The codebase has **intentional, strategic duplication** for mode-specific operations (VOLUME vs AZURE), but shares common transformation logic. The duplication is **mostly acceptable** given the architectural tradeoffs, but there are **refactoring opportunities** to reduce it by 40-50%.

### Key Findings

| Category | Duplication Level | Recommendation |
|----------|------------------|----------------|
| **File Reading Methods** | 🔴 High (~70% similar) | Refactor to abstract base |
| **File Listing Methods** | 🟡 Moderate (~50% similar) | Extract common patterns |
| **Schema/Metadata Loading** | 🟡 Moderate (~60% similar) | Consolidate with strategy pattern |
| **Record Processing** | 🟢 Low (well separated) | Keep as is |
| **Analysis Functions** | 🟡 Moderate (~65% similar) | Extract common logic |

---

## 1. File Reading Methods (🔴 High Duplication)

### Duplicate Code Pattern

**Methods:**
- `_read_table_from_volume()` - 180 lines (lines 1282-1462)
- `_read_table_from_azure_parquet()` - 120 lines (lines 1938-2058)
- `_read_table_from_azure_json()` - Similar pattern (not shown)

**Similarity:** ~70% of logic is identical

### Common Flow (Duplicated)

```python
# 1. Load schema (different source, same logic)
schema_info = self._load_schema_from_XXX(...)
primary_keys = schema_info.get('primary_keys', [])

# 2. List files (different method, same logic)
file_list = self._list_XXX_files(...)
if not file_list:
    return iter([]), start_offset

# 3. Filter by cursor (IDENTICAL)
new_files = [f for f in file_list if f['name'] > last_cursor]
new_files.sort(key=lambda f: f['name'])

# 4. Process each file (IDENTICAL core logic)
all_rows = []
for file_info in new_files:
    # Read file (different I/O, same transformation)
    records = read_from_source(file_info)
    
    # Transform records (IDENTICAL)
    if is_parquet:
        transformed = self._process_parquet_records(...)
    else:
        transformed = self._process_json_records(...)
    
    all_rows.extend(transformed)

# 5. Deduplication (IDENTICAL in all methods)
if all_rows and primary_keys:
    # ... 50+ lines of deduplication logic ...

# 6. Return (IDENTICAL)
return iter(all_rows), end_offset
```

### Refactoring Opportunity

**Potential Savings:** ~200-300 lines

```python
class FileReader(ABC):
    """Abstract base for file-based CDC readers"""
    
    @abstractmethod
    def list_files(self, path, cursor) -> List[FileInfo]:
        """List files from storage (VOLUME or AZURE)"""
        pass
    
    @abstractmethod
    def read_file(self, file_info) -> List[Dict]:
        """Read and parse file (JSON or Parquet)"""
        pass
    
    @abstractmethod
    def load_schema(self, table_name) -> Dict:
        """Load schema metadata"""
        pass
    
    def read_table(self, table_name, start_offset, table_options):
        """Common reading logic (ONE implementation)"""
        # 1. Load schema
        schema_info = self.load_schema(table_name)
        primary_keys = schema_info.get('primary_keys', [])
        
        # 2. List files
        file_list = self.list_files(self.path, start_offset.get('cursor'))
        
        # 3. Filter by cursor (ONCE)
        new_files = [f for f in file_list if f['name'] > last_cursor]
        new_files.sort(key=lambda f: f['name'])
        
        # 4. Process files (ONCE)
        all_rows = []
        for file_info in new_files:
            records = self.read_file(file_info)  # Polymorphic
            transformed = self._transform_records(records, file_info)
            all_rows.extend(transformed)
        
        # 5. Deduplicate (ONCE)
        if all_rows and primary_keys:
            all_rows = self._deduplicate(all_rows, primary_keys)
        
        # 6. Return
        return iter(all_rows), end_offset

class VolumeReader(FileReader):
    """Volume-specific implementation (50 lines instead of 180)"""
    def list_files(self, path, cursor):
        return self._list_volume_files(path)
    
    def read_file(self, file_info):
        if file_info['name'].endswith('.parquet'):
            return self.spark.read.parquet(file_info['path']).toPandas().to_dict('records')
        else:
            return self.spark.read.json(file_info['path']).toPandas().to_dict('records')
    
    def load_schema(self, table_name):
        return self._load_schema_from_volume(self.volume_path)

class AzureReader(FileReader):
    """Azure-specific implementation (50 lines instead of 120)"""
    # Similar overrides for Azure...
```

**Pros:**
- ✅ 60-70% less code (500 lines → 200 lines)
- ✅ Single source of truth for deduplication logic
- ✅ Easier to test (test abstract base once)
- ✅ Easier to add new storage backends (S3, GCS)

**Cons:**
- ❌ More complex architecture
- ❌ Harder for beginners to understand
- ❌ Risk of breaking existing functionality
- ❌ Abstraction overhead

**Recommendation:** 🟡 **Consider for v3.0** (major refactor)

---

## 2. File Listing Methods (🟡 Moderate Duplication)

### Duplicate Code Pattern

**Methods:**
- `_list_volume_files()` - 86 lines (lines 1194-1279)
- `_list_azure_files()` - 40 lines (lines 2740-2778) 
- `_list_azure_parquet_files()` - 60 lines (lines 2847-2906)

**Similarity:** ~50% (recursive traversal, filtering, metadata extraction)

### Common Patterns

```python
# Pattern 1: Recursive directory traversal
def list_files(path):
    all_files = []
    
    def _list_recursive(current_path):  # DUPLICATED
        items = get_items(current_path)
        for item in items:
            if is_directory(item):
                _list_recursive(item.path)  # Recurse
            elif matches_extension(item):
                all_files.append(extract_metadata(item))
    
    _list_recursive(path)
    return all_files

# Pattern 2: Metadata extraction
def extract_metadata(item):  # DUPLICATED
    return {
        'name': item.name,
        'path': item.path,
        'size': item.size
    }

# Pattern 3: Filtering
files = [f for f in all_files if not f['name'].startswith('_')]  # DUPLICATED
```

### Refactoring Opportunity

**Potential Savings:** ~80-100 lines

```python
def _list_files_recursive(
    list_fn: Callable,           # How to list items (dbutils.fs.ls or container_client.list_blobs)
    is_dir_fn: Callable,         # How to detect directories
    extract_fn: Callable,        # How to extract metadata
    path: str,
    extensions: List[str]
) -> List[Dict[str, Any]]:
    """
    Generic recursive file listing.
    Works for both Volume and Azure by accepting strategy functions.
    """
    all_files = []
    
    def _recurse(current_path):
        items = list_fn(current_path)
        for item in items:
            # Skip metadata
            item_name = extract_fn(item, 'name')
            if item_name.startswith('_') or '/_metadata/' in extract_fn(item, 'path'):
                continue
            
            # Recurse into directories
            if is_dir_fn(item):
                _recurse(extract_fn(item, 'path'))
            # Collect files with matching extensions
            elif any(item_name.endswith(ext) for ext in extensions):
                all_files.append({
                    'name': extract_fn(item, 'name'),
                    'path': extract_fn(item, 'path'),
                    'size': extract_fn(item, 'size')
                })
    
    _recurse(path)
    return all_files

# Then:
def _list_volume_files(self, path, extensions=['.parquet', '.json', '.ndjson']):
    return _list_files_recursive(
        list_fn=lambda p: self.dbutils.fs.ls(p),
        is_dir_fn=lambda item: item.path.endswith('/'),
        extract_fn=lambda item, attr: getattr(item, attr),
        path=path,
        extensions=extensions
    )

def _list_azure_files(self, container, prefix, extensions=['.parquet', '.json']):
    return _list_files_recursive(
        list_fn=lambda p: container.list_blobs(name_starts_with=p),
        is_dir_fn=lambda item: False,  # Azure blobs don't have dirs
        extract_fn=lambda item, attr: item.name if attr == 'name' else getattr(item, attr),
        path=prefix,
        extensions=extensions
    )
```

**Recommendation:** 🟢 **Implement in v2.4** (low risk, high benefit)

---

## 3. Schema/Metadata Loading (🟡 Moderate Duplication)

### Duplicate Code Pattern

**Methods:**
- `_load_schema_from_volume()` - 60 lines (lines 3094-3153)
- `_load_schema_from_azure()` - 45 lines (lines 3049-3093)
- `_get_schema_from_files()` - 95 lines (lines 361-456)
- `_get_metadata_from_files()` - 52 lines (lines 458-510)

**Similarity:** ~60% (error handling, validation, fallbacks)

### Common Patterns

```python
# Pattern 1: Load and validate (DUPLICATED)
def load_schema(source):
    try:
        raw_data = read_from_source(source)
        schema_info = json.loads(raw_data)
        
        if not schema_info:
            raise ValueError("Schema file empty")
        
        return schema_info
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

# Pattern 2: Extract primary keys (DUPLICATED)
def get_primary_keys(schema_or_table):
    primary_keys = extract_keys(schema_or_table)
    if not primary_keys:
        raise ValueError("No primary keys found")
    return primary_keys

# Pattern 3: Metadata formatting (DUPLICATED)
return {
    "primary_keys": primary_keys,
    "cursor_field": "_cdc_updated",
    "ingestion_type": "cdc"
}
```

### Refactoring Opportunity

**Potential Savings:** ~100-120 lines

```python
class SchemaLoader:
    """Strategy pattern for schema loading"""
    
    @staticmethod
    def load_from_volume(volume_path, dbutils) -> Dict:
        """Load schema from Unity Catalog Volume"""
        schema_path = f"{volume_path}/_metadata/schema.json"
        return SchemaLoader._load_and_validate(
            lambda: dbutils.fs.head(schema_path, 1048576),
            source_name=schema_path
        )
    
    @staticmethod
    def load_from_azure(account, key, container, path) -> Dict:
        """Load schema from Azure Blob Storage"""
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient(account_url=account, credential=key)
        blob = client.get_blob_client(container=container, blob=f"{path}/_metadata/schema.json")
        return SchemaLoader._load_and_validate(
            lambda: blob.download_blob().readall().decode('utf-8'),
            source_name=f"azure://{container}/{path}"
        )
    
    @staticmethod
    def _load_and_validate(read_fn: Callable, source_name: str) -> Dict:
        """
        Common validation logic (ONCE).
        """
        try:
            raw_data = read_fn()
            
            if not raw_data or len(raw_data) == 0:
                raise ValueError(f"Schema file empty: {source_name}")
            
            if len(raw_data) >= 1048576:
                raise ValueError(f"Schema file too large (>1MB): {source_name}")
            
            schema_info = json.loads(raw_data)
            
            if not schema_info:
                raise ValueError(f"Schema file parsed as empty: {source_name}")
            
            # Validate structure
            if 'primary_keys' not in schema_info:
                raise ValueError(f"Schema missing 'primary_keys': {source_name}")
            
            return schema_info
            
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in schema file: {source_name}\nError: {e}")
        except Exception as e:
            if "FileNotFoundException" in str(type(e).__name__):
                return None
            raise RuntimeError(f"Error loading schema from {source_name}: {e}")
    
    @staticmethod
    def to_metadata(schema_info: Dict) -> Dict:
        """
        Convert schema to metadata format (ONCE).
        """
        primary_keys = schema_info.get('primary_keys', [])
        if not primary_keys:
            raise ValueError("No primary keys found in schema")
        
        return {
            "primary_keys": primary_keys,
            "cursor_field": "_cdc_updated",
            "ingestion_type": "cdc"
        }
```

**Recommendation:** 🟢 **Implement in v2.4** (improves error handling consistency)

---

## 4. Record Processing (🟢 Low Duplication)

### Well-Separated Methods

**Methods:**
- `_process_parquet_records()` - 94 lines (lines 1748-1841)
- `_process_json_records()` - 96 lines (lines 1842-1937)

**Similarity:** ~20% (only shared transformation logic, which is properly extracted)

### Analysis

These methods have **minimal duplication** because:
- ✅ Format-specific logic is properly separated
- ✅ Common transformations are extracted (`_determine_cdc_operation()`, `_add_cdc_metadata_to_dataframe()`)
- ✅ Each handles its format's quirks (Parquet `__crdb__event_type` vs JSON `before/after`)

**Recommendation:** 🟢 **No changes needed** (well-architected)

---

## 5. Analysis Functions (🟡 Moderate Duplication)

### Duplicate Code Pattern

**Functions:**
- `analyze_volume_changefeed_files()` - 274 lines (lines 4171-4444)
- `analyze_azure_changefeed_files()` - 561 lines (lines 3610-4170)

**Similarity:** ~65% of logic is identical

### Common Patterns

```python
# Pattern 1: File discovery (DUPLICATED)
file_list = list_files(path)
print(f"Found {len(file_list)} files")

# Pattern 2: File processing loop (DUPLICATED)
for file_info in file_list:
    try:
        # Read file
        if is_parquet:
            df = read_parquet(file_info)
        else:
            df = read_json(file_info)
        
        # Analyze
        count = len(df)
        operations = df.groupBy('_cdc_operation').count()
        
        # Track stats
        total_count += count
        operation_counts[op] += count
        
    except Exception as e:
        print(f"Error: {e}")

# Pattern 3: Summary report (DUPLICATED)
print(f"Total records: {total_count}")
print(f"Files processed: {files_processed}")
for op, count in operation_counts.items():
    print(f"  {op}: {count}")
```

### Refactoring Opportunity

**Potential Savings:** ~300-400 lines

```python
def analyze_changefeed_files(
    file_reader: Callable,          # How to read files
    file_lister: Callable,          # How to list files
    path: str,
    primary_key_columns: List[str] = None,
    debug: bool = False
) -> Dict[str, Any]:
    """
    Generic changefeed file analyzer.
    Works for both Volume and Azure via strategy functions.
    """
    # List files
    file_list = file_lister(path)
    
    if debug:
        print(f"📂 Found {len(file_list)} files in {path}")
    
    # Process files
    stats = {
        'total_files': len(file_list),
        'files_processed': 0,
        'files_errored': 0,
        'total_records': 0,
        'operation_counts': {},
        'fragmentation_detected': False
    }
    
    for file_info in file_list:
        try:
            # Read file (strategy pattern)
            df = file_reader(file_info)
            
            # Analyze
            count = len(df)
            operations = df.groupBy('_cdc_operation').count().collect()
            
            # Update stats
            stats['files_processed'] += 1
            stats['total_records'] += count
            for row in operations:
                op = row['_cdc_operation']
                stats['operation_counts'][op] = stats['operation_counts'].get(op, 0) + row['count']
            
            # Check fragmentation
            if primary_key_columns:
                unique_keys = df.select(*primary_key_columns).distinct().count()
                if count > unique_keys:
                    stats['fragmentation_detected'] = True
            
        except Exception as e:
            stats['files_errored'] += 1
            if debug:
                print(f"⚠️ Error processing {file_info['name']}: {e}")
    
    # Print summary
    if debug:
        print(f"\n📊 Analysis Summary:")
        print(f"   Files processed: {stats['files_processed']}")
        print(f"   Total records: {stats['total_records']:,}")
        print(f"   Operations:")
        for op, count in sorted(stats['operation_counts'].items()):
            print(f"      {op}: {count:,}")
    
    return stats

# Then:
def analyze_volume_changefeed_files(volume_path, primary_keys=None, debug=False, spark=None, dbutils=None):
    return analyze_changefeed_files(
        file_reader=lambda fi: spark.read.parquet(fi['path']) if fi['name'].endswith('.parquet') 
                               else spark.read.json(fi['path']),
        file_lister=lambda p: _list_volume_files(p, spark, dbutils),
        path=volume_path,
        primary_key_columns=primary_keys,
        debug=debug
    )

def analyze_azure_changefeed_files(account, key, container, prefix, format='parquet', primary_keys=None, debug=False):
    return analyze_changefeed_files(
        file_reader=lambda fi: read_from_azure(account, key, container, fi['name'], format),
        file_lister=lambda p: _list_azure_files(container, prefix),
        path=prefix,
        primary_key_columns=primary_keys,
        debug=debug
    )
```

**Recommendation:** 🟡 **Consider for v2.5** (improves maintainability)

---

## Summary: Duplication Metrics

| Code Category | Lines | Duplication | Refactorable | Savings |
|--------------|-------|-------------|--------------|---------|
| File reading methods | ~450 | 70% | Yes | 200-300 lines |
| File listing methods | ~180 | 50% | Yes | 80-100 lines |
| Schema/metadata loading | ~250 | 60% | Yes | 100-120 lines |
| Record processing | ~200 | 20% | No | N/A |
| Analysis functions | ~835 | 65% | Yes | 300-400 lines |
| **TOTAL** | **1,915** | **~55%** | **Most** | **~700-1,000 lines** |

---

## Recommendations by Priority

### Priority 1: Low-Hanging Fruit (v2.4)
**Effort: Low | Risk: Low | Benefit: Medium**

1. ✅ **Extract file listing logic** (~80 lines saved)
   - Create `_list_files_recursive()` helper
   - Refactor `_list_volume_files()` and `_list_azure_files()`

2. ✅ **Consolidate schema validation** (~100 lines saved)
   - Create `SchemaLoader` class with `_load_and_validate()`
   - Unify error handling and validation

### Priority 2: Moderate Refactoring (v2.5)
**Effort: Medium | Risk: Low | Benefit: High**

3. ✅ **Extract analysis logic** (~300 lines saved)
   - Create `analyze_changefeed_files()` with strategy pattern
   - Refactor both `analyze_volume_changefeed_files()` and `analyze_azure_changefeed_files()`

### Priority 3: Major Refactoring (v3.0)
**Effort: High | Risk: Medium | Benefit: Very High**

4. ⚠️ **Abstract file reading** (~500 lines saved)
   - Create `FileReader` abstract base class
   - Implement `VolumeReader` and `AzureReader` subclasses
   - Move deduplication logic to base class
   - **Risk:** Major architecture change, extensive testing needed

---

## Why Some Duplication Is Acceptable

### Strategic Duplication Benefits

1. **Clarity for different storage backends**
   - Volume uses `dbutils.fs.ls()`
   - Azure uses `BlobServiceClient`
   - Keeping them separate makes it obvious which API is used

2. **Independent evolution**
   - Volume features can evolve without affecting Azure
   - Azure-specific optimizations don't complicate Volume code

3. **Easier debugging**
   - Stack traces clearly show which mode failed
   - No abstraction layers to wade through

4. **Lower cognitive load**
   - Developers can understand one mode without understanding all modes
   - New contributors can modify one backend without breaking others

### When to Refactor

✅ Refactor if:
- Logic changes must be synchronized across multiple methods
- Bugs are being fixed in multiple places
- Tests need to be duplicated for each variant

❌ Don't refactor if:
- Duplication is small (<20 lines)
- Methods are diverging over time
- Abstraction would obscure the implementation

---

## Conclusion

**Current State:** The codebase has ~55% duplication in file I/O operations, which is **acceptable for a connector library** where different storage backends require different APIs.

**Recommendation:** Implement **Priority 1 and 2 refactorings** (v2.4-v2.5) to reduce duplication by ~40%, saving ~400-500 lines while maintaining clarity.

**Defer:** Major architecture refactoring (Priority 3) until v3.0 when there's more certainty about which patterns are most common and which backends need to be added (S3, GCS, etc.).

**Bottom Line:** The duplication is **intentional and manageable**, not **accidental and problematic**. Strategic refactoring can improve maintainability without sacrificing clarity.
