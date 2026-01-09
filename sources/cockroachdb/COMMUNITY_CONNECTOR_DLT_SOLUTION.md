# Community Connector + DLT: Making dbutils Work

## Problem

The community connector interface has a **fixed signature**:

```python
def read_table(
    self, 
    table_name: str, 
    start_offset: Dict[str, str], 
    table_options: Dict[str, str]
) -> Iterator[Dict[str, Any]]:
    """Can't change this signature - it's the interface contract!"""
```

But we need `dbutils` to list files in `_read_table_from_volume()`!

---

## ✅ **Solution 1: Store dbutils During Initialization (Best)**

### **Modify `__init__()` to Accept dbutils**

```python
class LakeflowConnect:
    def __init__(
        self,
        config: Dict[str, Any],
        dbutils = None  # ✅ Add this parameter
    ):
        """Initialize connector with optional dbutils."""
        # Store dbutils for later use
        self._dbutils = dbutils
        
        # Rest of initialization...
        if config.get("volume_path"):
            self.mode = "volume"
            self.volume_path = config["volume_path"]
            # ...
```

### **Use Stored dbutils in _read_table_from_volume()**

```python
def _read_table_from_volume(
    self, 
    table_name: str, 
    start_offset: Dict[str, str], 
    table_options: Dict[str, str]
) -> Iterator[Dict[str, Any]]:
    """Use stored dbutils from initialization."""
    
    try:
        # ✅ Use stored dbutils if available
        if self._dbutils is not None:
            files = self._dbutils.fs.ls(self.volume_path)
            
            # dbutils returns FileInfo with attributes
            file_list = []
            for file_info in files:
                if file_info.name.endswith('.parquet'):
                    file_list.append({
                        'name': file_info.name,
                        'path': file_info.path,
                        'size': file_info.size
                    })
        else:
            # ⚠️ Fallback to JVM (may fail in DLT serverless)
            files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(self.volume_path)
            
            # JVM returns FileInfo with methods
            file_list = []
            for file_info in files:
                name = file_info.name()
                if name.endswith('.parquet'):
                    file_list.append({
                        'name': name,
                        'path': file_info.path(),
                        'size': file_info.size()
                    })
    except Exception as e:
        # Final fallback: Use Spark
        try:
            file_df = spark.read.format("binaryFile").load(self.volume_path)
            file_list = [
                {
                    'name': row.path.split('/')[-1],
                    'path': row.path,
                    'size': row.length
                }
                for row in file_df.select("path", "length").collect()
                if row.path.endswith('.parquet')
            ]
        except Exception as e2:
            return iter([]), start_offset
    
    # ... rest of implementation
```

### **Usage in DLT:**

```python
import dlt
from cockroachdb import LakeflowConnect

# ✅ Pass dbutils during initialization
connector = LakeflowConnect(
    config={"volume_path": "dbfs:/Volumes/main/schema/volume/"},
    dbutils=dbutils  # Available in DLT!
)

@dlt.table
def usertable():
    """Community connector - dbutils already stored!"""
    return connector.read_table(
        table_name="usertable",
        start_offset={},
        table_options={}
        # No need to pass dbutils - it's stored in connector!
    )
```

**Pros:**
- ✅ No signature change to `read_table()`
- ✅ Works in all DLT environments
- ✅ Clean API
- ✅ Backward compatible (dbutils is optional)

---

## ✅ **Solution 2: Try to Get dbutils from Global Scope (Fallback)**

### **Add Helper Method to Get dbutils**

```python
class LakeflowConnect:
    def _get_dbutils(self):
        """Try to get dbutils from various sources."""
        # 1. Use stored dbutils if available
        if hasattr(self, '_dbutils') and self._dbutils is not None:
            return self._dbutils
        
        # 2. Try to import from global scope (works in notebooks/DLT)
        try:
            import IPython
            ipython = IPython.get_ipython()
            if ipython is not None and hasattr(ipython, 'user_ns'):
                dbutils = ipython.user_ns.get('dbutils')
                if dbutils is not None:
                    return dbutils
        except:
            pass
        
        # 3. Try globals (works in some contexts)
        try:
            if 'dbutils' in globals():
                return globals()['dbutils']
        except:
            pass
        
        # 4. Return None (will trigger fallback)
        return None
```

### **Use in _read_table_from_volume()**

```python
def _read_table_from_volume(
    self, 
    table_name: str, 
    start_offset: Dict[str, str], 
    table_options: Dict[str, str]
) -> Iterator[Dict[str, Any]]:
    """Try to get dbutils automatically."""
    
    # ✅ Try to get dbutils
    dbutils = self._get_dbutils()
    
    try:
        if dbutils is not None:
            files = dbutils.fs.ls(self.volume_path)
            # ... handle dbutils FileInfo
        else:
            # Fallback to JVM
            files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(self.volume_path)
            # ... handle JVM FileInfo
    except Exception as e:
        # Final fallback
        # ...
```

**Pros:**
- ✅ Automatic detection
- ✅ Works without explicit passing
- ✅ No API changes needed

**Cons:**
- ⚠️ Fragile (depends on IPython internals)
- ⚠️ May not work in all environments
- ⚠️ Hard to debug when it fails

---

## ✅ **Solution 3: Document JVM Fallback Limitations**

### **Accept Current Implementation with Clear Documentation**

If you keep the current code:

```python
def _read_table_from_volume(self, ...) -> Iterator[Dict[str, Any]]:
    """Read table data from Databricks Unity Catalog Volume."""
    
    try:
        # Use dbutils to list files (works in Databricks)
        files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(self.volume_path)
        
        # Convert Java collection to Python list
        file_list = []
        for file_info in files:
            name = file_info.name()  # JVM method
            if name.endswith('.parquet'):
                file_list.append({
                    'name': name,
                    'path': file_info.path(),
                    'size': file_info.size()
                })
    except Exception as e:
        # Fallback: use Spark to list files
        file_df = spark.read.format("binaryFile").load(self.volume_path)
        file_list = [...]
```

**Add Documentation:**

```python
"""
COMPATIBILITY NOTES:
- DLT Standard: ✅ Works (JVM access available)
- DLT Serverless: ⚠️ May work with Spark fallback
- Spark Connect: ⚠️ May work with Spark fallback

For maximum compatibility, pass dbutils during initialization:
    connector = LakeflowConnect(config, dbutils=dbutils)
"""
```

**Usage:**

```python
import dlt
from cockroachdb import LakeflowConnect

# Current usage (no changes needed)
connector = LakeflowConnect({
    "volume_path": "dbfs:/Volumes/main/schema/volume/"
})

@dlt.table
def usertable():
    """May work in DLT standard, might fail in serverless"""
    return connector.read_table(
        table_name="usertable",
        start_offset={},
        table_options={}
    )
```

**Pros:**
- ✅ No code changes needed
- ✅ Works in many environments
- ✅ Has fallback to Spark

**Cons:**
- ❌ May fail in DLT serverless
- ❌ Unpredictable behavior
- ❌ Harder to debug

---

## 📊 **Comparison**

| Solution | API Change | DLT Standard | DLT Serverless | Recommended |
|----------|-----------|--------------|----------------|-------------|
| **1. Store dbutils in init** | ✅ Minimal | ✅ Works | ✅ Works | ✅ **Best** |
| **2. Auto-detect dbutils** | ✅ None | ✅ Works | ⚠️ Maybe | ⚠️ OK |
| **3. Current (JVM fallback)** | ✅ None | ✅ Works | ❌ May fail | ❌ Avoid |

---

## 🎯 **Recommended Implementation (Solution 1)**

### **Changes to cockroachdb.py:**

```python
class LakeflowConnect:
    """CockroachDB connector for Databricks Lakeflow."""
    
    # Class variables
    _shared_snapshot_timestamp = None
    _column_family_cache = {}
    
    def __init__(
        self,
        config: Dict[str, Any],
        dbutils = None  # ✅ ADD THIS
    ):
        """
        Initialize the CockroachDB connector.
        
        Args:
            config: Configuration dictionary containing connection details
            dbutils: Optional DBUtils instance (recommended for DLT)
        
        Example:
            # In notebook or DLT
            connector = LakeflowConnect(
                config={"volume_path": "..."},
                dbutils=dbutils
            )
        """
        # ✅ Store dbutils for later use
        self._dbutils = dbutils
        
        # Existing initialization code...
        if config.get("volume_path"):
            self.mode = "volume"
            self.volume_path = config["volume_path"]
            # ...
        elif config.get("azure_storage_account"):
            self.mode = "azure_parquet"
            # ...
        else:
            self.mode = "direct"
            # ...
    
    def _read_table_from_volume(
        self, 
        table_name: str, 
        start_offset: Dict[str, str], 
        table_options: Dict[str, str]
    ) -> Iterator[Dict[str, Any]]:
        """
        Read table data from Databricks Unity Catalog Volume.
        
        Uses stored dbutils if available, falls back to JVM/Spark otherwise.
        """
        from pyspark.sql import SparkSession
        import pyarrow.parquet as pq
        
        spark = SparkSession.builder.getOrCreate()
        last_cursor = start_offset.get("cursor", "") if start_offset else ""
        
        # Load schema to get primary keys
        schema_info = self._load_schema_from_volume(self.volume_path)
        primary_keys = schema_info.get('primary_keys', []) if schema_info else []
        
        try:
            # ✅ PRIORITY 1: Use stored dbutils (most reliable)
            if self._dbutils is not None:
                files = self._dbutils.fs.ls(self.volume_path)
                
                # dbutils FileInfo has attributes (not methods)
                file_list = []
                for file_info in files:
                    if file_info.name.endswith('.parquet'):
                        file_list.append({
                            'name': file_info.name,
                            'path': file_info.path,
                            'size': file_info.size
                        })
            else:
                # ⚠️ FALLBACK 1: Try JVM access (may fail in Spark Connect/DLT serverless)
                files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(self.volume_path)
                
                # JVM FileInfo has methods (not attributes)
                file_list = []
                for file_info in files:
                    name = file_info.name()
                    if name.endswith('.parquet'):
                        file_list.append({
                            'name': name,
                            'path': file_info.path(),
                            'size': file_info.size()
                        })
        
        except Exception as e:
            # ⚠️ FALLBACK 2: Use Spark DataFrame listing (slower but more compatible)
            try:
                file_df = spark.read.format("binaryFile").load(self.volume_path)
                file_list = [
                    {
                        'name': row.path.split('/')[-1],
                        'path': row.path,
                        'size': row.length
                    }
                    for row in file_df.select("path", "length").collect()
                    if row.path.endswith('.parquet')
                ]
            except Exception as e2:
                # Give up - no files accessible
                return iter([]), start_offset
        
        # Rest of implementation unchanged...
        # Filter and sort files by cursor
        new_files = [f for f in file_list if f['name'] > last_cursor]
        new_files.sort(key=lambda f: f['name'])
        
        if not new_files:
            return iter([]), start_offset
        
        # ... continue with existing logic
```

### **Usage Example:**

```python
import dlt
from cockroachdb import LakeflowConnect

# ✅ Initialize with dbutils
connector = LakeflowConnect(
    config={
        "volume_path": "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/"
    },
    dbutils=dbutils  # Pass it here!
)

@dlt.table(
    name="usertable_bronze",
    comment="CockroachDB CDC - Community Connector"
)
def usertable_bronze():
    """Community connector pattern with dbutils."""
    return connector.read_table(
        table_name="usertable",
        start_offset={},
        table_options={}
        # No need to pass dbutils - already stored!
    )
```

---

## ✅ **Summary**

**For community connector to work in DLT:**

1. ✅ **Modify `__init__()`** to accept `dbutils` parameter
2. ✅ **Store as instance variable** (`self._dbutils`)
3. ✅ **Use in `_read_table_from_volume()`** as first choice
4. ✅ **Keep JVM/Spark fallbacks** for backward compatibility

**Benefits:**
- ✅ No change to `read_table()` signature (interface preserved)
- ✅ Works in all DLT environments
- ✅ Backward compatible (dbutils optional)
- ✅ Clean, predictable behavior

**One-line change needed:**
```python
# OLD
def __init__(self, config: Dict[str, Any]):

# NEW
def __init__(self, config: Dict[str, Any], dbutils = None):
    self._dbutils = dbutils  # Store for later use
```

Then update `_read_table_from_volume()` to check `self._dbutils` first!

---

## 🔗 **Related Files**

- **`cockroachdb.py` line 75**: `__init__()` method
- **`cockroachdb.py` lines 462-464**: `read_table()` interface
- **`cockroachdb.py` lines 819-900**: `_read_table_from_volume()` implementation
- **`DBUTILS_IN_DLT.md`**: Full dbutils compatibility analysis


