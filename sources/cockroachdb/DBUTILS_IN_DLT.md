# dbutils.fs.ls() in Delta Live Tables (DLT)

## Question
Would the `dbutils.fs.ls()` call work inside DLT (Delta Live Tables)?

```python
# From cockroachdb.py lines 3683-3689
if dbutils is not None:
    files = dbutils.fs.ls(volume_path)  # ❓ Works in DLT?
else:
    files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(volume_path)
```

## Answer: **Partially - With Restrictions** ⚠️

---

## 🔍 **DLT Environment Analysis**

### **What DLT Provides:**

| Resource | Available in DLT? | Notes |
|----------|------------------|-------|
| `spark` | ✅ Yes | SparkSession available |
| `dbutils` | ⚠️ **Limited** | Some commands restricted |
| `dbutils.fs.*` | ✅ **Mostly Yes** | File operations work |
| `dbutils.secrets.*` | ✅ Yes | Secret access works |
| `dbutils.notebook.*` | ❌ No | Not available in DLT |
| `dbutils.widgets.*` | ❌ No | Not available in DLT |

### **Specifically for `dbutils.fs.ls()`:**

✅ **YES - It works in DLT!**

```python
# This DOES work in DLT
files = dbutils.fs.ls("dbfs:/Volumes/main/schema/volume/")
```

---

## ⚠️ **Known Issues & Restrictions**

### **Issue 1: Community Connector Pattern ❌**

The code in question is from `_read_table_from_volume()`, which is part of the **DLT Community Connector pattern**:

```python
def read_table(self, table_name, start_offset, table_options) -> Iterator[Dict]:
    """DLT Community Connector interface"""
    return self._read_table_from_volume(...)
```

**Problem:** DLT Community Connectors have their own restrictions:
- Must return `Iterator[Dict]`
- Called during DLT pipeline execution
- Limited access to some runtime features

**Status:** `dbutils.fs.ls()` should still work, but you're **already using batch reading** in this mode, not Autoloader!

---

### **Issue 2: Spark Connect Mode ⚠️**

The fallback code tries to access Spark JVM:

```python
# Fallback - PROBLEMATIC!
files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(volume_path)
```

**Problems:**
- ❌ **Doesn't work with Spark Connect** (client/server mode)
- ❌ **Not available in DLT serverless** (no JVM access)
- ⚠️ **Deprecated pattern** (internal API)

**Recommendation:** Always pass `dbutils` explicitly to functions, don't rely on fallback.

---

### **Issue 3: Iterator Pattern in DLT ⚠️**

Your current implementation uses **batch reading** for the iterator pattern:

```python
def _read_table_from_volume(self, ...) -> Iterator[Dict]:
    # List files
    files = dbutils.fs.ls(volume_path)  # ✅ Works
    
    # Read with spark.read (batch)
    for file in files:
        df = spark.read.parquet(file)  # ✅ Works
        records = df.toPandas().to_dict('records')  # ⚠️ Might be slow
        
        for record in records:
            yield record  # ✅ Works
```

**Issues:**
- ⚠️ `.toPandas()` loads entire file into memory
- ⚠️ For large files, this can cause OOM errors
- ⚠️ Not optimized for DLT's distributed processing

---

## ✅ **What DOES Work in DLT**

### **Option 1: Autoloader with DLT Table Definition (Best)**

```python
import dlt

@dlt.table(
    name="usertable_bronze",
    comment="CockroachDB CDC ingestion"
)
def usertable_bronze():
    """DLT table definition - uses Autoloader directly"""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/Volumes/.../schema")
            .load("/Volumes/main/schema/volume/")
    )

@dlt.table(
    name="usertable_silver",
    comment="Merged and transformed data"
)
def usertable_silver():
    """Apply transformations"""
    from cockroachdb import merge_column_family_fragments
    
    df = dlt.read_stream("usertable_bronze")
    return merge_column_family_fragments(df, primary_keys=["ycsb_key"])
```

**Pros:**
- ✅ Native DLT pattern
- ✅ Uses Autoloader (optimal performance)
- ✅ No iterator needed
- ✅ Fully integrated with DLT lifecycle

---

### **Option 2: Community Connector with dbutils (Current)**

```python
# In DLT pipeline file
import dlt
from cockroachdb import LakeflowConnect

# Initialize connector
connector = LakeflowConnect({
    "volume_path": "dbfs:/Volumes/main/schema/volume/"
})

@dlt.table
def usertable():
    """Using community connector pattern"""
    # Pass dbutils explicitly!
    return connector.read_table(
        table_name="usertable",
        start_offset={},
        table_options={}
    )
```

**Current implementation in `_read_table_from_volume()`:**
```python
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

**Issues:**
- ⚠️ Uses JVM access (may fail in DLT serverless)
- ⚠️ No `dbutils` passed explicitly
- ⚠️ Fallback might also fail

---

## 🔧 **Recommended Fix: Pass dbutils Explicitly**

### **Problem:**
Current code doesn't accept `dbutils` as parameter in `_read_table_from_volume()`.

### **Solution:**
Modify `_read_table_from_volume()` to accept `dbutils`:

```python
def _read_table_from_volume(
    self, 
    table_name: str, 
    start_offset: Dict[str, str], 
    table_options: Dict[str, str],
    dbutils = None  # ✅ Add this parameter
) -> Iterator[Dict[str, Any]]:
    """Read table data from Databricks Unity Catalog Volume."""
    from pyspark.sql import SparkSession
    import pyarrow.parquet as pq
    
    spark = SparkSession.builder.getOrCreate()
    last_cursor = start_offset.get("cursor", "") if start_offset else ""
    
    # Load schema to get primary keys
    schema_info = self._load_schema_from_volume(self.volume_path)
    primary_keys = schema_info.get('primary_keys', []) if schema_info else []
    
    try:
        if dbutils is not None:
            # ✅ PREFERRED: Use dbutils directly
            files = dbutils.fs.ls(self.volume_path)
            
            # dbutils returns FileInfo objects with attributes (not methods)
            file_list = []
            for file_info in files:
                if file_info.name.endswith('.parquet'):
                    file_list.append({
                        'name': file_info.name,
                        'path': file_info.path,
                        'size': file_info.size
                    })
        else:
            # ⚠️ FALLBACK: Try JVM access (may fail in Spark Connect/DLT serverless)
            files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(self.volume_path)
            
            # JVM returns FileInfo objects with methods (not attributes)
            file_list = []
            for file_info in files:
                name = file_info.name()  # Method call
                if name.endswith('.parquet'):
                    file_list.append({
                        'name': name,
                        'path': file_info.path(),
                        'size': file_info.size()
                    })
    except Exception as e:
        # ⚠️ LAST RESORT: Use Spark DataFrame listing (slower)
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

### **Then modify `read_table()` to pass dbutils:**

```python
def read_table(
    self, 
    table_name: str, 
    start_offset: Dict[str, str], 
    table_options: Dict[str, str],
    dbutils = None  # ✅ Add this parameter
) -> Iterator[Dict[str, Any]]:
    """Read data from a CockroachDB table."""
    if self.mode == "volume":
        return self._read_table_from_volume(
            table_name, 
            start_offset, 
            table_options,
            dbutils=dbutils  # ✅ Pass it through
        )
    elif self.mode == "azure_parquet":
        return self._read_table_from_azure_parquet(table_name, start_offset, table_options)
    else:
        return self._read_table_direct(table_name, start_offset, table_options)
```

### **Usage in DLT:**

```python
import dlt
from cockroachdb import LakeflowConnect

connector = LakeflowConnect({"volume_path": "..."})

@dlt.table
def usertable():
    # ✅ Pass dbutils explicitly
    return connector.read_table(
        table_name="usertable",
        start_offset={},
        table_options={},
        dbutils=dbutils  # ✅ Available in DLT!
    )
```

---

## 📊 **Compatibility Matrix**

| Environment | `dbutils` param | JVM fallback | Spark fallback | Result |
|-------------|----------------|--------------|----------------|--------|
| **DLT Standard** | ✅ Works | ⚠️ Works | ✅ Works | ✅ **All work** |
| **DLT Serverless** | ✅ Works | ❌ Fails | ✅ Works | ⚠️ **Need dbutils param** |
| **Spark Connect** | ✅ Works | ❌ Fails | ✅ Works | ⚠️ **Need dbutils param** |
| **Notebook** | ✅ Works | ⚠️ Works | ✅ Works | ✅ **All work** |

**Conclusion:** Always pass `dbutils` explicitly for maximum compatibility!

---

## ✅ **Recommended Architecture for DLT**

### **Best: Use Native DLT Tables (No Connector)**

```python
import dlt
from pyspark.sql import functions as F

@dlt.table
def usertable_bronze():
    """Bronze layer - raw CDC data with Autoloader"""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/Volumes/.../schema")
            .load("/Volumes/main/schema/volume/")
            .withColumn("_cdc_operation",
                F.when(F.col("__crdb__event_type") == "c", F.lit("UPSERT"))
                .when(F.col("__crdb__event_type") == "d", F.lit("DELETE"))
            )
    )

@dlt.table
def usertable_silver():
    """Silver layer - merged column families"""
    from cockroachdb import merge_column_family_fragments
    
    df = dlt.read_stream("usertable_bronze")
    return merge_column_family_fragments(df, primary_keys=["ycsb_key"])
```

**Why this is better:**
- ✅ No iterator pattern (native streaming)
- ✅ No file listing needed (Autoloader handles it)
- ✅ No `dbutils` dependencies
- ✅ Full DLT integration
- ✅ Optimal performance

---

### **Alternative: Community Connector (If Required)**

If you must use the community connector pattern:

```python
import dlt
from cockroachdb import LakeflowConnect

# Initialize connector with explicit volume path
connector = LakeflowConnect({
    "volume_path": "dbfs:/Volumes/main/schema/volume/"
})

@dlt.table
def usertable():
    """Using community connector - pass dbutils!"""
    return connector.read_table(
        table_name="usertable",
        start_offset={},
        table_options={},
        dbutils=dbutils  # ✅ Pass explicitly
    )
```

---

## 🎯 **Summary**

**Q:** Does `dbutils.fs.ls()` work in DLT?  
**A:** **Yes**, but with caveats:

| Scenario | Works? | Action Needed |
|----------|--------|---------------|
| `dbutils.fs.ls()` directly in DLT | ✅ Yes | None |
| Via JVM fallback in DLT serverless | ❌ No | Pass `dbutils` explicitly |
| Via Spark fallback | ✅ Yes | Slower, but works |

**Recommendations:**
1. ✅ **Best:** Use native DLT tables with Autoloader (no connector)
2. ⚠️ **Alternative:** Modify connector to accept `dbutils` parameter
3. ❌ **Avoid:** Relying on JVM fallback in DLT

**Status:** Your current code will mostly work, but should be improved to:
- Accept `dbutils` as explicit parameter
- Avoid JVM fallback in DLT environments
- Consider using native DLT table definitions instead of iterator pattern

---

## 🔗 **Related Files**

- **`cockroachdb.py` lines 3683-3689**: Current `dbutils.fs.ls()` implementation
- **`cockroachdb.py` lines 819-900**: `_read_table_from_volume()` full implementation
- **`notebooks/NOTEBOOK_COLUMN_FAMILY_FIX.md`**: Example DLT usage
- **`learnings/BUNDLE_TEST_RESULTS.md`**: DLT deployment notes


