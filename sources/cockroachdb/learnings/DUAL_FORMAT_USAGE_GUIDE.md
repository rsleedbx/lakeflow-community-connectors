# Dual-Format CDC Usage Guide

**Date:** 2025-12-23  
**Feature:** Multi-catalog/schema support with dual-format (JSON + Parquet)

---

## 🎯 **Overview**

The CockroachDB connector now supports:
1. ✅ **Required catalog/schema parameters** - Prevents namespace collisions
2. ✅ **Format selection** - Parquet, JSON, or both
3. ✅ **Automatic path organization** - Format-first hierarchy
4. ✅ **Easy format switching** - Change one parameter

---

## 📁 **Storage Structure**

```
changefeed-events/                          # Azure container
├── parquet/                                # Format separation
│   └── {catalog}/                          # e.g., ecommerce
│       └── {schema}/                       # e.g., public
│           └── {table}/                    # e.g., orders
│               └── YYYY-MM-DD/             # Date partitioning
│                   └── {timestamp}-...parquet
└── json/                                   # Format separation
    └── {catalog}/                          # e.g., ecommerce
        └── {schema}/                       # e.g., public
            └── {table}/                    # e.g., orders
                └── YYYY-MM-DD/             # Date partitioning
                    └── {timestamp}-...ndjson
```

---

## 🔧 **Configuration**

### **Required Parameters**

```python
credentials = {
    # REQUIRED: Namespace identification
    "catalog": "ecommerce",  # Database/catalog name
    "schema": "public",      # Schema name
    
    # REQUIRED: CockroachDB connection
    "token": "username:password",
    "base_url": "postgresql://host:26257/ecommerce?sslmode=require",
    
    # REQUIRED: Azure storage (for Azure mode)
    "azure_storage_account": "cockroachcdc",
    "azure_storage_key": "your-key-here",
    "azure_storage_container": "changefeed-events",
    
    # OPTIONAL: Format selection (default: 'parquet')
    "format": "parquet"  # or "json" or "both"
}
```

### **Format Options**

| Format | Description | Use Case | Changefe files |
|--------|-------------|----------|----------------|
| `parquet` | Columnar format | Analytics, large datasets | 1 changefeed |
| `json` | Human-readable | Debugging, small datasets | 1 changefeed |
| `both` | Creates both | Testing, comparison | 2 changefeeds |

---

## 💻 **Usage Examples**

### **Example 1: Parquet Format (Recommended)**

```python
from cockroachdb import LakeflowConnect

credentials = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "parquet",
    
    "token": "user:pass",
    "base_url": "postgresql://host:26257/ecommerce?sslmode=require",
    
    "azure_storage_account": "cockroachcdc",
    "azure_storage_key": "...",
    "azure_storage_container": "changefeed-events"
}

connector = LakeflowConnect(credentials)

# Connector automatically:
# 1. Creates changefeed to: parquet/ecommerce/public/
# 2. Lists files from: parquet/ecommerce/public/orders/
# 3. Processes Parquet CDC events

for row in connector.read_table("orders", {}):
    print(row)
```

**Output:**
```
Mode: Azure Parquet | Container: changefeed-events
Creating changefeed for: ecommerce.public.orders
Files will be written to: parquet/ecommerce/public/
```

---

### **Example 2: JSON Format**

```python
credentials = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "json",  # Changed to JSON
    
    # ... other credentials ...
}

connector = LakeflowConnect(credentials)

# Connector automatically:
# 1. Creates changefeed to: json/ecommerce/public/
# 2. Lists files from: json/ecommerce/public/orders/
# 3. Processes JSON CDC events with before/after values

for row in connector.read_table("orders", {}):
    print(f"Operation: {row['_cdc_operation']}")
    print(f"Before: {row.get('_before')}")
    print(f"After: {row.get('_after')}")
```

**Output:**
```
Mode: Azure JSON | Container: changefeed-events
Creating changefeed for: ecommerce.public.orders
Files will be written to: json/ecommerce/public/
Operation: UPDATE
Before: {'status': 'pending'}
After: {'status': 'shipped'}
```

---

### **Example 3: Dual Format (Both)**

```python
credentials = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "both",  # Creates BOTH changefeeds
    
    # ... other credentials ...
}

connector = LakeflowConnect(credentials)

# Connector automatically:
# 1. Creates TWO changefeeds:
#    - JSON: json/ecommerce/public/
#    - Parquet: parquet/ecommerce/public/
# 2. Reads from both (for comparison)

for row in connector.read_table("orders", {}):
    print(row)
```

**Output:**
```
Mode: Azure Dual (JSON+Parquet) | Container: changefeed-events
Creating JSON changefeed for: ecommerce.public.orders
Creating Parquet changefeed for: ecommerce.public.orders
Files will be written to:
  - json/ecommerce/public/
  - parquet/ecommerce/public/
```

---

## 🔀 **Format Switching**

### **Switch Between Formats**

```python
# Test with Parquet
credentials_parquet = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "parquet",  # Change this line
    # ... other credentials ...
}

# Test with JSON
credentials_json = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "json",  # Change this line
    # ... other credentials ...
}

# Test with both
credentials_both = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "both",  # Change this line
    # ... other credentials ...
}
```

**That's it!** Just change ONE parameter.

---

## 🏢 **Multi-Database Example**

### **Scenario: Multiple databases with duplicate table names**

```python
# Database 1: ecommerce.public.orders
ecommerce_creds = {
    "catalog": "ecommerce",
    "schema": "public",
    "format": "parquet",
    # ... connection details ...
}

# Database 2: warehouse.public.orders
warehouse_creds = {
    "catalog": "warehouse",
    "schema": "public",
    "format": "parquet",
    # ... connection details ...
}

# Database 3: analytics.staging.orders
analytics_creds = {
    "catalog": "analytics",
    "schema": "staging",
    "format": "json",  # Different format for testing
    # ... connection details ...
}

# Each creates files in separate paths
ecommerce_conn = LakeflowConnect(ecommerce_creds)
warehouse_conn = LakeflowConnect(warehouse_creds)
analytics_conn = LakeflowConnect(analytics_creds)

# Files are organized:
# parquet/ecommerce/public/orders/
# parquet/warehouse/public/orders/
# json/analytics/staging/orders/
```

**Result:** No collisions! Each table has its own namespace.

---

## 📊 **Databricks Autoloader Integration**

### **Parquet Format:**

```python
# Databricks notebook
from pyspark.sql import functions as F

# Path automatically constructed by connector
source_path = "wasbs://changefeed-events@account.blob.core.windows.net/parquet/ecommerce/public/orders/"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/checkpoints/parquet/schema")
    .load(source_path)
)

# Process and write to Delta
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/parquet/data") \
    .table("ecommerce_catalog.public.orders")
```

### **JSON Format:**

```python
# Switch to JSON by changing ONE line
source_path = "wasbs://changefeed-events@account.blob.core.windows.net/json/ecommerce/public/orders/"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")  # Changed
    .option("cloudFiles.schemaLocation", "/checkpoints/json/schema")  # Changed
    .load(source_path)
)

# Process and write to Delta
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/json/data") \
    .table("ecommerce_catalog.public.orders_json")
```

### **Compare Both Formats:**

```python
# Read from both paths
parquet_df = spark.read.parquet("...parquet/ecommerce/public/orders/")
json_df = spark.read.json("...json/ecommerce/public/orders/")

print(f"Parquet rows: {parquet_df.count()}")
print(f"JSON rows: {json_df.count()}")
print(f"Parquet size: {parquet_df.rdd.map(len).sum()} bytes")
print(f"JSON size: {json_df.rdd.map(len).sum()} bytes")
```

---

## ✅ **Migration from Old Version**

### **Old Credentials (No catalog/schema):**

```python
# OLD - Will fail with new version
old_credentials = {
    "token": "user:pass",
    "base_url": "postgresql://host:26257/ecommerce",
    "azure_storage_account": "cockroachcdc",
    "azure_storage_key": "...",
    "azure_storage_container": "changefeed-events",
    "azure_path_prefix": "my-custom-path"  # Manual path
}

# ERROR: ValueError: 'catalog' parameter is REQUIRED
```

### **New Credentials (With catalog/schema):**

```python
# NEW - Required parameters
new_credentials = {
    "catalog": "ecommerce",  # ADD THIS
    "schema": "public",      # ADD THIS
    "format": "parquet",     # ADD THIS (optional, defaults to 'parquet')
    
    "token": "user:pass",
    "base_url": "postgresql://host:26257/ecommerce",
    "azure_storage_account": "cockroachcdc",
    "azure_storage_key": "...",
    "azure_storage_container": "changefeed-events"
    # azure_path_prefix is AUTO-CONSTRUCTED
}

# SUCCESS: Path automatically becomes parquet/ecommerce/public/
```

---

## 🎓 **Best Practices**

1. **Use Parquet for production analytics**
   - Smaller files
   - Faster queries
   - Better compression

2. **Use JSON for debugging**
   - Human-readable
   - Explicit operations (INSERT/UPDATE/DELETE)
   - Before/after values

3. **Use 'both' for testing**
   - Compare performance
   - Validate correctness
   - Switch between formats easily

4. **Always specify catalog and schema**
   - Prevents naming collisions
   - Enables multi-tenant deployments
   - Clear data organization

5. **Use format-first paths**
   - Easy to switch formats
   - Clear separation of concerns
   - Databricks-friendly

---

## 🔗 **References**

- **Tutorial:** `STREAM_CHANGEFEED_TO_DATABRICKS.md`
- **Multi-catalog:** `MULTI_CATALOG_SCHEMA_SUPPORT.md`
- **Parquet CDC:** `PARQUET_CDC_PROCESSING_GUIDE.md`
- **Test results:** `PARQUET_CDC_TEST_RESULTS.md`

---

**Last Updated:** 2025-12-23  
**Status:** ✅ Production-ready




