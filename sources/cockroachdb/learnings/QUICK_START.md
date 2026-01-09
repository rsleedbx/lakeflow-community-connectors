# Quick Start Guide

**Get started with CockroachDB CDC in 5 minutes**

---

## 🚀 **Fastest Path to Production**

### **Step 1: Create Credentials (30 seconds)**

Create `my_credentials.json`:

```json
{
  "catalog": "ecommerce",
  "schema": "public",
  "format": "parquet",
  
  "token": "username:password",
  "base_url": "postgresql://host:26257/ecommerce?sslmode=require",
  
  "azure_storage_account": "myaccount",
  "azure_storage_key": "your-key-here",
  "azure_storage_container": "changefeed-events"
}
```

Replace:
- `ecommerce` with your database name
- `username:password` with your CockroachDB credentials
- `host:26257` with your CockroachDB host
- Azure storage details

---

### **Step 2: Run Connector (10 seconds)**

```python
from cockroachdb import LakeflowConnect

# Load credentials
import json
with open('my_credentials.json') as f:
    credentials = json.load(f)

# Create connector
connector = LakeflowConnect(credentials)

# Read data
for row in connector.read_table("orders", {}):
    print(row)
```

**That's it!** The connector automatically:
- Creates changefeed to Azure
- Organizes files by format/catalog/schema
- Processes CDC events

---

### **Step 3: Use in Databricks (2 minutes)**

```python
# Databricks notebook
path = f"wasbs://changefeed-events@{account}.blob.core.windows.net/parquet/ecommerce/public/orders/"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load(path)
)

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/data") \
    .table("catalog.schema.orders")
```

Done! Data is streaming to Delta Lake.

---

## 🎯 **What Format Should I Use?**

| If you need... | Use |
|----------------|-----|
| **Production analytics** | `"format": "parquet"` |
| **Debugging/auditing** | `"format": "json"` |
| **Testing both** | `"format": "both"` |

**Default:** `parquet` (best for most use cases)

---

## 🏢 **Multiple Databases?**

Just create separate credentials for each:

```python
# Database 1
ecommerce = {"catalog": "ecommerce", "schema": "public", ...}

# Database 2
warehouse = {"catalog": "warehouse", "schema": "public", ...}

# No collisions!
conn1 = LakeflowConnect(ecommerce)  # Files: parquet/ecommerce/public/
conn2 = LakeflowConnect(warehouse)  # Files: parquet/warehouse/public/
```

---

## 📁 **Where Are My Files?**

Files are automatically organized:

```
changefeed-events/
└── {format}/          # parquet or json
    └── {catalog}/     # ecommerce
        └── {schema}/  # public
            └── {table}/
                └── YYYY-MM-DD/
```

Example:
```
parquet/ecommerce/public/orders/2025-12-23/202512...orders-1.parquet
```

---

## ❓ **Common Questions**

### **Q: Do I need to create the changefeed manually?**
**A:** No! The connector creates it automatically.

### **Q: Can I switch between JSON and Parquet?**
**A:** Yes! Just change `"format": "parquet"` to `"format": "json"`.

### **Q: What if I have duplicate table names?**
**A:** No problem! The `catalog` and `schema` parameters prevent collisions.

### **Q: Does this work with Databricks?**
**A:** Yes! It's specifically optimized for Databricks Autoloader.

### **Q: Is this easier than Snowflake?**
**A:** Much easier! 1 command vs 7 steps for Snowflake.

---

## 📚 **Next Steps**

- **Full guide:** `DUAL_FORMAT_USAGE_GUIDE.md`
- **Databricks tutorial:** `STREAM_CHANGEFEED_TO_DATABRICKS.md`
- **Test results:** `PARQUET_CDC_TEST_RESULTS.md`
- **Multi-catalog:** `MULTI_CATALOG_SCHEMA_SUPPORT.md`

---

## 💡 **Pro Tips**

1. **Start with Parquet** - It's the default for a reason
2. **Use `format: "both"`** - To compare JSON vs Parquet performance
3. **Specify catalog/schema** - Always! It prevents production issues
4. **Let Databricks Autoloader** - Do the heavy lifting (it's automatic!)

---

**Ready to go?** Just copy the credentials template above and you're done in 5 minutes! 🚀




