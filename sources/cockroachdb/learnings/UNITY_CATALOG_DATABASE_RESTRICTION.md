# Unity Catalog Database and Schema Restrictions

## 🐛 **The Error**

```
AnalysisException: [INVALID_DATASOURCE_OPTION_OVERRIDE_ATTEMPT] 
Option database already exists on connection robert_lee_battle-walrus-11108 
and can not be overridden. SQLSTATE: 42000
```

## 🔍 **Root Cause**

Unity Catalog connections have **fixed database AND schema** that are set when the connection is created. Neither can be overridden at the table level.

### **Why This Restriction Exists**

1. **Security**: Unity Catalog controls access at the connection level
2. **Governance**: Database and schema access must be explicitly granted
3. **Auditing**: All data access is tracked via the connection
4. **Simplicity**: One connection = one database + one schema for easier management
5. **Immutability**: Connection options cannot be modified after creation

## ❌ **What Doesn't Work**

### **Attempting to Override Database OR Schema in Table Configuration**

```python
# BOTH of these will FAIL:
table_configuration = {
    "database": "ycsb",  # ❌ Cannot override!
    "schema": "public",   # ❌ Cannot override!
    # ...
}
```

**Errors:**
- `[INVALID_DATASOURCE_OPTION_OVERRIDE_ATTEMPT] Option database already exists on connection`
- `[INVALID_DATASOURCE_OPTION_OVERRIDE_ATTEMPT] Option schema already exists on connection`

### **Code That Tried This**

```python
def _get_database_from_options(self, table_options: Dict[str, str]) -> str:
    """Get database from table_options, falling back to connection database."""
    return table_options.get("database", self.database)  # ❌ Doesn't work!

def _get_connection(self, table_options: Dict[str, str] = None):
    database = self._get_database_from_options(table_options)  # ❌ Rejected by UC
    dsn = f"...dbname={database}..."
```

## ✅ **What Works**

### **1. Database from Connection (Fixed)**

```python
# Connection URL specifies the database
CONNECTION_URL = "postgresql://user:pass@host:26257/ycsb?sslmode=require"
                                                      ^^^^
                                                      Fixed database
```

### **2. Schema per Table (Flexible)**

```python
# Schema CAN be overridden per table:
table_configuration = {
    "schema": "public",   # ✅ Can override!
    "initial_scan": "yes",
    "resolved_interval": "10s"
}
```

## 🎯 **Solution**

### **Connector Design**

```python
class LakeflowConnect:
    def __init__(self, options: Dict[str, str]):
        # Database from connection (fixed)
        self.database = self._parse_from_connection_url()
        
        # Schema can be overridden per-table
        self.default_schema = options.get("schema", "public")
    
    def _get_schema_from_options(self, table_options: Dict[str, str]) -> str:
        """Get schema from table_options, falling back to default."""
        return table_options.get("schema", self.default_schema)
    
    def _get_connection(self, table_options: Dict[str, str] = None):
        # Database is ALWAYS from self.database (no override)
        dsn = f"...dbname={self.database}..."
        
        # Schema comes from table_options (can override)
        schema = self._get_schema_from_options(table_options or {})
```

### **Pipeline Configuration**

```python
# Create connection with correct database
databricks connections create --json '{
    "name": "my_cockroachdb_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
        "base_url": "postgresql://host:26257/ycsb?sslmode=require",
        #                                      ^^^^
        #                                      Database fixed here
        # ...
    }
}'

# Configure pipeline with schema overrides
pipeline_spec = {
    "connection_name": "my_cockroachdb_connection",
    "objects": [
        {
            "table": {
                "source_table": "table1",
                "table_configuration": {
                    "schema": "public",    # ✅ Can specify different schemas
                    # No "database" key!
                }
            }
        },
        {
            "table": {
                "source_table": "table2",
                "table_configuration": {
                    "schema": "staging",   # ✅ Different schema, same database
                }
            }
        }
    ]
}
```

## 📋 **Comparison with Other Connectors**

| Connector | Database Override | Schema Override |
|-----------|-------------------|-----------------|
| **CockroachDB** | ❌ No (UC restriction) | ✅ Yes (per-table) |
| **SQL Server** | ❌ No (UC restriction) | ✅ Yes (per-table) |
| **PostgreSQL** | ❌ No (UC restriction) | ✅ Yes (per-table) |
| **Snowflake** | ❌ No (UC restriction) | ✅ Yes (per-table) |

**Pattern:** Unity Catalog enforces this restriction universally across all database connectors.

## 💡 **Workarounds**

### **If You Need Multiple Databases**

Create **separate connections** for each database:

```bash
# Connection 1: Production database
databricks connections create --json '{
    "name": "cockroach_production",
    "options": {
        "base_url": "postgresql://host:26257/production?sslmode=require",
        # ...
    }
}'

# Connection 2: Analytics database
databricks connections create --json '{
    "name": "cockroach_analytics",
    "options": {
        "base_url": "postgresql://host:26257/analytics?sslmode=require",
        # ...
    }
}'

# Pipeline using both connections
pipeline_spec = {
    "objects": [
        {
            "table": {
                "source_table": "customers",
                "connection_name": "cockroach_production",  # Database: production
            }
        },
        {
            "table": {
                "source_table": "reports",
                "connection_name": "cockroach_analytics",   # Database: analytics
            }
        }
    ]
}
```

## 📚 **Key Learnings**

1. **Unity Catalog controls database access** at the connection level
2. **Database cannot be overridden** in table configuration
3. **Schema can be overridden** per-table for flexibility
4. **Multiple databases require multiple connections**
5. **This is a security/governance feature**, not a limitation

## 🔗 **Related**

- **Connection Parameters**: [HARDCODED_CONNECTION_BUG.md](HARDCODED_CONNECTION_BUG.md)
- **SSL Configuration**: [SSL_CERTIFICATE_FIX.md](SSL_CERTIFICATE_FIX.md)
- **Spark Serialization**: [SPARK_SERIALIZATION_FIX.md](SPARK_SERIALIZATION_FIX.md)

## 📊 **Final Configuration**

**Pipeline Update ID:** `5dd85d57-9f72-46cf-8780-d933d18bff60`

**Configuration:**
- ✅ Connection database: `ycsb` (fixed)
- ✅ Table schema: `public` (can override per-table)
- ✅ SSL mode: `require`
- ✅ Lazy connection initialization
- ✅ Ready to ingest!

**View Pipeline:** https://e2-dogfood.staging.cloud.databricks.com/pipelines/aa91e2c9-e62b-4fca-bd65-cedb89129265

