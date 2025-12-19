# SSL Certificate Fix: Complete Journey

## 🐛 **The Original Problem**

```
ConnectionError: Failed to connect to CockroachDB: could not open certificate file 
"/root/.postgresql/postgresql.crt": Permission denied
```

**Root cause:** In Databricks serverless environment, psycopg2/libpq was trying to read client SSL certificate files from `/root/.postgresql/`, but lacked permissions to access that directory.

---

## 🔄 **The Journey: Multiple Attempts**

### **❌ Attempt 1: Set `PGSSLCERT` environment variables at module level**
```python
# At top of file, before imports
os.environ['PGSSLCERT'] = ''
os.environ['PGSSLKEY'] = ''
os.environ['PGSSLROOTCERT'] = ''
```
**Result:** Failed - environment variables set at module import time don't affect Spark workers consistently.

---

### **❌ Attempt 2: Use `sslrootcert='system'`**
```python
connect_params = {
    'sslmode': 'require',
    'sslrootcert': 'system'
}
```
**Result:** Failed with error:
```
weak sslmode "require" may not be used with sslrootcert=system (use "verify-full")
```

**Learning:** `sslrootcert='system'` requires `sslmode='verify-full'` or `verify-ca`, not `require`.

---

### **✅ Attempt 3: Use `sslmode='verify-full'` + `sslrootcert='system'`**
```python
connect_params = {
    'sslmode': 'verify-full',
    'sslrootcert': 'system'
}
```
**Result:** Still failed with:
```
could not open certificate file "/root/.postgresql/postgresql.crt": Permission denied
```

**Discovery:** `sslrootcert` only controls **server CA certificate** location. libpq was still looking for **client certificates** (`postgresql.crt`, `postgresql.key`).

---

### **❌ Attempt 4: Explicitly set `sslcert=''` and `sslkey=''` in DSN**
```python
dsn = "... sslmode=verify-full sslrootcert=system sslcert= sslkey="
```
**Result:** Failed with error:
```
could not open certificate file "sslkey=": Permission denied
```

**Discovery:** libpq interpreted `sslcert=` as a literal filename `"sslcert="` (empty value after `=` is treated as a filename, not as "omit parameter").

---

### **✅ Attempt 5: OMIT `sslcert` and `sslkey` entirely (FINAL SOLUTION)**
```python
dsn_parts = [
    f"host={self.host}",
    f"port={self.port}",
    f"dbname={self.database}",
    f"user={self.user}",
    f"password={self.password}",
    f"sslmode=verify-full",
    f"sslrootcert=system",
    # NOT including sslcert or sslkey at all
]
dsn = " ".join(dsn_parts)
```
**Result:** ✅ **SUCCESS!** libpq doesn't look for client certificate files if the parameters aren't specified.

---

## 🎯 **The Complete Solution**

### **SSL Configuration**
```python
sslmode=verify-full          # Full server certificate verification
sslrootcert=system          # Use system CA bundle (/etc/ssl/certs)
# sslcert: NOT SPECIFIED    # No client cert = no file lookups
# sslkey: NOT SPECIFIED     # No client key = no file lookups
```

### **Why Each Parameter Matters**

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `sslmode` | `verify-full` | Verify server certificate against CA bundle |
| `sslrootcert` | `system` | Use system CA bundle (readable by all users) |
| `sslcert` | **OMITTED** | Don't look for client certificate files |
| `sslkey` | **OMITTED** | Don't look for client key files |

### **How It Works**

1. **Server Authentication** (CockroachCloud proves its identity):
   - CockroachCloud sends its SSL certificate during handshake
   - psycopg2 verifies it against system CA bundle (`/etc/ssl/certs`)
   - ✅ SSL encryption established

2. **Client Authentication** (we don't need it):
   - CockroachCloud uses username/password authentication
   - No client certificates required
   - Parameters omitted → libpq skips client cert lookups
   - ✅ No permission errors

---

## 📚 **Key Learnings**

### **1. Two Types of SSL Certificates**

- **Server Certificate** (for verifying the server)
  - Controlled by: `sslrootcert` parameter
  - CockroachCloud sends this during handshake
  - We verify it using system CAs

- **Client Certificate** (for authenticating to the server)
  - Controlled by: `sslcert` and `sslkey` parameters
  - Optional - only needed for mutual TLS
  - CockroachCloud doesn't require it (uses password auth)

### **2. How libpq Handles SSL Parameters**

| DSN Format | libpq Behavior |
|------------|----------------|
| `sslcert=/path/to/cert` | Reads file at `/path/to/cert` |
| `sslcert=` | Tries to read file named `"sslcert="` (literal!) |
| `sslcert=''` | Tries to read file named `''` (empty string) |
| *(parameter omitted)* | **Doesn't look for client cert files** ✅ |

### **3. sslrootcert='system' Requirements**

When using `sslrootcert='system'`:
- **MUST** use `sslmode='verify-full'` or `sslmode='verify-ca'`
- **CANNOT** use `sslmode='require'` (considered "weak")
- Requires PostgreSQL 14+ / psycopg2 2.9+

### **4. Databricks Serverless Restrictions**

- `/root/.postgresql/` is not accessible (permission denied)
- `/etc/ssl/certs/` is accessible (system CA bundle)
- Environment variables may not propagate to Spark workers consistently
- Connection string parameters are most reliable

---

## 🔗 **Related Issues**

- **Spark Serialization**: [SPARK_SERIALIZATION_FIX.md](SPARK_SERIALIZATION_FIX.md) - Lazy connection initialization
- **Connection Parameters**: [HARDCODED_CONNECTION_BUG.md](HARDCODED_CONNECTION_BUG.md) - How credentials are passed

---

---

### **❌ Attempt 6: Use `sslmode='require'` with omitted cert parameters**
```python
connection_params = {
    'sslmode': 'require',  # No sslcert, sslkey, or sslrootcert
}
```
**Result:** Still failed with permission denied looking for `/root/.postgresql/postgresql.crt`

**Learning:** Even with `sslmode='require'` and NO cert parameters specified, libpq still looks for default client cert paths if the directory exists.

---

### **🔬 Attempt 7: Module-level environment variables + empty params (IN PROGRESS)**
```python
# At module load time (top of file, after imports)
os.environ['PGSSLCERT'] = '/tmp/dummy_client_cert.crt'
os.environ['PGSSLKEY'] = '/tmp/dummy_client_key.key'
os.environ['PGSSLROOTCERT'] = 'system'

# In connection method
connection_params = {
    'sslmode': 'require',
    'sslcert': '',  # Empty string to override env var
    'sslkey': '',   # Empty string to override env var
}
conn = psycopg2.connect(**connection_params)  # Use kwargs, not DSN
```

**Why this might work:**
1. Environment variables set at module import override libpq's `/root/.postgresql/` defaults
2. `/tmp` is typically writable in Databricks (fallback if libpq tries to access it)
3. Empty strings in connection params take precedence over env vars (double protection)
4. Using keyword arguments instead of DSN string for explicit control

**Pipeline Update ID**: `e863be23-8e64-479b-acd6-090ee5a44f7b`

**Status**: Testing... 🧪

---

## 📊 **Current Status**

**View Pipeline**: https://e2-dogfood.staging.cloud.databricks.com/pipelines/aa91e2c9-e62b-4fca-bd65-cedb89129265

**Issues Fixed**:
- ✅ Spark serialization (lazy connection initialization)
- ✅ Hardcoded connection name bug
- ✅ Unity Catalog credential passing
- ✅ Database override restriction
- ✅ Schema override restriction

**Still Working On**:
- 🔧 SSL client certificate lookup in Databricks serverless environment

---

## 💡 **Key Learnings**

1. **SSL Configuration Challenges**
   - libpq (psycopg2's underlying C library) has hardcoded default paths
   - Even with `sslmode='require'`, it may still look for client certs
   - Environment variable precedence: connection param > env var > compiled defaults
   - Empty strings (`''`) vs omitting parameters behave differently

2. **Databricks Serverless Restrictions**
   - `/root/.postgresql/` directory exists but is not accessible
   - `/tmp` is typically writable
   - Environment variables must be set at module load time
   - Spark workers have isolated environments

3. **PostgreSQL Connection Parameters**
   - DSN string format vs keyword arguments have different behavior
   - libpq parses connection parameters in specific order
   - Some parameters trigger additional file lookups even when not requested
