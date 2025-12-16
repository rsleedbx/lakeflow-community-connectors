# Remote CockroachCloud Testing

## Overview

The `test_local.py` script now supports testing against remote CockroachDB clusters (CockroachCloud) using the `--url` parameter. This allows you to test the connector against a real production-like environment without needing a local CockroachDB installation.

## Quick Start

### Testing Against CockroachCloud

```bash
python scripts/test_local.py \
  --url "postgresql://user:password@host:port/database?sslmode=verify-full" \
  --no-data
```

**Note:** Use `--no-data` to skip workload generation, as workload commands require the CockroachDB CLI to be configured for remote access.

## Connection URL Format

### Standard Format

```
postgresql://[user][:password]@[host]:[port]/[database][?sslmode=...]
```

### Components

| Component | Description | Example |
|-----------|-------------|---------|
| **user** | Database username | `rslee` |
| **password** | Database password | `W5oTzuQNgpOR2KpQBljdCQ` |
| **host** | Cluster hostname | `battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud` |
| **port** | Port number (usually 26257) | `26257` |
| **database** | Target database | `defaultdb` |
| **sslmode** | SSL/TLS mode | `verify-full`, `require`, `disable` |

### SSL Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **verify-full** | Verify certificate and hostname | CockroachCloud (recommended) |
| **require** | Require SSL but don't verify | Development with SSL |
| **disable** | No SSL (insecure) | Local testing only |

## Examples

### CockroachCloud (Production)

```bash
python scripts/test_local.py \
  --url "postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full" \
  --no-data \
  --duration 30
```

**What it tests:**
- ✅ Connection to remote cluster
- ✅ Rangefeeds enabled check
- ✅ List tables
- ✅ Get schema (with CDC fields)
- ✅ Read metadata
- ✅ Read data (snapshot mode)
- ⚠️ CDC streaming (limited without live data)

### Local CockroachDB (Default)

```bash
# Still works without --url parameter
python scripts/test_local.py --duration 60
```

**Equivalent to:**
```bash
python scripts/test_local.py \
  --url "postgresql://root@localhost:26257/ycsb?sslmode=disable" \
  --duration 60
```

### Self-Hosted CockroachDB with SSL

```bash
python scripts/test_local.py \
  --url "postgresql://admin:securepass@my-crdb-cluster.example.com:26257/mydb?sslmode=require" \
  --no-data
```

## Full Example: CockroachCloud Testing

### 1. Get Connection String from CockroachCloud

1. Log into [CockroachCloud](https://cockroachlabs.cloud/)
2. Select your cluster
3. Click "Connect"
4. Copy the connection string (PostgreSQL format)

Example:
```
postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full
```

### 2. Run Tests

```bash
cd sources/cockroachdb

python scripts/test_local.py \
  --url "postgresql://rslee:W5oTzuQNgpOR2KpQBljdCQ@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full" \
  --no-data \
  --duration 10
```

### 3. Expected Output

```
🔗 Using connection URL:
   Host: battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257
   Database: defaultdb
   User: rslee
   SSL Mode: verify-full

🧹 Cleaning up existing processes...

⚠️  Running tests WITHOUT data generation
   Changefeed tests will likely timeout (this is expected)

============================================================
CockroachDB Connector - Local Testing
============================================================

🔍 Checking rangefeed setting...
✅ Rangefeeds are enabled
🔌 Testing connection to CockroachCloud...
   Host: battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257
   Database: defaultdb
✅ Connection successful!

📋 Listing tables...
✅ Found 5 tables:
   - customers
   - orders
   - products
   ...

📊 Getting schema for 'customers'...
✅ Schema retrieved for 'customers':
   - id: LongType() (nullable: False)
   - name: StringType() (nullable: True)
   - _cdc_key: ArrayType(StringType(), True) (nullable: True)
   - _cdc_updated: StringType() (nullable: True)
   - _cdc_operation: StringType() (nullable: True)

...
```

## Limitations

### Data Generation

**Issue:** CockroachDB workload commands (YCSB, TPC-C, etc.) are **not** run when using `--url` with remote clusters.

**Reason:** The `cockroach workload` command would need to connect to the remote cluster, which requires:
- CockroachDB CLI configured for remote access
- Certificates (if using `sslmode=verify-full`)
- Credentials configured

**Workarounds:**

1. **Use `--no-data` flag** (recommended for quick tests):
   ```bash
   python scripts/test_local.py --url "..." --no-data
   ```
   - Tests connection, schema, metadata
   - CDC streaming will timeout (expected)

2. **Generate data separately** (if you need live CDC testing):
   ```bash
   # From another terminal or machine:
   cockroach workload run ycsb --duration=120s "postgresql://..."
   
   # Then run tests:
   python scripts/test_local.py --url "..." --no-cleanup
   ```

3. **Use existing tables** (if cluster has data):
   ```bash
   python scripts/test_local.py \
     --url "..." \
     --table your_existing_table \
     --no-data
   ```

## Comparison: Local vs Remote Testing

| Feature | Local (`--url` not specified) | Remote (`--url` specified) |
|---------|-------------------------------|----------------------------|
| **Connection** | localhost:26257 | Custom host:port |
| **SSL** | Disabled | Configurable (verify-full, require, disable) |
| **Data Generation** | ✅ Automatic (YCSB, TPC-C, KV, MovR) | ❌ Not supported (`--no-data` required) |
| **CDC Streaming** | ✅ Full testing with live data | ⚠️ Limited (timeouts without data) |
| **Use Case** | Development, full testing | Production validation, remote debugging |
| **Setup** | Requires local CockroachDB | Only needs connection string |

## Security Notes

### Password Handling

**⚠️ WARNING:** Connection URLs contain passwords in plaintext.

**Best Practices:**

1. **Use environment variables:**
   ```bash
   export CRDB_URL="postgresql://user:pass@host:port/db?sslmode=verify-full"
   python scripts/test_local.py --url "$CRDB_URL"
   ```

2. **Don't commit URLs to git:**
   ```bash
   # Create a local script (gitignored)
   cat > test_remote.sh << 'EOF'
   #!/bin/bash
   python scripts/test_local.py \
     --url "postgresql://..." \
     --no-data
   EOF
   chmod +x test_remote.sh
   ```

3. **Use short-lived credentials:**
   - Create a dedicated test user
   - Grant minimal permissions
   - Rotate credentials regularly

4. **Clear shell history:**
   ```bash
   history -d $(history 1)  # Delete last command
   ```

### SSL Certificate Verification

For production clusters, **always use `sslmode=verify-full`**:

```bash
--url "postgresql://user:pass@host:port/db?sslmode=verify-full"
```

Only use `sslmode=disable` for:
- Local testing (localhost)
- Development clusters
- Internal networks

## Troubleshooting

### Connection Refused

**Symptoms:**
```
❌ Connection failed: connection refused
```

**Solutions:**
- Check firewall rules (CockroachCloud IP allowlist)
- Verify cluster is running
- Check port number (usually 26257)

### SSL Errors

**Symptoms:**
```
❌ SSL verification failed
```

**Solutions:**
1. Check SSL mode matches cluster requirements
2. For CockroachCloud: Use `sslmode=verify-full`
3. For self-hosted without certs: Use `sslmode=disable` (not recommended)

### Rangefeeds Not Enabled

**Symptoms:**
```
⚠️  Rangefeeds are NOT enabled
```

**Solution:**
```sql
-- Enable rangefeeds on the cluster
SET CLUSTER SETTING kv.rangefeed.enabled = true;
```

### Authentication Failed

**Symptoms:**
```
❌ password authentication failed
```

**Solutions:**
- Check username/password in URL
- Verify user has permissions on the database
- Check if password contains special characters (URL-encode if needed)

### URL Parsing Errors

**Symptoms:**
```
❌ Failed to parse connection URL
```

**Solutions:**
- Ensure URL format is correct: `postgresql://user:pass@host:port/db?sslmode=...`
- URL-encode special characters in password:
  ```python
  from urllib.parse import quote
  password_encoded = quote("my!pass@word", safe="")
  ```

## Advanced Usage

### Testing Multiple Clusters

```bash
# Test dev cluster
python scripts/test_local.py --url "$DEV_CRDB_URL" --no-data

# Test staging cluster
python scripts/test_local.py --url "$STAGING_CRDB_URL" --no-data

# Test prod cluster (read-only user recommended)
python scripts/test_local.py --url "$PROD_CRDB_URL" --no-data
```

### Custom Table Testing

```bash
python scripts/test_local.py \
  --url "postgresql://..." \
  --table my_custom_table \
  --no-data
```

### With Diagnostic Mode

```bash
python scripts/test_local.py \
  --url "postgresql://..." \
  --diagnostic \
  --no-data
```

Runs `test_changefeed_direct.py` first to verify changefeed functionality.

## Implementation Details

### Code Changes

The following functions were updated to use dynamic connection parameters:

1. **`CONNECTION_PARAMS`** - Global dict storing connection info
2. **`parse_postgres_url(url)`** - Parses PostgreSQL URLs
3. **`build_connection_url(database)`** - Builds URLs for workloads
4. **`get_workload_config()`** - Dynamic workload config
5. **`check_rangefeeds_enabled()`** - Uses `CONNECTION_PARAMS`
6. **`test_connection()`** - Uses `CONNECTION_PARAMS`
7. **CDC tests** - Use `CONNECTION_PARAMS`

### Backward Compatibility

✅ **Fully backward compatible** - existing scripts work without changes:

```bash
# Old way (still works):
python scripts/test_local.py --duration 60

# New way (same result for localhost):
python scripts/test_local.py \
  --url "postgresql://root@localhost:26257/ycsb?sslmode=disable" \
  --duration 60
```

## Summary

The `--url` parameter enables testing the CockroachDB connector against:
- ✅ CockroachCloud clusters
- ✅ Self-hosted CockroachDB clusters
- ✅ Remote clusters with SSL
- ✅ Multiple databases on the same cluster

**Use cases:**
- Validate connector against production-like environments
- Debug issues on specific clusters
- Test SSL/TLS configurations
- Verify rangefeeds on remote clusters

**Recommended for:**
- Production readiness validation
- Performance testing on real clusters
- Debugging customer issues

**Not recommended for:**
- Full CDC streaming tests (use local for that)
- Workload generation (requires local or CLI configuration)

