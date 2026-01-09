# CockroachDB Connector Unit Tests

This directory contains unit tests for the CockroachDB connector that can be run locally or in Databricks.

## Prerequisites

**Python Dependencies (Required):**
```bash
pip install GitPython      # For finding git root directory
pip install python-dotenv  # For loading .env files
pip install pg8000         # CockroachDB PostgreSQL driver
```

**Install all at once:**
```bash
pip install GitPython python-dotenv pg8000
```

## Quick Start

1. **Install dependencies:**
   
   **Option A: Using requirements.txt (recommended)**
   ```bash
   pip install -r requirements.txt
   ```

   **Option B: Manual installation**
   ```bash
   pip install GitPython python-dotenv pg8000
   ```

2. **Copy the credentials template:**
   ```bash
   cp ../test_credentials.json.template ../test_credentials.json
   ```

3. **Edit credentials:**
   ```bash
   # Edit with your actual CockroachDB and Azure credentials
   vi ../test_credentials.json
   ```

4. **Run the notebook:**
   - **In Databricks:** Upload `cockroachdb.ipynb` to your workspace and run all cells
   - **Locally:** Open in Jupyter with PySpark installed

## Credentials File Structure

The `test_credentials.json` file should contain credentials for each mode you want to test:

```json
{
  "direct_mode": {
    "host": "your-cluster.cockroachlabs.cloud",
    "port": "26257",
    "database": "defaultdb",
    "user": "your_user",
    "password": "your_password",
    "sslmode": "require",
    "schema": "public"
  },
  "azure_parquet_mode": {
    "host": "your-cluster.cockroachlabs.cloud",
    "port": "26257",
    "database": "defaultdb",
    "user": "your_user",
    "password": "your_password",
    "sslmode": "require",
    "schema": "public",
    "azure_account_name": "your_storage_account",
    "azure_account_key": "your_storage_key",
    "azure_container": "changefeed-events",
    "azure_path_prefix": "parquet-test"
  },
  "volume_mode": {
    "volume_path": "/Volumes/main/your_catalog/parquet_files",
    "schema": "public"
  }
}
```

## Test Modes

### 1. Direct Mode
- **What it tests:** Direct database connection using sinkless changefeed
- **Requirements:** CockroachDB credentials
- **Use case:** Testing real-time CDC without external storage

### 2. Azure Parquet Mode
- **What it tests:** Reading from Azure Blob Storage Parquet files
- **Requirements:** CockroachDB credentials + Azure storage credentials
- **Use case:** Testing changefeed sink to Azure → read back

### 3. Volume Mode (Databricks Only)
- **What it tests:** Reading from Unity Catalog Volume
- **Requirements:** Databricks runtime with Volume access
- **Use case:** Testing pre-synced files in UC Volume

## What Gets Tested

Each mode tests:
- ✅ Connector initialization
- ✅ `read_table()` method
- ✅ Row retrieval and formatting
- ✅ Offset/cursor tracking
- ✅ CDC metadata (_cdc_key, _cdc_updated, _cdc_operation)

## Example Output

```
================================================================================
TEST 1: Direct Mode Initialization
================================================================================

📋 Options:
  host: battle-walrus-11108.b02rs8i.gcp-us-central1.cockroachlabs.cloud
  port: 26257
  database: defaultdb
  user: robert.lee
  password: ***REDACTED***
  sslmode: require
  schema: public

✅ Connector initialized successfully!
   Mode: direct
   Host: battle-walrus-11108.b02rs8i.gcp-us-central1.cockroachlabs.cloud
   Database: defaultdb
   Schema: public

================================================================================
TEST 1: Direct Mode - read_table()
================================================================================

📖 Reading table: usertable
   Start offset: {}
   Options: {'initial_scan': 'only'}

✅ Successfully read 10 rows (limited to 10 for testing)

📊 End offset: {'cursor': 'eyJmaWx0ZXIiOiJGQUxTRSIsImZ...'}

📋 First row:
{
  "ycsb_key": "user1234567890",
  "field0": "value0",
  "field1": "value1",
  "_cdc_key": "[\"user1234567890\"]",
  "_cdc_updated": "2024-12-22 10:30:00.123456",
  "_cdc_operation": "INSERT"
}
```

## Troubleshooting

### `ModuleNotFoundError: No module named 'cockroachdb'`
- The notebook automatically adds the parent directory to `sys.path`
- Ensure `cockroachdb.py` exists in the parent directory

### `ModuleNotFoundError: No module named 'pg8000'`
- Install: `pip install pg8000>=1.30.0`
- Or in Databricks: Add to cluster libraries

### Volume Mode fails with "dbutils not available"
- Volume mode requires Databricks runtime
- Cannot run locally in Jupyter

### Azure Parquet Mode: `INSUFFICIENT_PERMISSIONS`
- Ensure storage key is correct in credentials
- Check Azure storage account exists and is accessible
- May require Unity Catalog External Location setup

## Security Note

⚠️ **NEVER commit `test_credentials.json` to version control!**
- The template file (`.template`) is safe to commit
- The actual credentials file is gitignored
- Always double-check before committing

## Integration with CI/CD

For automated testing:
1. Store credentials in secure vault (e.g., GitHub Secrets, Azure Key Vault)
2. Inject credentials at test runtime
3. Use Databricks Jobs API to run notebook tests
4. Parse output for pass/fail status

## Related Files

- `../cockroachdb.py` - The connector implementation
- `../test_credentials.json.template` - Credentials template
- `../TESTING_GUIDE.md` - Full DLT pipeline testing guide
- `../VOLUME_MODE.md` - Volume mode documentation

