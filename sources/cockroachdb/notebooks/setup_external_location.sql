-- ============================================================================
-- Unity Catalog External Location Setup for CockroachDB CDC
-- ============================================================================
-- Run this in a Databricks SQL notebook or SQL editor
-- Prerequisites: Unity Catalog enabled, metastore admin privileges
-- ============================================================================

-- Step 1: Get your Azure storage key
-- Run this locally first:
-- cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env
-- cat cockroachdb_cdc_azure.json | jq -r '.azure_storage_key'
-- Then paste the key below

-- ============================================================================
-- CONFIGURATION - Update these values
-- ============================================================================
-- Replace <YOUR_STORAGE_KEY> with the actual key from cockroachdb_cdc_azure.json
-- Replace <YOUR_EMAIL> with your Databricks user email

-- ============================================================================
-- Step 1: Create Storage Credential
-- ============================================================================

CREATE STORAGE CREDENTIAL IF NOT EXISTS cockroachdb_cdc_storage_credential
WITH (
  AZURE_STORAGE_ACCOUNT_NAME 'cockroachcdc1766504458',
  AZURE_STORAGE_ACCOUNT_KEY '<YOUR_STORAGE_KEY>'  -- ⚠️ REPLACE THIS
)
COMMENT 'Azure Blob Storage credentials for CockroachDB CDC changefeeds';

-- Verify creation
DESCRIBE STORAGE CREDENTIAL cockroachdb_cdc_storage_credential;

-- ============================================================================
-- Step 2: Create External Locations
-- ============================================================================

-- External location for Parquet CDC files
CREATE EXTERNAL LOCATION IF NOT EXISTS cockroachdb_cdc_parquet
URL 'wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_storage_credential)
COMMENT 'CockroachDB CDC Parquet changefeeds from Azure Blob Storage';

-- External location for JSON CDC files
CREATE EXTERNAL LOCATION IF NOT EXISTS cockroachdb_cdc_json
URL 'wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/json-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_storage_credential)
COMMENT 'CockroachDB CDC JSON changefeeds from Azure Blob Storage';

-- Verify creation
SHOW EXTERNAL LOCATIONS LIKE 'cockroachdb_cdc%';

-- ============================================================================
-- Step 3: Grant Permissions
-- ============================================================================

-- Grant to your user (replace with your email)
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `<YOUR_EMAIL>`;  -- ⚠️ REPLACE THIS
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_json TO `<YOUR_EMAIL>`;     -- ⚠️ REPLACE THIS

-- Optional: Grant to a group (uncomment and modify if needed)
-- GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `data-engineers`;
-- GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_json TO `data-engineers`;

-- Optional: Grant to all workspace users (use with caution)
-- GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `account users`;

-- ============================================================================
-- Step 4: Verify Setup
-- ============================================================================

-- Check external locations
DESCRIBE EXTERNAL LOCATION cockroachdb_cdc_parquet;
DESCRIBE EXTERNAL LOCATION cockroachdb_cdc_json;

-- Check permissions
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_parquet;
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_json;

-- ============================================================================
-- Step 5: Test Access (Run in Python notebook)
-- ============================================================================

-- After setting up external locations, test in a Python notebook:
-- 
-- # Test listing files
-- files = dbutils.fs.ls("wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc/")
-- print(f"Found {len(files)} files")
-- 
-- # Test reading Parquet
-- df = spark.read.parquet("wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc/")
-- print(f"Successfully read {df.count()} rows")

-- ============================================================================
-- Optional: Cleanup Commands (if you need to start over)
-- ============================================================================

-- DROP EXTERNAL LOCATION IF EXISTS cockroachdb_cdc_parquet;
-- DROP EXTERNAL LOCATION IF EXISTS cockroachdb_cdc_json;
-- DROP STORAGE CREDENTIAL IF EXISTS cockroachdb_cdc_storage_credential;

-- ============================================================================
-- SUCCESS!
-- ============================================================================
-- Next step: Update your load_parquet_files.ipynb notebook to use the external location
-- See: SETUP_UNITY_CATALOG_EXTERNAL_LOCATION.md for detailed instructions





