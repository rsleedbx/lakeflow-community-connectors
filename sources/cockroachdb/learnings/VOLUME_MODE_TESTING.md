# CockroachDB Volume Mode Testing Guide

**Modeled after**: Direct Mode testing pattern  
**Date**: December 19, 2025  
**Status**: Ready for Testing

---

## Overview

Volume mode allows `cockroachdb.py` to read pre-synced Parquet files from Unity Catalog Volumes instead of connecting to the database directly.

**Benefits:**
- ✅ No database credentials needed
- ✅ Unity Catalog governance
- ✅ Pre-synced data (faster ingestion)
- ✅ File-based cursor (exactly-once processing)

---

## Prerequisites

### 1. Volume Must Have Parquet Files

```bash
# Sync files from Azure to Volume first
cd sources/cockroachdb_s3/scripts
./sync_azure_to_volume.sh
```

**Verify files:**
```bash
databricks fs ls dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/
```

Expected: 11+ Parquet files

### 2. Databricks CLI Configured

```bash
databricks configure --token
# Or use existing configuration
```

---

## Testing Pattern (Modeled After Direct Mode)

### Direct Mode Pattern:
```bash
./copydir.sh                     # Deploy connector
./create_databricks_connection.sh   # Create connection
./createpipeline.sh             # Create pipeline
./monitor_pipeline.sh           # Monitor
```

### Volume Mode Pattern:
```bash
./copydir.sh                    # Deploy connector (same)
# No connection needed!         # Skip connection step
./create_volume_pipeline.sh    # Create Volume pipeline
./monitor_pipeline.sh          # Monitor (same)
```

---

## Step-by-Step Testing

### Option A: Complete Setup (Recommended)

Single command that does everything:

```bash
cd sources/cockroachdb/scripts
./setup_volume_pipeline.sh
```

**This script:**
1. ✅ Verifies Volume has Parquet files
2. 📤 Deploys connector to Databricks
3. 🔧 Creates Volume mode pipeline
4. ▶️ Starts pipeline (optional)
5. 📊 Monitors progress

### Option B: Manual Step-by-Step

```bash
cd sources/cockroachdb/scripts

# Step 1: Deploy connector
./copydir.sh

# Step 2: Create Volume pipeline
./create_volume_pipeline.sh

# Step 3: Start pipeline
databricks pipelines start-update <pipeline-id> --full-refresh

# Step 4: Monitor
./monitor_pipeline.sh robert_lee_cockroachdb_volume main robert_lee_cockroachdb
```

---

## Pipeline Configuration

### Direct Mode Config:
```json
{
  "connection_name": "robert_lee_battle-walrus-11108",
  "source_name": "cockroachdb",
  "table_list": "usertable"
}
```

### Volume Mode Config:
```json
{
  "volume_path": "/Volumes/main/robert_lee_cockroachdb/parquet_files",
  "source_name": "cockroachdb",
  "table_list": "usertable"
}
```

**Key Difference:** `volume_path` instead of `connection_name`

---

## Expected Behavior

### Connector Initialization:
```
════════════════════════════════════════════════════════
🎯 OPERATION MODE: Volume (Databricks Native)
════════════════════════════════════════════════════════
  Data Source: Unity Catalog Volume
  Volume Path: /Volumes/main/robert_lee_cockroachdb/parquet_files
  Format: Parquet files
  Benefits: No external credentials, UC governance, pre-synced data
════════════════════════════════════════════════════════
```

### Pipeline Processing:
1. Lists Parquet files in Volume
2. Sorts by filename (timestamp order)
3. Processes files > cursor
4. Extracts CDC data from Parquet
5. Writes to Delta tables
6. Updates cursor

### Output Tables:
- `main.robert_lee_cockroachdb.usertable` - Final table
- `main.robert_lee_cockroachdb.usertable_cdc` - CDC events (optional)

---

## Monitoring

### Using monitor_pipeline.sh:
```bash
./monitor_pipeline.sh robert_lee_cockroachdb_volume main robert_lee_cockroachdb
```

**Shows:**
- Pipeline status
- Event counts
- Latest update time
- Error messages (if any)

### Manual Monitoring:
```bash
# Get pipeline status
databricks pipelines get robert_lee_cockroachdb_volume

# Get pipeline events
databricks pipelines list-pipeline-events robert_lee_cockroachdb_volume --max-results 50

# Query results
databricks sql execute --warehouse-id <warehouse-id> \
  --statement "SELECT COUNT(*) FROM main.robert_lee_cockroachdb.usertable"
```

---

## Verification

### 1. Check Row Count:
```sql
SELECT COUNT(*) FROM main.robert_lee_cockroachdb.usertable;
```

### 2. View Sample Data:
```sql
SELECT * FROM main.robert_lee_cockroachdb.usertable LIMIT 10;
```

### 3. Check CDC Metadata:
```sql
SELECT 
  _cdc_key,
  _cdc_updated,
  _cdc_operation,
  _source_file
FROM main.robert_lee_cockroachdb.usertable
LIMIT 10;
```

### 4. Verify Cursor:
The cursor tracks the last processed filename. Check pipeline configuration or DLT checkpoint.

---

## Comparison: Direct vs Volume Mode

| Aspect | Direct Mode | Volume Mode |
|--------|-------------|-------------|
| **Setup** | `copydir.sh` + `create_databricks_connection.sh` + `createpipeline.sh` | `copydir.sh` + `create_volume_pipeline.sh` |
| **Connection** | UC Connection required | No connection needed |
| **Data Source** | Live database | Pre-synced Parquet files |
| **Monitoring** | `monitor_pipeline.sh` | `monitor_pipeline.sh` (same!) |
| **Real-time** | Yes | No (batch on synced files) |
| **Use Case** | Real-time CDC | Pre-exported data, no DB access |

---

## Troubleshooting

### No Files in Volume
```bash
# Check Volume contents
databricks fs ls dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/

# If empty, sync from Azure
cd ../../cockroachdb_s3/scripts
./sync_azure_to_volume.sh
```

### Pipeline Creation Fails
```bash
# Check connector is deployed
databricks workspace ls /Workspace/Repos/<user>/lakeflow-community-connectors/sources/cockroachdb/

# Re-deploy if needed
./copydir.sh
```

### Pipeline Fails During Execution
```bash
# Get detailed errors
databricks pipelines list-pipeline-events robert_lee_cockroachdb_volume --max-results 50

# Check connector logs in pipeline UI
```

### No Data in Output Tables
```bash
# Verify pipeline completed
databricks pipelines get robert_lee_cockroachdb_volume

# Check for errors
./monitor_pipeline.sh robert_lee_cockroachdb_volume main robert_lee_cockroachdb
```

---

## Files Reference

### New Volume Mode Scripts:
- `create_volume_pipeline.sh` - Create Volume mode pipeline
- `setup_volume_pipeline.sh` - Complete setup script

### Shared with Direct Mode:
- `copydir.sh` - Deploy connector
- `monitor_pipeline.sh` - Monitor any pipeline
- `ingest.py` - DLT notebook (handles both modes)

### Connector:
- `cockroachdb.py` - Dual/triple mode connector
  - Detects `volume_path` → Volume mode
  - Detects Azure credentials → Azure Parquet mode
  - Default → Direct mode

---

## Next Steps

1. **Run Complete Setup:**
   ```bash
   cd sources/cockroachdb/scripts
   ./setup_volume_pipeline.sh
   ```

2. **Monitor Progress:**
   ```bash
   ./monitor_pipeline.sh robert_lee_cockroachdb_volume main robert_lee_cockroachdb
   ```

3. **Verify Results:**
   ```sql
   SELECT * FROM main.robert_lee_cockroachdb.usertable LIMIT 10;
   ```

4. **Compare with Direct Mode:**
   - Run both pipelines
   - Compare row counts
   - Verify data consistency

---

## Success Criteria

✅ Pipeline creates successfully  
✅ Reads all 11 Parquet files from Volume  
✅ Processes events without errors  
✅ Creates output table with data  
✅ Cursor advances through files  
✅ Can query results in catalog  

---

**Ready to test? Run:**
```bash
cd sources/cockroachdb/scripts
./setup_volume_pipeline.sh
```

