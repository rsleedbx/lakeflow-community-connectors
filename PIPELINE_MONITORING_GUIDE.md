# Pipeline Monitoring Quick Reference

## 🚀 Quick Start

```bash
cd sources/cockroachdb/scripts
chmod +x monitor_pipeline.sh
./monitor_pipeline.sh <pipeline_name> <catalog> <schema>
```

## 📋 What the Script Does

1. **Gets Pipeline ID** from pipeline name
2. **Starts Full Refresh** with `--full-refresh` flag
3. **Captures Update ID** from the start response
4. **Polls Status** every 10 seconds
5. **Shows Final Metrics** when complete:
   - Duration
   - Input/Output rows
   - Input bytes
6. **Queries Table Counts** to verify data
7. **Reports Errors/Warnings** if any

## 🎯 Example Usage

```bash
# For cockroachdb connector
./sources/cockroachdb/scripts/monitor_pipeline.sh \
  robert_lee_cockroachdb \
  main \
  robert_lee_cockroachdb_cdc

# For cockroachdb_s3 connector (when implemented)
./sources/cockroachdb_s3/scripts/monitor_pipeline.sh \
  robert_lee_cockroachdb_cdc \
  main \
  cdc
```

## 📊 Expected Output

```
🚀 CockroachDB Pipeline Monitor
════════════════════════════════════════════════════════
   Pipeline: robert_lee_cockroachdb
   Catalog: main
   Schema: robert_lee_cockroachdb_cdc
════════════════════════════════════════════════════════

📋 Step 1: Getting pipeline ID...
   ✅ Pipeline ID: 12345-abcde-67890

🔄 Step 2: Starting full refresh...
   ✅ Update ID: update-98765
   ✅ Started at: 2025-12-19 12:00:00

⏳ Step 3: Monitoring progress...
────────────────────────────────────────────────────────
   [12:00:05] [ 5s] State: QUEUED         
   [12:00:15] [15s] State: INITIALIZING   
   [12:00:25] [25s] State: RUNNING        
   [12:02:35] [155s] State: COMPLETED      

✅ Pipeline completed successfully!

📊 Step 4: Final Metrics
════════════════════════════════════════════════════════
   Duration:            155s
   Input Rows:          110000
   Output Rows:         10000
   Input Bytes:         25000000

📋 Step 5: Table Row Counts
════════════════════════════════════════════════════════
   usertable_raw_events:                      110000 rows
   usertable_coalesced:                        10000 rows
   usertable:                                  10000 rows

════════════════════════════════════════════════════════
✅ Monitoring Complete!

📊 Summary:
   Pipeline: robert_lee_cockroachdb
   Update ID: update-98765
   Duration: 155s
   Output Rows: 10000
   Status: COMPLETED

💡 View in UI:
   https://YOUR_WORKSPACE.databricks.com#joblist/pipelines/12345/updates/98765
```

## 🔍 Manual Commands

### Get Pipeline ID

```bash
PIPELINE_ID=$(databricks pipelines get \
  --pipeline-name "robert_lee_cockroachdb" \
  --output json | jq -r '.pipeline_id')
```

### Start Full Refresh

```bash
UPDATE_ID=$(databricks pipelines start-update \
  --pipeline-id "$PIPELINE_ID" \
  --full-refresh \
  --output json | jq -r '.update_id')
```

### Check Update Status

```bash
databricks pipelines get-update \
  --pipeline-id "$PIPELINE_ID" \
  --update-id "$UPDATE_ID" \
  --output json | jq -r '.update.state'
```

### Get Full Update Details

```bash
databricks pipelines get-update \
  --pipeline-id "$PIPELINE_ID" \
  --update-id "$UPDATE_ID" \
  --output json | jq '{
    state: .update.state,
    duration: ((.update.completion_time // now) - .update.start_time) / 1000,
    input_rows: .update.metrics.num_input_rows,
    output_rows: .update.metrics.num_output_rows
  }'
```

### Query Table Row Counts

```bash
WAREHOUSE_ID=$(databricks warehouses list --output json | jq -r '.[0].id')

databricks sql query \
  --warehouse-id "$WAREHOUSE_ID" \
  --statement "SELECT COUNT(*) FROM main.schema.table_name"
```

### Get Error Logs

```bash
databricks pipelines get-update \
  --pipeline-id "$PIPELINE_ID" \
  --update-id "$UPDATE_ID" \
  --output json | jq -r '.update.events[] | select(.level == "ERROR") | .message'
```

## 🐛 Debugging Commands

### Compare Raw vs Coalesced Row Counts

```bash
databricks sql query \
  --warehouse-id "$WAREHOUSE_ID" \
  --statement "
    SELECT 'raw' as stage, COUNT(*) as cnt 
    FROM main.schema.table_raw_events
    UNION ALL
    SELECT 'coalesced', COUNT(*) 
    FROM main.schema.table_coalesced
    UNION ALL
    SELECT 'final', COUNT(*) 
    FROM main.schema.table_name
  "
```

**Expected for 10K rows with 11 column families:**
- raw: ~110,000 (10,000 × 11)
- coalesced: ~10,000
- final: ~10,000

### Check CockroachDB Changefeed Status

```bash
cockroach sql $COCKROACHDB_URL -e "
  SELECT 
    job_id, 
    status, 
    running_status, 
    error 
  FROM [SHOW JOBS] 
  WHERE job_type = 'CHANGEFEED' 
  ORDER BY created DESC 
  LIMIT 5;
"
```

### View Recent Pipeline Runs

```bash
databricks pipelines list-updates \
  --pipeline-id "$PIPELINE_ID" \
  --output json | jq -r '.[] | "\(.update_id): \(.state) (\(.metrics.num_output_rows // 0) rows)"'
```

## ⚠️ Common Issues

### Issue: "No rows were output"

**Symptoms:**
- `num_output_rows: null` or `0`
- Table count is 0

**Debug:**
```bash
# Check if raw events were captured
databricks sql query --warehouse-id "$WAREHOUSE_ID" \
  --statement "SELECT COUNT(*) FROM main.schema.table_raw_events"

# Check coalescing logic
databricks sql query --warehouse-id "$WAREHOUSE_ID" \
  --statement "SELECT * FROM main.schema.table_coalesced LIMIT 5"
```

### Issue: Row count mismatch

**Symptoms:**
- Source has 10K rows, target has 11K or 1K

**Debug:**
```bash
# Check for duplicates
databricks sql query --warehouse-id "$WAREHOUSE_ID" \
  --statement "
    SELECT ycsb_key, COUNT(*) as cnt 
    FROM main.schema.table_name 
    GROUP BY ycsb_key 
    HAVING cnt > 1
  "

# Check if coalescing is working
databricks sql query --warehouse-id "$WAREHOUSE_ID" \
  --statement "
    SELECT 
      ycsb_key,
      field0,
      field1,
      _cdc_updated
    FROM main.schema.table_coalesced 
    WHERE field0 IS NULL OR field1 IS NULL 
    LIMIT 10
  "
```

### Issue: Pipeline hangs/timeouts

**Symptoms:**
- Pipeline stuck in RUNNING state for > 10 minutes
- No progress in logs

**Debug:**
```bash
# Check if changefeed is producing events
az storage blob list \
  --account-name "$AZURE_STORAGE_ACCOUNT" \
  --account-key "$AZURE_STORAGE_KEY" \
  --container-name "$AZURE_STORAGE_CONTAINER" \
  --output table

# Check CockroachDB changefeed job
cockroach sql $COCKROACHDB_URL -e "
  SELECT 
    job_id,
    status,
    running_status
  FROM [SHOW JOBS]
  WHERE job_type = 'CHANGEFEED'
  AND status IN ('running', 'pending')
"
```

## 📁 Cursor Rules Integration

This monitoring workflow is documented in `.cursorrules_cockroachdb_pipeline`.

When you ask me to "run the pipeline", I will:
1. Use `monitor_pipeline.sh` or equivalent commands
2. Start with `--full-refresh`
3. Poll for status
4. Report final metrics
5. Check table row counts
6. Provide debugging commands if issues found

## 🎯 Success Criteria

A successful pipeline run should show:

✅ State: `COMPLETED`  
✅ Duration: < 5 minutes for 10K rows  
✅ Output Rows: Matches expected count  
✅ No ERROR level events  
✅ Table row count matches source  
✅ For split column families: `coalesced_rows ≈ raw_rows / 11`

## 📚 Related Documentation

- `.cursorrules_cockroachdb_pipeline` - Full cursor rules
- `sources/cockroachdb/STATUS.md` - Development status
- `sources/cockroachdb/CDC_SETUP_SUCCESS.md` - CDC setup guide
- `sources/cockroachdb_s3/AUTOLOADER_APPROACH.md` - Autoloader approach
- `sources/cockroachdb_s3/DATABRICKS_VOLUMES_APPROACH.md` - Volumes approach








