# Lakeflow CockroachDB Community Connector

This documentation provides setup instructions and reference information for the CockroachDB source connector using Change Data Capture (CDC) via sinkless changefeeds.

## Directory Structure

```
sources/cockroachdb/
├── cockroachdb.py              # Main connector implementation
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── cockroachdb_api_doc.md      # API documentation
├── configs/                    # Configuration examples
│   ├── example_config.json
│   ├── dev_config.json
│   └── dev_table_config.json
├── scripts/                    # Test and setup scripts
│   ├── README.md               # Scripts documentation
│   ├── local_setup.sh          # Setup local CockroachDB
│   ├── test_local.py           # Main test script ⭐
│   ├── test_changefeed_direct.py
│   ├── check_setup.sh          # Diagnostic tool
│   ├── enable_rangefeeds.sh    # Enable CDC
│   └── run_pytest.sh
├── test/                       # Pytest test suite
│   └── test_cockroachdb_lakeflow_connect.py
└── learnings/                  # Technical documentation
    ├── README.md               # Documentation index
    ├── WORKLOAD_TESTING_SUMMARY.md
    ├── TEST_REPORT_CDC.md
    └── ... (bug fixes, features, optimizations)
```

## Quick Start Guide

**Complete setup in 3 commands:**

```bash
cd sources/cockroachdb

# 1. Install dependencies
pip install -r requirements.txt

# 2. Start CockroachDB cluster with test data (includes rangefeed enablement)
./scripts/local_setup.sh start

# 3. Run tests (auto-generates live data for 120 seconds using fast YCSB workload)
python scripts/test_local.py
```

**What happens:**
- The test script automatically starts CockroachDB's built-in YCSB workload (~5,000 ops/sec)
- Generates realistic INSERT/UPDATE/DELETE operations for 120 seconds
- Tests run against live streaming changefeeds
- Data generator stops automatically when tests complete
- **10-100x faster than the old bash script approach!**

**Expected output:**
```
🚀 Starting CockroachDB YCSB workload for 120 seconds...
   (Generates ~5,000 ops/sec - much faster than bash script)
✅ YCSB workload started (PID: 12345)
   Will run for ~2 minute(s)
   Generating INSERT/UPDATE/DELETE operations on YCSB tables

✅ Connection successful!
✅ Found 3 tables: events, temp_records, usertable
✅ Schema retrieved
✅ Metadata retrieved
✅ Read 5 records
✅ Changefeed captured events
✅ All tests completed!
```

**If tests fail:** Run `./scripts/check_setup.sh` to diagnose issues.

**Testing Options:**
```bash
# Default: YCSB workload, auto-cleanup, 120 seconds (~5,000 ops/sec)
python scripts/test_local.py

# Custom duration
python scripts/test_local.py --duration 60

# Run diagnostic test first (recommended if tests are hanging)
python scripts/test_local.py --diagnostic

# No data generation (tests will timeout - for debugging)
python scripts/test_local.py --no-data

# Don't kill existing processes (if you want multiple tests running)
python scripts/test_local.py --no-cleanup
```

**Key Features:**
- 🚀 **Fast data generation**: Uses CockroachDB built-in YCSB workload (~5,000 ops/sec)
- 🧹 **Auto-cleanup**: Automatically kills any existing test processes before starting
- 🔍 **Diagnostic mode**: Run `--diagnostic` to test if changefeeds work before full tests
- ⚡ **One command**: Everything needed for end-to-end testing
- 📦 **Simple**: No bash scripts needed - just Python and CockroachDB CLI

## Remote CockroachCloud Testing

Test the connector against **remote CockroachDB clusters** using the `--url` parameter:

```bash
python scripts/test_local.py \
  --url "postgresql://user:password@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full" \
  --no-data
```

**Benefits:**
- ✅ Test against production-like environments
- ✅ Validate SSL/TLS configurations
- ✅ Debug issues on specific clusters
- ✅ Verify rangefeeds on remote clusters

**📖 See [learnings/REMOTE_TESTING.md](learnings/REMOTE_TESTING.md) for:**
- Full connection URL format and examples
- SSL mode configuration (verify-full, require, disable)
- Security best practices
- Troubleshooting guide
- Limitations and workarounds

## Testing Options

This connector provides two testing approaches:

### 1. Pytest Test Suite (Recommended for CI/CD)

Uses the standard Lakeflow test suite for automated validation:

```bash
# Run pytest test suite
pytest sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py -v

# With detailed output
pytest sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py -v -s
```

**Prerequisites:**
- CockroachDB cluster running (`./scripts/local_setup.sh start`)
- Rangefeeds enabled (automatically enabled by `local_setup.sh`)
- Test data loaded (YCSB workload via `local_setup.sh`)

**What it tests:**
- ✅ Connection establishment
- ✅ Table discovery (`list_tables`)
- ✅ Schema retrieval (`get_table_schema`)
- ✅ Metadata retrieval (`read_table_metadata`)
- ✅ Data reading (`read_table`)
- ✅ Changefeed CDC operations

**Configuration:**
- Connection: `configs/dev_config.json`
- Table options: `configs/dev_table_config.json`

### 2. Manual Interactive Testing (Recommended for Development)

Uses the standalone test script for interactive testing with live data generation:

```bash
# Default: YCSB workload, 120 seconds
python scripts/test_local.py

# With different workloads
python scripts/test_local.py --workload tpcc    # TPC-C benchmark
python scripts/test_local.py --workload kv      # Key-Value workload
python scripts/test_local.py --workload movr    # MovR multi-region

# Test specific tables
python scripts/test_local.py --table usertable
python scripts/test_local.py --workload tpcc --table warehouse

# Custom duration
python scripts/test_local.py --duration 60

# Diagnostic mode (troubleshoot hangs)
python scripts/test_local.py --diagnostic
```

**Benefits:**
- 🎯 **Interactive feedback**: Real-time progress and visual output
- 🚀 **Auto data generation**: Starts workloads automatically
- 📊 **Operation statistics**: Shows INSERT/UPDATE/DELETE counts
- 🔧 **Flexible**: Multiple workloads and table options
- 🐛 **Debugging**: Diagnostic mode and detailed error messages

**Use Cases:**
- Quick validation during development
- Testing with different workloads (YCSB, TPC-C, KV, MovR)
- Troubleshooting changefeed issues
- Performance testing with live data

---

## Prerequisites

- A CockroachDB cluster (self-hosted or Cockroach Cloud)
- Python 3.8+ with required packages (see `requirements.txt`)
  - `psycopg2-binary>=2.9.0` - PostgreSQL driver for CockroachDB connectivity
  - `pyspark>=3.3.0` - Apache Spark (provided in Databricks environments)
- Database user with the following permissions:
  - `CONNECT` privilege on the database
  - `SELECT` privilege on tables to be monitored
  - `CHANGEFEED` privilege on tables for CDC
- Network connectivity to the Cockroach DB cluster
- Tables must have a primary key (CockroachDB requirement for changefeeds)

## Local Testing Setup

For local development and testing, you can quickly set up a single-node CockroachDB cluster with sample data.

### Quick Start (Automated)

Use the provided setup script to automatically configure everything:

```bash
# Navigate to the cockroachdb source directory
cd sources/cockroachdb

# Install Python dependencies (for testing)
pip install -r requirements.txt

# Start cluster and load test data (includes rangefeed enablement)
./scripts/local_setup.sh start

# Verify everything works
python scripts/test_local.py

# Start YCSB workload generator (optional)
./scripts/local_setup.sh workload

# Test changefeed manually
./scripts/local_setup.sh changefeed

# Check status
./scripts/local_setup.sh status

# Stop cluster
./scripts/local_setup.sh stop

# For Docker mode
DOCKER_MODE=true ./scripts/local_setup.sh start
```

The automated script will:
- ✅ Start a single-node CockroachDB cluster (local binary or Docker)
- ✅ Create the `ycsb` database
- ✅ Load 10,000 records via YCSB workload
- ✅ Create additional test tables (`events`, `temp_records`)
- ✅ Grant changefeed privileges
- ✅ Provide commands to generate continuous load

**Connection Details After Setup:**
- **Admin UI**: http://localhost:8080
- **SQL Endpoint**: localhost:26257
- **Database**: ycsb
- **User**: root (no password)
- **SSL Mode**: disable (insecure mode for testing)

---

### Manual Setup (Step-by-Step)

If you prefer to set up manually or understand the details, follow these steps:

### Step 1: Download CockroachDB

#### macOS (Intel/Apple Silicon)

```bash
# Download the latest binary
curl https://binaries.cockroachdb.com/cockroach-latest.darwin-10.9-amd64.tgz | tar -xz

# Copy binary to PATH
sudo cp -i cockroach-latest.darwin-10.9-amd64/cockroach /usr/local/bin/

# Verify installation
cockroach version
```

#### Linux

```bash
# Download the latest binary
curl https://binaries.cockroachdb.com/cockroach-latest.linux-amd64.tgz | tar -xz

# Copy binary to PATH
sudo cp -i cockroach-latest.linux-amd64/cockroach /usr/local/bin/

# Verify installation
cockroach version
```

#### Using Homebrew (macOS)

```bash
brew install cockroachdb/tap/cockroach
cockroach version
```

#### Using Docker

```bash
docker pull cockroachdb/cockroach:latest
```

### Step 2: Start a Single-Node Cluster

#### Option A: Local Binary

```bash
# Create a data directory
mkdir -p ~/cockroach-data

# Start CockroachDB in insecure mode (for testing only)
cockroach start-single-node \
  --insecure \
  --store=~/cockroach-data \
  --listen-addr=localhost:26257 \
  --http-addr=localhost:8080 \
  --background

# Check cluster status
cockroach node status --insecure --host=localhost:26257
```

**Access the Admin UI**: http://localhost:8080

#### Option B: Docker

```bash
# Start CockroachDB container
docker run -d \
  --name=cockroach-local \
  -p 26257:26257 \
  -p 8080:8080 \
  -v cockroach-data:/cockroach/cockroach-data \
  cockroachdb/cockroach:latest \
  start-single-node --insecure

# Check status
docker exec -it cockroach-local ./cockroach node status --insecure
```

### Step 3: Load Sample Data with YCSB Workload

CockroachDB includes a built-in YCSB (Yahoo! Cloud Serving Benchmark) workload generator.

#### Initialize YCSB Database

```bash
# Connect to CockroachDB SQL shell
cockroach sql --insecure --host=localhost:26257

# Create database and grant permissions
CREATE DATABASE ycsb;
USE ycsb;

# Exit SQL shell (Ctrl+D or \q)
```

#### Load Initial Data

```bash
# Initialize YCSB workload with 10,000 records
cockroach workload init ycsb \
  --insecure \
  --host=localhost:26257 \
  --db=ycsb \
  --insert-count=10000

# Verify data loaded
cockroach sql --insecure --host=localhost:26257 -e "SELECT COUNT(*) FROM ycsb.usertable;"
```

**YCSB Schema**:
- **Table**: `usertable`
- **Columns**:
  - `ycsb_key` (VARCHAR, PRIMARY KEY) - Unique record ID
  - `field0` through `field9` (VARCHAR) - 10 data fields

#### Run Continuous Workload (for CDC Testing)

```bash
# Run workload in the background (generates INSERT, UPDATE operations)
cockroach workload run ycsb \
  --insecure \
  --host=localhost:26257 \
  --db=ycsb \
  --duration=10m \
  --concurrency=10 \
  --max-rate=100 &

# Monitor workload
watch -n 1 'cockroach sql --insecure --host=localhost:26257 -e "SELECT COUNT(*) FROM ycsb.usertable;"'
```

### Step 4: Enable Rangefeeds and Grant Changefeed Permissions

Rangefeeds must be enabled for changefeeds to work.

**Option A: Using Helper Script (Recommended)**

```bash
# Enable rangefeeds using the provided script
./scripts/enable_rangefeeds.sh

# For remote cluster
./scripts/enable_rangefeeds.sh --host my-cluster.cloud --port 26257
```

**Option B: Manual SQL**

```bash
# Connect to SQL shell
cockroach sql --insecure --host=localhost:26257

# Enable rangefeeds (required for CDC)
SET CLUSTER SETTING kv.rangefeed.enabled = true;

# Grant changefeed privilege
GRANT CHANGEFEED ON TABLE ycsb.usertable TO root;

# Verify privileges
SHOW GRANTS ON TABLE ycsb.usertable;

# Verify rangefeed setting
SHOW CLUSTER SETTING kv.rangefeed.enabled;
```

### Step 5: Test Changefeed Manually

```bash
# Test sinkless changefeed (will stream changes to stdout)
cockroach sql --insecure --host=localhost:26257 -e \
  "EXPERIMENTAL CHANGEFEED FOR ycsb.usertable WITH updated, resolved='5s';" &

# In another terminal, generate some changes
cockroach sql --insecure --host=localhost:26257 -e \
  "UPDATE ycsb.usertable SET field0 = 'test' WHERE ycsb_key LIKE 'user%' LIMIT 5;"
```

### Step 6: Test the Connector

#### Quick Test with Python Script

Use the provided test script to verify everything works:

```bash
# Navigate to cockroachdb directory
cd sources/cockroachdb

# Install Python dependencies
pip install -r requirements.txt

# Run the test script
python scripts/test_local.py
```

The test script will:
- ✅ Test connection to CockroachDB
- ✅ List all tables
- ✅ Get table schema
- ✅ Read table metadata
- ✅ Read data via changefeed
- ✅ Test live updates (INSERT operations)

#### Manual Configuration

Create `sources/cockroachdb/configs/dev_config.json`:

```json
{
  "host": "localhost",
  "port": "26257",
  "database": "ycsb",
  "user": "root",
  "password": "",
  "sslmode": "disable",
  "schema": "public"
}
```

**Note**: `sslmode: "disable"` is only for local testing with `--insecure` mode.

### Step 7: Create Additional Test Tables (Optional)

```bash
cockroach sql --insecure --host=localhost:26257 <<EOF
USE ycsb;

-- Create a simple test table
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    user_id INT,
    data JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Insert test data
INSERT INTO events (event_type, user_id, data)
VALUES
    ('login', 1, '{"ip": "192.168.1.1"}'),
    ('logout', 1, '{"duration": 3600}'),
    ('purchase', 2, '{"amount": 99.99, "product": "widget"}');

-- Grant privileges
GRANT SELECT, CHANGEFEED ON TABLE events TO root;

-- Create a table for testing deletions
CREATE TABLE temp_records (
    id SERIAL PRIMARY KEY,
    value VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO temp_records (value) VALUES ('record1'), ('record2'), ('record3');
GRANT SELECT, CHANGEFEED ON TABLE temp_records TO root;
EOF
```

### Cleanup

#### Stop the Cluster

```bash
# Using local binary
cockroach quit --insecure --host=localhost:26257

# Using Docker
docker stop cockroach-local
docker rm cockroach-local
```

#### Remove Data

```bash
# Local binary
rm -rf ~/cockroach-data

# Docker volumes
docker volume rm cockroach-data
```

### Test Script Troubleshooting

**Quick Diagnostic:** Run the diagnostic script to check all prerequisites:

```bash
./scripts/check_setup.sh
```

This will check:
- ✅ CockroachDB binary installed
- ✅ CockroachDB cluster running  
- ✅ ycsb database and tables exist
- ✅ Rangefeeds enabled (required for CDC)
- ✅ Python dependencies installed

**Note:** If `check_setup.sh` reports rangefeeds are not enabled even after running `./scripts/local_setup.sh start`, verify manually:

```bash
# Verify rangefeeds (detailed diagnostic)
./verify_rangefeeds.sh

# Or check directly
cockroach sql --insecure -e "SHOW CLUSTER SETTING kv.rangefeed.enabled;"
# Should show: true

# If it shows false or you're unsure, enable it:
./scripts/enable_rangefeeds.sh
```

If `python scripts/test_local.py` still fails, check these items manually:

#### 1. **Cluster Not Running**
```bash
# Check if CockroachDB is running
./scripts/local_setup.sh status

# If not running, start it
./scripts/local_setup.sh start
```

#### 2. **Rangefeeds Not Enabled** (Most Common Issue)
```bash
# The test script will detect this and show:
# "❌ Rangefeeds are NOT enabled"

# Fix by running:
./scripts/local_setup.sh start    # Recommended: resets and enables everything

# OR manually enable on existing cluster:
./scripts/enable_rangefeeds.sh

# Verify:
cockroach sql --insecure -e "SHOW CLUSTER SETTING kv.rangefeed.enabled;"
# Should output: true
```

#### 3. **Dependencies Not Installed**
```bash
# Install required Python packages
pip install -r requirements.txt

# Verify:
python -c "import psycopg2; import pyspark; print('✅ OK')"
```

#### 4. **Database Not Initialized**
```bash
# Check if ycsb database exists
cockroach sql --insecure -e "SHOW DATABASES;"

# If ycsb is missing, reinitialize:
./scripts/local_setup.sh start
```

#### 5. **Port Already in Use**
```bash
# Check what's running on port 26257
lsof -i :26257

# If another CockroachDB instance is running, stop it:
pkill -9 cockroach

# Then start fresh:
./scripts/local_setup.sh start
```

### Testing with Live Data

**Important:** Changefeeds are **streaming** - they wait for changes to occur in real-time. If no data is changing, the changefeed will hang waiting for events.

#### Automatic Data Generation (Default)

The `test_local.py` script **automatically** handles data generation:

```bash
# Automatically starts generate_test_data.sh for 120 seconds
python scripts/test_local.py

# Custom duration
python scripts/test_local.py --duration 60

# Skip data generation (for debugging)
python scripts/test_local.py --no-data
```

**How it works:**
1. Test script launches `generate_test_data.sh` as a background process
2. Waits 2 seconds for data generation to start
3. Runs all tests against live changefeed data
4. Automatically stops the data generator when tests complete

#### Alternative: Manual Data Generation

If you want more control, you can manually start the workload:

```bash
# Run CockroachDB workload directly
cockroach workload run ycsb \
  "postgresql://root@localhost:26257/ycsb?sslmode=disable" \
  --duration=2m

# Other available workloads:
cockroach workload run kv --duration=2m "postgresql://root@localhost:26257/ycsb?sslmode=disable"     # Key-value ops
cockroach workload run bank --duration=2m "postgresql://root@localhost:26257/ycsb?sslmode=disable"   # Bank transfers
cockroach workload run tpcc --duration=2m "postgresql://root@localhost:26257/ycsb?sslmode=disable"   # TPC-C benchmark

# Or use the legacy wrapper
./scripts/local_setup.sh workload      # Runs YCSB for 10 minutes
```

Then run tests with `--no-data` to skip automatic generation:
```bash
python scripts/test_local.py --no-data
```

**Why CockroachDB workloads?**
- ⚡ **10-100x faster** than bash scripts (written in Go)
- 📊 **Realistic patterns**: Mix of reads, writes, updates, deletes
- 🔄 **Industry standard**: YCSB, TPC-C, TPC-H, MovR benchmarks
- 🎯 **High throughput**: Generates ~5,000 operations/sec

### Local Testing Tips

1. **Watch Changefeed Events**: Use the SQL shell to watch changes in real-time
   ```bash
   cockroach sql --insecure --host=localhost:26257 -d ycsb
   EXPERIMENTAL CHANGEFEED FOR usertable WITH updated, resolved='5s';
   ```

2. **Generate Test Deletes**: Test DELETE operation handling
   ```bash
   cockroach sql --insecure -e "DELETE FROM ycsb.temp_records WHERE id = 1;"
   ```

3. **Check CDC Cursor**: Track the MVCC timestamp
   ```bash
   cockroach sql --insecure -e "SELECT cluster_logical_timestamp();"
   ```

4. **Monitor Performance**: Use the Admin UI at http://localhost:8080
   - View metrics, queries, and changefeed performance
   - Check for slow queries or bottlenecks

5. **Test Schema Changes**: Verify connector behavior after DDL
   ```sql
   ALTER TABLE ycsb.usertable ADD COLUMN new_field VARCHAR(100);
   ```

## Features

- **Real-time CDC**: Capture INSERT, UPDATE, and DELETE operations as they happen
- **Sinkless Changefeeds**: Direct streaming to SQL client without external infrastructure
- **Filtered Changefeeds**: Use inline SELECT statements to filter data at the source
- **Resumable**: Automatically tracks cursor position to resume from interruptions
- **Schema Discovery**: Automatically discovers tables and schemas

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `host` | string | Yes | CockroachDB cluster hostname | `free-tier.gcp-us-central1.cockroachlabs.cloud` |
| `port` | integer | No | Port number (default: 26257) | `26257` |
| `database` | string | Yes | Database name | `defaultdb` |
| `user` | string | Yes | Database username | `myuser` |
| `password` | string | Yes | User password | `********` |
| `sslmode` | string | No | SSL mode (default: 'require') | `require`, `verify-full` |
| `schema` | string | No | Schema name (default: 'public') | `public` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of allowed table options | `cursor,include_diff,select_query,resolved_interval,batch_size` |

**Note**: `externalOptionsAllowList` is **required** for this connector as it supports table-specific configuration options.

### Obtaining Connection Parameters

#### For CockroachDB Cloud:

1. Log in to your [CockroachDB Cloud Console](https://cockroachlabs.cloud/)
2. Navigate to your cluster
3. Click "Connect" to get connection details
4. Copy the connection string which contains:
   - `host`: The cluster hostname
   - `port`: Usually 26257
   - `database`: Your database name
   - `user`: Your username
5. Generate or retrieve your password from the console
6. **Important**: Enable rangefeeds for CDC:
   ```sql
   SET CLUSTER SETTING kv.rangefeed.enabled = true;
   ```

#### For Self-Hosted CockroachDB:

1. Obtain the hostname or IP address of your CockroachDB node
2. Default port is 26257 (or 5432 if using PostgreSQL-compatible mode)
3. Use your configured database name and credentials

### Grant Required Permissions

Connect to your CockroachDB cluster and grant necessary permissions:

```sql
-- Grant CONNECT privilege
GRANT CONNECT ON DATABASE your_database TO your_user;

-- Grant SELECT privilege on specific tables
GRANT SELECT ON TABLE your_schema.your_table TO your_user;

-- Grant CHANGEFEED privilege (required for CDC)
GRANT CHANGEFEED ON TABLE your_schema.your_table TO your_user;

-- Or grant on all tables in a schema
GRANT SELECT, CHANGEFEED ON ALL TABLES IN SCHEMA your_schema TO your_user;
```

### Create a Unity Catalog Connection 

A Unity Catalog connection for this connector can be created via the Databricks UI or API.

#### Via Databricks UI:
1. Navigate to the "Add Data" page in Databricks
2. Select "Lakeflow Community Connector"
3. Choose "CockroachDB" as the source
4. Enter your connection parameters
5. In the connection options, set `externalOptionsAllowList` to: `cursor,include_diff,select_query,resolved_interval,batch_size`

#### Via Databricks CLI:

```bash
# Set variables
CONNECTION_NAME="my_cockroachdb_connection"
HOST="your-cluster.cockroachlabs.cloud"
PORT="26257"
DATABASE="defaultdb"
USER="your_username"
PASSWORD="your_password"

# Create the connection
databricks connections create --json '{
  "name": "'"$CONNECTION_NAME"'",
  "connection_type": "GENERIC_LAKEFLOW_CONNECT",
  "options": {
    "sourceName": "cockroachdb",
    "host": "'"$HOST"'",
    "port": "'"$PORT"'",
    "database": "'"$DATABASE"'",
    "user": "'"$USER"'",
    "password": "'"$PASSWORD"'",
    "sslmode": "require",
    "schema": "public",
    "externalOptionsAllowList": "cursor,include_diff,select_query,resolved_interval,batch_size"
  }
}'
```

## Supported Objects

This connector supports all tables in the configured CockroachDB schema that meet the following requirements:

- Table must have a primary key (CockroachDB requirement for changefeeds)
- User must have `SELECT` and `CHANGEFEED` privileges on the table

### Table Discovery

Tables are automatically discovered from the `information_schema.tables` view. The connector will list all `BASE TABLE` types in the specified schema (default: `public`).

### Primary Keys

Primary key columns are automatically detected from `information_schema.key_column_usage`. Composite primary keys (multiple columns) are supported.

### Incremental Ingestion Strategy

All tables use **CDC (Change Data Capture)** via CockroachDB's sinkless changefeeds:

- **Ingestion Type**: `cdc`
- **Cursor Field**: `_cdc_updated` (CockroachDB MVCC timestamp)
- **Supported Operations**: INSERT, UPDATE, DELETE
- **Resumable**: Yes, using cursor timestamps

### Table-Specific Options

Each table can be configured with the following options:

| Option | Type | Required | Description | Example |
|---|---|---|---|---|
| `cursor` | string | No | Resume position (CockroachDB timestamp) | `'1734346800.0000000000'` |
| `include_diff` | boolean | No | Include before/after values for updates | `true`, `false` (default) |
| `select_query` | string | No | Custom SELECT for filtered changefeed | `SELECT id, name FROM users WHERE status='active'` |
| `resolved_interval` | string | No | Interval for progress markers (default: '10s') | `'10s'`, `'30s'`, `'1m'` |
| `batch_size` | integer | No | Events per batch (default: 1000) | `1000`, `5000` |

### CDC Metadata Fields

**The connector adds these fields** to each record to support Lakeflow/Delta Lake CDC workflows:

| Connector Field | Type | Derived From | Description |
|---|---|---|---|
| `_cdc_key` | array<string> | CockroachDB `key` column | Primary key values |
| `_cdc_updated` | string | CockroachDB `updated` column | MVCC timestamp (for cursor/resumption) |
| `_cdc_operation` | string | CockroachDB `value` column | Operation type: `INSERT`, `UPDATE`, or `DELETE` |

**Data Flow:**

```
┌─────────────────────────────────────────────┐
│ CockroachDB Changefeed (Native Columns)    │
├─────────────────────────────────────────────┤
│ key:     ["123"]                            │
│ value:   {"after": {"id": 123, ...}}        │
│ updated: "1734346800.0000000000"            │
│ topic:   "events"                           │
└─────────────────────────────────────────────┘
                    ↓
        (Connector Transformation)
                    ↓
┌─────────────────────────────────────────────┐
│ Spark DataFrame (Lakeflow CDC Format)      │
├─────────────────────────────────────────────┤
│ _cdc_key:       ["123"]           ← Added   │
│ _cdc_updated:   "1734346800..."   ← Added   │
│ _cdc_operation: "INSERT"          ← Added   │
│ id:             123                ← Original│
│ event_type:     "test"             ← Original│
│ ...                                ← Original│
└─────────────────────────────────────────────┘
```

**Key Points:**
- CockroachDB changefeeds natively return 4 columns: `(key, value, updated, topic)`
- The connector reads these columns (no database modification)
- The connector adds `_cdc_*` fields to follow Lakeflow CDC conventions
- Original table columns come from the `value.after` JSON
- **The source database is never modified** - fields only exist in ingested data

## Data Type Mapping

| CockroachDB Type | Spark Type | Notes |
|---|---|---|
| `INT`, `INT2`, `INT4`, `INT8`, `BIGINT`, `SERIAL`, `BIGSERIAL` | `LongType` | 64-bit integer |
| `FLOAT`, `FLOAT4`, `FLOAT8`, `DOUBLE PRECISION`, `REAL` | `DoubleType` | 64-bit float |
| `DECIMAL`, `NUMERIC` | `DecimalType` | Arbitrary precision |
| `BOOL`, `BOOLEAN` | `BooleanType` | True/False |
| `STRING`, `VARCHAR`, `CHAR`, `TEXT` | `StringType` | Variable length text |
| `BYTES`, `BYTEA` | `BinaryType` | Binary data |
| `DATE` | `DateType` | Calendar date |
| `TIMESTAMP` | `TimestampType` | Timestamp without timezone |
| `TIMESTAMPTZ` | `TimestampType` | Timestamp with timezone (recommended) |
| `TIME`, `INTERVAL` | `StringType` | Stored as string |
| `UUID` | `StringType` | UUID as string |
| `INET`, `CIDR`, `MACADDR` | `StringType` | Network addresses as string |
| `JSON`, `JSONB` | `StringType` | JSON stored as string |
| `ARRAY` | `ArrayType` | Array of values |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the CockroachDB source connector code.

### Step 2: Configure Your Pipeline

Create or update your `ingest.py` file with the pipeline specification:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Configuration
source_name = "cockroachdb"
connection_name = "my_cockroachdb_connection"

# Define pipeline specification
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "users",
                "table_configuration": {
                    "include_diff": "true",
                    "batch_size": "5000"
                }
            }
        },
        {
            "table": {
                "source_table": "orders",
                "table_configuration": {
                    "select_query": "SELECT id, user_id, total, created_at FROM orders WHERE status = 'completed'",
                    "resolved_interval": "30s"
                }
            }
        }
    ]
}

# Register and run the pipeline
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Step 3: Run and Schedule the Pipeline

#### Create the Pipeline

```bash
# Set variables
PIPELINE_NAME="cockroachdb_cdc_pipeline"
CATALOG="main"
SCHEMA="cdc_data"

# Create Unity Catalog schema if needed
databricks schemas create "$SCHEMA" "$CATALOG" || true

# Create the pipeline
databricks pipelines create --json '{
  "name": "'"$PIPELINE_NAME"'",
  "catalog": "'"$CATALOG"'",
  "schema": "'"$SCHEMA"'",
  "libraries": [
    {
      "file": {
        "path": "/Workspace/Users/your.email@company.com/cockroachdb/ingest.py"
      }
    }
  ],
  "serverless": true,
  "continuous": true,
  "development": false
}'
```

#### Start the Pipeline

```bash
PIPELINE_ID="<your-pipeline-id>"
databricks pipelines start-update "$PIPELINE_ID"
```

### Best Practices

- **Start Small**: Begin with a few tables to test the connector
- **Use Filtered Changefeeds**: Use `select_query` option to filter data at the source for better performance
- **Monitor Resolved Timestamps**: Track `_cdc_updated` to ensure continuous progress
- **Set Appropriate Batch Sizes**: Larger batches (5000-10000) improve throughput for high-volume tables
- **Use Continuous Mode**: For real-time CDC, run the pipeline in continuous mode
- **Handle Schema Changes**: Changefeeds may fail on schema changes (ADD/DROP COLUMN). Plan for pipeline restarts after DDL operations

### Troubleshooting

**Common Issues:**

1. **Rangefeeds Not Enabled**
   ```
   Error: rangefeeds require the kv.rangefeed.enabled setting
   ```
   - **Solution**: Enable rangefeeds with SQL:
     ```sql
     SET CLUSTER SETTING kv.rangefeed.enabled = true;
     ```
   - This is required for all changefeed operations
   - The `local_setup.sh` script does this automatically

2. **Connection Refused**
   - Check firewall rules and network connectivity
   - Verify the host and port are correct
   - For CockroachDB Cloud, ensure your IP is whitelisted

3. **Permission Denied**
   - Verify user has `CONNECT`, `SELECT`, and `CHANGEFEED` privileges
   - Check that the schema and table names are correct

4. **Table Has No Primary Key**
   - CockroachDB changefeeds require primary keys
   - Add a primary key to the table: `ALTER TABLE tablename ADD PRIMARY KEY (id)`

5. **Changefeed Fails After Schema Change**
   - Stop the pipeline
   - Apply the schema change
   - Restart the pipeline (it will resume from the last cursor)

6. **Slow Initial Snapshot**
   - Large tables take time for initial snapshot
   - Consider using `cursor` with current timestamp to skip historical data
   - Increase `batch_size` for better throughput

7. **Missing DELETE Events**
   - DELETE events are included by default
   - Check that `_cdc_operation` field is being processed correctly

## Examples

### Example 1: Basic CDC for All Tables

```python
pipeline_spec = {
    "connection_name": "my_cockroachdb",
    "objects": [
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "orders"}},
        {"table": {"source_table": "products"}}
    ]
}
```

### Example 2: Filtered Changefeed with Custom Query

```python
pipeline_spec = {
    "connection_name": "my_cockroachdb",
    "objects": [
        {
            "table": {
                "source_table": "transactions",
                "table_configuration": {
                    "select_query": "SELECT id, user_id, amount, created_at FROM transactions WHERE amount > 100"
                }
            }
        }
    ]
}
```

### Example 3: CDC with Before/After Values

```python
pipeline_spec = {
    "connection_name": "my_cockroachdb",
    "objects": [
        {
            "table": {
                "source_table": "user_profiles",
                "table_configuration": {
                    "include_diff": "true",
                    "resolved_interval": "5s"
                }
            }
        }
    ]
}
```

### Example 4: Resume from Specific Cursor

```python
pipeline_spec = {
    "connection_name": "my_cockroachdb",
    "objects": [
        {
            "table": {
                "source_table": "events",
                "table_configuration": {
                    "cursor": "1734346800.0000000000",
                    "batch_size": "10000"
                }
            }
        }
    ]
}
```

## Limitations

1. **Experimental Feature**: `EXPERIMENTAL CHANGEFEED FOR` is marked experimental in some CockroachDB versions
2. **Schema Changes**: Changefeeds do not automatically handle schema changes (requires pipeline restart)
3. **Connection Limits**: Each changefeed holds an open database connection
4. **Clock Skew**: Ensure NTP is configured on CockroachDB nodes for accurate timestamps
5. **Read-Only**: This connector only supports reading data (CDC), not writing back to CockroachDB

## References

- [CockroachDB Changefeeds Documentation](https://www.cockroachlabs.com/docs/stable/changefeed-for)
- [CockroachDB CREATE CHANGEFEED](https://www.cockroachlabs.com/docs/stable/create-changefeed)
- [CockroachDB Connection Parameters](https://www.cockroachlabs.com/docs/stable/connection-parameters)
- [CockroachDB Data Types](https://www.cockroachlabs.com/docs/stable/data-types)
- [CockroachDB Cloud Documentation](https://www.cockroachlabs.com/docs/cockroachcloud/)

