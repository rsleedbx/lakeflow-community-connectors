# CockroachDB Connector Scripts

This directory contains test scripts and setup utilities for the CockroachDB connector.

## Setup Scripts

### `local_setup.sh`
Sets up a local CockroachDB cluster for testing.

**Usage:**
```bash
./scripts/local_setup.sh start      # Start cluster and load YCSB data
./scripts/local_setup.sh stop       # Stop cluster
./scripts/local_setup.sh status     # Check cluster status
./scripts/local_setup.sh workload   # Run YCSB workload for 10 minutes
./scripts/local_setup.sh changefeed # Test changefeed manually
```

**Features:**
- Starts single-node CockroachDB cluster
- Enables rangefeeds (required for CDC)
- Loads YCSB benchmark data
- Creates `ycsb` database with `usertable`

### `enable_rangefeeds.sh`
Standalone helper to enable rangefeeds on a CockroachDB cluster.

**Usage:**
```bash
./scripts/enable_rangefeeds.sh                                    # Local cluster
./scripts/enable_rangefeeds.sh --host my-cluster.cloud --port 26257  # Remote cluster
```

**Note:** `local_setup.sh start` already enables rangefeeds automatically.

### `check_setup.sh`
Diagnostic script to verify local setup is correct.

**Usage:**
```bash
./scripts/check_setup.sh
```

**Checks:**
- ✅ CockroachDB is running
- ✅ Rangefeeds are enabled
- ✅ YCSB database and tables exist
- ✅ Test data is loaded

---

## Test Scripts

### `test_local.py` ⭐ Main Test Script
Comprehensive end-to-end testing for the CockroachDB connector.

**Quick Start:**
```bash
# Default: YCSB workload, 120 seconds, auto-cleanup
python scripts/test_local.py

# Custom duration
python scripts/test_local.py --duration 60

# Different workload
python scripts/test_local.py --workload tpcc    # TPC-C
python scripts/test_local.py --workload kv      # Key-Value
python scripts/test_local.py --workload movr    # MovR
```

**Features:**
- ✅ Auto-generates live data using CockroachDB workloads
- ✅ Tests connection, schema, metadata, read operations
- ✅ Tests CDC streaming with operation statistics
- ✅ Automatic process cleanup
- ✅ Supports multiple workloads (YCSB, TPC-C, KV, MovR)

**Options:**
```bash
--duration 60          # Run for 60 seconds (default: 120)
--workload ycsb        # Workload type (default: ycsb)
--table usertable      # Specific table to test
--no-data              # Skip data generation (for debugging)
--diagnostic           # Run diagnostic test first
--no-cleanup           # Don't kill existing processes
```

**What it tests:**
1. Connection to CockroachDB
2. Listing tables
3. Getting schema (with CDC fields)
4. Reading metadata (CDC ingestion type)
5. Reading data (snapshot mode)
6. CDC streaming with live updates
7. Operation statistics (INSERT/UPDATE/DELETE)

### `test_changefeed_direct.py`
Standalone diagnostic tool for testing CockroachDB changefeeds directly.

**Usage:**
```bash
python scripts/test_changefeed_direct.py
```

**Purpose:**
- Low-level changefeed testing without the connector
- Useful for diagnosing changefeed issues
- Tests `initial_scan='only'` mode
- Validates changefeed column parsing

**When to use:**
- Troubleshooting changefeed failures
- Verifying CockroachDB setup
- Understanding changefeed output format

### `run_pytest.sh`
Runs the pytest test suite for the connector.

**Usage:**
```bash
./scripts/run_pytest.sh
```

**What it runs:**
- `sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py`
- Uses the generic `LakeflowConnectTester` framework
- Tests basic interface compliance

**Note:** For comprehensive CDC testing, use `test_local.py` instead. The pytest suite has limitations with CDC connectors (see `learnings/PYTEST_INTEGRATION_SUMMARY.md`).

---

## Typical Workflow

### First Time Setup
```bash
# 1. Start CockroachDB and load data
./scripts/local_setup.sh start

# 2. Run comprehensive tests
python scripts/test_local.py
```

### Daily Development
```bash
# Quick test
python scripts/test_local.py --duration 30

# Test different workloads
python scripts/test_local.py --workload tpcc
```

### Troubleshooting
```bash
# Diagnose setup
./scripts/check_setup.sh

# Enable rangefeeds manually
./scripts/enable_rangefeeds.sh

# Run diagnostic test
python scripts/test_local.py --diagnostic

# Test changefeed directly
python scripts/test_changefeed_direct.py
```

---

## Workload Comparison

See `../learnings/WORKLOAD_TESTING_SUMMARY.md` for complete details.

| Workload | Throughput | Tables | Use Case |
|----------|-----------|--------|----------|
| **ycsb** | ~5,000 ops/sec | 1 | General OLTP ⭐ Default |
| **tpcc** | ~1,000 ops/sec | 9 | Financial transactions |
| **kv** | ~10,000 ops/sec | 1 | Cache, sessions |
| **movr** | ~2,000 ops/sec | 6 | Multi-region apps |

---

## Prerequisites

**Required:**
- CockroachDB binary (download from cockroachlabs.com)
- Python 3.8+
- Dependencies: `psycopg2-binary`, `pyspark`

**Setup:**
```bash
# Install Python dependencies
cd sources/cockroachdb
pip install -r requirements.txt

# Start CockroachDB
./scripts/local_setup.sh start
```

---

## See Also

- **[../README.md](../README.md)** - Main connector documentation
- **[../learnings/](../learnings/)** - Detailed technical documentation
- **[../configs/](../configs/)** - Configuration examples

