# CockroachDB Connector - Development Learnings

This directory contains technical documentation, bug fixes, testing insights, and architectural decisions made during the development of the CockroachDB connector.

## 📚 Contents

### Project Organization

- **[SCRIPT_REORGANIZATION.md](SCRIPT_REORGANIZATION.md)**  
  Summary of script organization into `../scripts/` directory:
  - All test and setup scripts moved to dedicated directory
  - Documentation updates across all files
  - Before/after structure comparison
  - Benefits of cleaner organization

### Bug Fixes & Issues

- **[BUG_FIX_OPERATION_DETECTION.md](BUG_FIX_OPERATION_DETECTION.md)**  
  Critical bug discovered during operation statistics implementation. Documents two issues:
  - `memoryview` object handling from psycopg2
  - Wrong column index mapping in changefeed results
  - How the fix was discovered and implemented

- **[CHANGEFEED_OPTIONS_FIX.md](CHANGEFEED_OPTIONS_FIX.md)**  
  Documents the fix for CockroachDB changefeed option incompatibilities:
  - `initial_scan='only'` cannot be combined with `updated` or `resolved`
  - Conditional query building based on scan mode

- **[CTRL_C_FIX.md](CTRL_C_FIX.md)**  
  Explains why Ctrl+C wasn't working in `test_local.py` and the solution:
  - Issue with `signal.SIGALRM` interfering with `KeyboardInterrupt`
  - Migration to `threading.Thread` for timeouts

### Testing & Development

- **[REMOTE_TESTING.md](REMOTE_TESTING.md)**  
  Complete guide for testing against remote CockroachCloud clusters:
  - Using `--url` parameter with PostgreSQL connection URLs
  - SSL/TLS configuration (verify-full, require, disable)
  - Security best practices for password handling
  - Limitations and workarounds for remote testing
  - Troubleshooting connection issues

- **[TEST_REPORT_CDC.md](TEST_REPORT_CDC.md)**  
  Comprehensive CDC testing report including:
  - Test results for snapshot and streaming modes
  - Performance metrics (~5,000 ops/sec with YCSB)
  - Technical insights on changefeed behavior
  - Production recommendations

- **[PYTEST_INTEGRATION_SUMMARY.md](PYTEST_INTEGRATION_SUMMARY.md)**  
  Documents the integration of pytest test suite:
  - Created test files and configs per lakeflow-community-connectors guidelines
  - Test results (80% pass rate)
  - Known CDC-specific limitations
  - Comparison of pytest vs manual testing

- **[TEST_LOCAL_FEATURES.md](TEST_LOCAL_FEATURES.md)** (if exists)  
  Features and capabilities of the interactive test script

- **[DEBUGGING_HANGS.md](DEBUGGING_HANGS.md)**  
  Troubleshooting guide for common issues causing test hangs:
  - Rangefeeds not enabled
  - Network/firewall issues
  - Changefeed timeout behavior

### Performance & Optimization

- **[FAST_TESTING_UPGRADE.md](FAST_TESTING_UPGRADE.md)**  
  Documents the upgrade from bash scripts to CockroachDB built-in workloads:
  - 2,500x performance improvement (~2 ops/sec → ~5,000 ops/sec)
  - Using `cockroach workload` for realistic data generation
  - Benefits for testing and development

- **[WORKLOAD_GENERATORS.md](WORKLOAD_GENERATORS.md)**  
  Comparison of different data generation methods:
  - Custom bash scripts vs built-in workloads
  - YCSB, TPC-C, KV, MovR workload characteristics
  - Performance metrics and use cases

- **[WORKLOAD_TESTING_SUMMARY.md](WORKLOAD_TESTING_SUMMARY.md)**  
  Complete testing matrix for all CockroachDB workloads:
  - 4 fully supported workloads (ycsb, tpcc, kv, movr)
  - 2 partially supported (bank, tpch) - database setup required
  - 4 not applicable (special-purpose workloads)
  - Performance comparison and recommendations

### Architecture & Design

- **[SIMPLIFIED_ARCHITECTURE.md](SIMPLIFIED_ARCHITECTURE.md)**  
  Documents the simplification of testing architecture:
  - Removal of redundant shell scripts
  - Integration of functionality into `test_local.py`
  - Rationale for design decisions

## 🎯 Purpose

These documents serve as:
- **Learning resources** for future contributors
- **Historical record** of design decisions and bug fixes
- **Troubleshooting guides** for common issues
- **Best practices** for CDC connector development

## 📖 Related Documentation

For user-facing documentation, see:
- **[../README.md](../README.md)** - Main connector documentation
- **[../cockroachdb_api_doc.md](../cockroachdb_api_doc.md)** - CockroachDB API reference

## 🤝 Contributing

When adding new learnings:
1. Use descriptive filenames (e.g., `BUG_FIX_*.md`, `DEBUGGING_*.md`)
2. Include problem description, root cause, and solution
3. Add code examples where relevant
4. Update this README with a brief description

