# One-Off Test Scripts

This directory contains standalone Python test scripts that are **not called by shell scripts** in the main test suite. These are utility scripts for manual testing, experimentation, and one-off investigations.

## Scripts in This Directory

### Changefeed Management

**`cancel_changefeed_job.py`**
- Utility to cancel a specific changefeed job
- Standalone tool (use `changefeed_helper.py cancel-changefeed` instead for automated workflows)

**`setup_test_table.py`**
- Creates the `cdc_test` table with sample data for testing
- Referenced in `test_azure_cdc_small.sh` comments but not executed automatically
- Run manually before using `test_azure_cdc_small.sh`

### Connection Tests

**`test_asyncpg.py`**
- Tests async PostgreSQL connections to CockroachDB using asyncpg
- Validates connection string and basic query functionality

**`test_asyncpg_simple.py`**
- Simplified version of asyncpg connection testing
- Quick validation of async connectivity

**`test_local.py`**
- Tests local CockroachDB connections
- Development/debugging tool

### Changefeed-Specific Tests

**`test_changefeed_direct.py`**
- Direct changefeed testing without shell wrapper
- Manual investigation of changefeed behavior

**`test_column_families.py`**
- Tests CockroachDB column families behavior
- Investigates `split_column_families` option requirements

**`test_family_batch_query.py`**
- Tests batch queries across column families
- Performance investigation tool

**`test_resolved_timestamps.py`**
- Tests changefeed resolved timestamps
- Investigates timestamp behavior and ordering

## Usage

These scripts are meant to be run manually for specific testing needs:

```bash
# Example: Run connection test
cd /path/to/scripts/oneoff
python3 test_asyncpg_simple.py

# Example: Set up test table
python3 setup_test_table.py

# Example: Test column families
python3 test_column_families.py
```

## Main Test Suite Scripts

For automated testing workflows, use the main test scripts in the parent directory:

- `../changefeed_helper.py` - Unified CLI for changefeed operations
- `../test_cdc_matrix.sh` - Comprehensive CDC test matrix
- `../test_azure_cdc.sh` - Azure CDC integration tests
- `../test_single_operations.sh` - Individual operation tests

## Why These Are Separate

These scripts are kept in `oneoff/` because:
- ✅ Not part of automated test workflows
- ✅ Used for manual investigation and debugging
- ✅ Experimental or prototype code
- ✅ Specific to development/troubleshooting scenarios
- ✅ Not dependencies for any shell scripts

## Adding New One-Off Scripts

If you create a new standalone test script that is:
- Not called by shell scripts
- Used for manual testing only
- Experimental or investigative

Place it in this directory to keep the main `scripts/` folder organized.





