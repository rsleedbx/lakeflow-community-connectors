# CockroachDB CDC Tests - Quick Start

## TL;DR

```bash
# 1. Generate test data (bash)
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh

# 2. Validate patterns (pytest)
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -v

# 3. View results (Python/notebook)
from sources.cockroachdb.test_log_manager import TestLogManager
log_manager = TestLogManager('main', 'robert_lee_cockroachdb', 'parquet_files', dbutils)
report = log_manager.read_report(log_manager.list_test_runs()[0])  # Latest
log_manager.print_report(report, show_details=True)
```

## What Gets Tested

### 8 CDC Scenarios (All Combinations)
- **Formats**: JSON, Parquet
- **Tables**: usertable (10K rows), simple_test (1K rows)
- **Column Families**: with_split, no_split

### 2 Consumption Patterns
1. **Autoloader**: Production streaming pattern (`load_and_merge_cdc_to_delta`)
2. **Iterator**: Community connector batch pattern (`collect_all_records`)

### Validation
- ✅ Expected row count (initial + inserts - deletes)
- ✅ test_cdc_matrix.sh results
- ✅ Autoloader results
- ✅ Iterator results
- ✅ Autoloader vs Iterator consistency

## Shared Log Structure

**Location**: `/Volumes/{catalog}/{schema}/{volume}/test_logs/{timestamp}.json`

**Why**:
- Accessible from bash scripts AND Python notebooks
- Persists across sessions
- Enables cross-tool validation
- Historical test tracking

## Usage Examples

### Run All Tests

```bash
# Full matrix (8 scenarios × 2 patterns = 16 tests)
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -v
```

### Run Specific Tests

```bash
# JSON scenarios only
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k json -v

# Parquet scenarios only
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k parquet -v

# Iterator pattern only
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k iterator -v

# Autoloader pattern only
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k autoloader -v

# Pattern consistency checks only
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k consistency -v

# Specific scenario
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k "usertable_with_split" -v
```

### Use Specific Test Run

```bash
# Use specific test_run_id (timestamp)
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py \
  --test-run-id=1769034791 \
  -v
```

### View Results Programmatically

```python
from sources.cockroachdb.test_log_manager import TestLogManager

# Initialize
log_manager = TestLogManager('main', 'robert_lee_cockroachdb', 'parquet_files', dbutils)

# List all test runs
test_runs = log_manager.list_test_runs()
print(f"Found {len(test_runs)} test runs:")
for run in test_runs[:5]:  # Show latest 5
    print(f"  - {run}")

# Load latest report
report = log_manager.read_report(test_runs[0])

# Print summary
print(f"\n{'='*80}")
print(f"Test Run: {report.test_run_id}")
print(f"Total Scenarios: {report.total_scenarios}")
print(f"Passed: {report.passed_scenarios}")
print(f"Failed: {report.failed_scenarios}")
print(f"Success Rate: {report.success_rate():.1f}%")
print(f"{'='*80}\n")

# Find failures
for result in report.scenario_results:
    if result.validation_status != "PASS":
        print(f"❌ {result.scenario_name}")
        print(f"   Expected: {result.expected_final_rows}")
        print(f"   Autoloader: {result.autoloader_row_count}")
        print(f"   Iterator: {result.iterator_row_count}")
        print(f"   Errors: {result.validation_errors}")
        print()

# Detailed report
log_manager.print_report(report, show_details=True)
```

## Expected Output

### Pytest Output

```
================================= test session starts ==================================
collected 24 items

test_cockroachdb_cdc_scenarios.py::TestCockroachDBCDCScenarios::test_autoloader_pattern[test-json_usertable_with_split] PASSED     [  4%]
test_cockroachdb_cdc_scenarios.py::TestCockroachDBCDCScenarios::test_autoloader_pattern[test-json_usertable_no_split] PASSED       [  8%]
test_cockroachdb_cdc_scenarios.py::TestCockroachDBCDCScenarios::test_autoloader_pattern[test-json_simple_test_with_split] PASSED   [ 12%]
...
test_cockroachdb_cdc_scenarios.py::TestCockroachDBCDCScenarios::test_iterator_pattern[test-json_usertable_with_split] PASSED       [ 50%]
...
test_cockroachdb_cdc_scenarios.py::TestCockroachDBCDCScenarios::test_pattern_consistency[test-json_usertable_with_split] PASSED    [ 91%]
...

================================================================================
COCKROACHDB CDC TEST REPORT
================================================================================
Test Run ID: 1769034791
Test Runner: test_cdc_matrix.sh + pytest
Timestamp: 2026-01-22T00:17:51Z

📊 SUMMARY:
  Total Scenarios: 8
  Passed: 8
  Failed: 0
  Errors: 0
  Success Rate: 100.0%

📈 PATTERN SUCCESS RATES:
  Autoloader: 8/8 (100.0%)
  Iterator: 8/8 (100.0%)

⏱  Total Duration: 420.5s

================================================================================
```

### Log File Structure

```json
{
  "test_run_id": "1769034791",
  "run_timestamp": "2026-01-22T00:00:00Z",
  "test_runner": "test_cdc_matrix.sh + pytest",
  "scenario_results": [
    {
      "scenario_name": "test-json_usertable_with_split",
      "format": "json",
      "table": "usertable",
      "split_option": "with_split",
      "expected_final_rows": 9950,
      
      "test_matrix_unique_keys": 9950,
      "test_matrix_status": "SUCCESS",
      
      "autoloader_row_count": 9950,
      "autoloader_match_expected": true,
      "autoloader_duration_seconds": 45.2,
      
      "iterator_row_count": 9950,
      "iterator_match_expected": true,
      "iterator_duration_seconds": 12.3,
      
      "validation_status": "PASS",
      "validation_errors": []
    }
  ],
  "total_scenarios": 8,
  "passed_scenarios": 8,
  "autoloader_pass_count": 8,
  "iterator_pass_count": 8,
  "total_duration_seconds": 420.5
}
```

## Troubleshooting

### No test data found

```bash
# Generate test data first
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

### Wrong test_run_id

```bash
# List available test runs
python3 -c "
from sources.cockroachdb.test_log_manager import TestLogManager
import sys
sys.path.insert(0, 'sources/cockroachdb')

# Note: This requires dbutils, so run in Databricks notebook instead
log_manager = TestLogManager('main', 'robert_lee_cockroachdb', 'parquet_files', dbutils)
print('Available test runs:')
for run in log_manager.list_test_runs():
    print(f'  - {run}')
"
```

### Test failures

```python
# Load report and analyze failures
report = log_manager.read_report('1769034791')

for result in report.scenario_results:
    if result.validation_status != "PASS":
        print(f"❌ {result.scenario_name}")
        print(f"   Validation errors: {result.validation_errors}")
        
        # Check which pattern failed
        if not result.autoloader_match_expected:
            print(f"   Autoloader: expected {result.expected_final_rows}, got {result.autoloader_row_count}")
        
        if not result.iterator_match_expected:
            print(f"   Iterator: expected {result.expected_final_rows}, got {result.iterator_row_count}")
```

## Integration with Existing Tests

### Standard API Tests (Already Exists)

```bash
# Run standard LakeflowConnect API tests
pytest sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py -v
```

Tests: initialization, list_tables, get_table_schema, read_table_metadata, read_table (samples 3 records)

### CDC Integration Tests (New)

```bash
# Run comprehensive CDC tests
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -v
```

Tests: 8 scenarios × 2 patterns = 16 tests, validating full dataset correctness

### Run Both

```bash
# Run all CockroachDB tests
pytest sources/cockroachdb/test/ -v
```

## See Also

- `CDC_TEST_FRAMEWORK_INTEGRATION.md` - Detailed architecture documentation
- `CONNECTOR_EVOLUTION_STRATEGY.md` - Overall connector strategy
- `sources/cockroachdb/test_log_manager.py` - Logging API reference
- `sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py` - Test implementation
