# CockroachDB CDC Test Framework Integration

## Overview

The CockroachDB CDC tests extend the existing community connector test framework with comprehensive integration tests that validate end-to-end data correctness across multiple CDC scenarios.

### Two-Tier Testing Architecture

```
Community Connector Tests
├── Tier 1: API Contract Tests (test_suite.py)
│   ├── test_initialization()
│   ├── test_list_tables()
│   ├── test_get_table_schema()
│   ├── test_read_table_metadata()
│   └── test_read_table() ← Samples 3 records
│
└── Tier 2: CDC Integration Tests (CockroachDB-specific)
    ├── test_cdc_matrix.sh ← Bash script (generates test data)
    ├── test_cockroachdb_cdc_scenarios.py ← Pytest (validates patterns)
    └── test_all_cdc_scenarios.ipynb ← Notebook (interactive validation)
```

## Architecture

### Shared Log Structure

**Location**: `/Volumes/{catalog}/{schema}/{volume}/test_logs/{timestamp}.json`

**Benefits**:
- ✅ Accessible from bash scripts AND notebooks
- ✅ Persists across sessions
- ✅ Enables cross-tool validation
- ✅ Historical test tracking

**Schema**: See `test_log_manager.py` for full structure.

### Components

#### 1. `test_log_manager.py` - Core Logging Library

```python
from sources.cockroachdb.test_log_manager import (
    TestLogManager,
    CDCTestReport,
    CDCTestResult
)

# Initialize
log_manager = TestLogManager('catalog', 'schema', 'volume', dbutils)

# Create report
report = log_manager.create_report(test_run_id="1769034791", test_runner="pytest")

# Add results
result = CDCTestResult(
    scenario_name="test-json_usertable_with_split",
    format="json",
    table="usertable",
    split_option="with_split",
    expected_final_rows=9950
)
log_manager.add_scenario_result(report, result)

# Validate and save
log_manager.validate_scenario(result)
log_manager.compute_summary(report)
log_manager.write_report(report)
log_manager.print_report(report)
```

**Key Classes**:
- `CDCTestResult`: Individual scenario result (compatible with `test_suite.TestResult`)
- `CDCTestReport`: Complete test report (compatible with `test_suite.TestReport`)
- `TestLogManager`: Reads/writes logs to Unity Catalog Volume

#### 2. `test_cockroachdb_cdc_scenarios.py` - Pytest Integration

**Purpose**: Automated CDC testing using pytest framework.

**Features**:
- ✅ Parametrized tests for all 8 scenarios
- ✅ Tests both Autoloader and Iterator patterns
- ✅ Validates pattern consistency
- ✅ Compatible with existing test framework structure
- ✅ Records results in shared log

**Usage**:

```bash
# Run all CDC scenarios
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -v

# Run specific test_run_id
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py --test-run-id=1769034791 -v

# Run only JSON scenarios
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k json -v

# Run only Iterator tests
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k iterator -v

# Run pattern consistency checks
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -k consistency -v
```

**Test Methods**:
1. `test_autoloader_pattern[scenario]` - Tests Autoloader pattern for each scenario
2. `test_iterator_pattern[scenario]` - Tests Iterator pattern for each scenario
3. `test_pattern_consistency[scenario]` - Validates Autoloader vs Iterator consistency

#### 3. `test_all_cdc_scenarios.ipynb` - Interactive Notebook (Future)

**Purpose**: Interactive validation and debugging.

**Features**:
- ✅ Runs all 8 scenarios in a loop
- ✅ Tests both patterns
- ✅ Generates visual comparison report
- ✅ Records results in shared log
- ✅ Can resume from existing test_run_id

## Comparison: Standard vs CDC Tests

| Aspect | Standard API Tests | CDC Integration Tests |
|--------|-------------------|----------------------|
| **Purpose** | Validate LakeflowConnect API contract | Validate end-to-end CDC data correctness |
| **Scope** | Single table, 3 sample records | 8 scenarios, full dataset (10k+ rows) |
| **Patterns** | Iterator only | Autoloader + Iterator |
| **Test Data** | Live API calls | Pre-generated file-based data |
| **Duration** | Seconds | Minutes |
| **Infrastructure** | API credentials | Azure, Volume, CockroachDB, Spark |
| **Framework** | `test_suite.py` | `test_suite.py` (extended) |

## Workflow

### Step 1: Generate Test Data (Bash)

```bash
# Run full test matrix (generates data + writes test_matrix results to log)
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh

# Test specific format only
./test_cdc_matrix.sh json

# Re-validate existing data (faster)
./test_cdc_matrix.sh --validate-only
```

**Output**:
- Creates 8 test scenarios in Azure + Volume
- Generates `/Volumes/.../test_logs/1769034791.json` with test_matrix results
- Leaves changefeeds running for incremental tests

### Step 2: Validate Patterns (Pytest)

```bash
# Run automated pattern validation
pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -v
```

**What it does**:
1. Reads existing log from Step 1
2. Runs Autoloader + Iterator patterns for each scenario
3. Updates log with pattern results
4. Validates:
   - Expected vs test_matrix
   - Expected vs Autoloader
   - Expected vs Iterator
   - Autoloader vs Iterator

**Output**:
- Updates `/Volumes/.../test_logs/1769034791.json` with pattern results
- Prints comprehensive test report
- Returns pytest exit code (0 = all passed)

### Step 3: Interactive Analysis (Notebook)

```python
# In Databricks notebook
from sources.cockroachdb.test_log_manager import TestLogManager

# Load existing report
log_manager = TestLogManager('main', 'robert_lee_cockroachdb', 'parquet_files', dbutils)
report = log_manager.read_report('1769034791')

# Print summary
log_manager.print_report(report, show_details=True)

# Deep-dive into specific scenario
for result in report.scenario_results:
    if result.validation_status != "PASS":
        print(f"❌ {result.scenario_name}")
        print(f"   Errors: {result.validation_errors}")
```

## Integration with Existing Framework

### How CDC Tests Extend Standard Tests

```python
# Standard test structure (test_suite.py)
class LakeflowConnectTester:
    def test_read_table(self):
        """Samples 3 records, validates iterator works"""
        iterator, offset = connector.read_table(...)
        for i, record in enumerate(iterator):
            if i >= 3:  # Sample only
                break

# CDC test structure (test_cockroachdb_cdc_scenarios.py)
class TestCockroachDBCDCScenarios:
    def test_iterator_pattern(self):
        """Reads ALL records, validates data correctness"""
        result = collect_all_records(connector, table_name, ...)
        assert len(result['records']) == expected_final_rows
        
    def test_autoloader_pattern(self):
        """Runs production Autoloader, validates Delta table"""
        result = load_and_merge_cdc_to_delta(...)
        assert result['delta_count'] == expected_final_rows
    
    def test_pattern_consistency(self):
        """Validates Autoloader == Iterator"""
        assert autoloader_count == iterator_count
```

### Shared Data Structures

```python
# test_suite.py
@dataclass
class TestResult:
    test_name: str
    status: TestStatus  # PASSED, FAILED, ERROR
    message: str
    details: Dict[str, Any]

@dataclass
class TestReport:
    connector_class_name: str
    test_results: List[TestResult]
    total_tests: int
    passed_tests: int
    failed_tests: int

# test_log_manager.py (extends for CDC)
@dataclass
class CDCTestResult:
    scenario_name: str
    format: str
    table: str
    
    # test_matrix results
    test_matrix_unique_keys: int
    test_matrix_status: str
    
    # Autoloader results
    autoloader_row_count: int
    autoloader_match_expected: bool
    
    # Iterator results
    iterator_row_count: int
    iterator_match_expected: bool
    
    # Validation
    validation_status: str  # PASS, FAIL, ERROR
    validation_errors: List[str]

@dataclass
class CDCTestReport:
    test_run_id: str
    scenario_results: List[CDCTestResult]
    total_scenarios: int
    passed_scenarios: int
    
    def success_rate(self) -> float:
        """Compatible with TestReport.success_rate()"""
```

## Log File Example

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
      
      "test_matrix_unique_keys": 9950,
      "test_matrix_status": "SUCCESS",
      
      "autoloader_row_count": 9950,
      "autoloader_match_expected": true,
      "autoloader_duration_seconds": 45.2,
      
      "iterator_row_count": 9950,
      "iterator_match_expected": true,
      "iterator_duration_seconds": 12.3,
      
      "expected_final_rows": 9950,
      "validation_status": "PASS",
      "validation_errors": [],
      
      "volume_path": "/Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split/1769034791"
    }
  ],
  "total_scenarios": 8,
  "passed_scenarios": 8,
  "failed_scenarios": 0,
  "autoloader_pass_count": 8,
  "autoloader_total_count": 8,
  "iterator_pass_count": 8,
  "iterator_total_count": 8,
  "total_duration_seconds": 420.5
}
```

## Benefits

### 1. Unified Logging
- ✅ Bash script and notebook share same log
- ✅ Can run tests from different tools and compare
- ✅ Historical test tracking

### 2. Cross-Tool Validation
```bash
# Step 1: Bash generates data
./test_cdc_matrix.sh  # Writes test_matrix results

# Step 2: Pytest validates patterns
pytest test_cockroachdb_cdc_scenarios.py  # Adds autoloader + iterator results

# Step 3: Notebook analyzes
# Load log, compare test_matrix vs autoloader vs iterator
```

### 3. Pytest Integration
- ✅ Standard test framework
- ✅ Parametrized tests
- ✅ CI/CD compatible
- ✅ JUnit XML reports

### 4. Incremental Testing
```python
# Day 1: Run test_cdc_matrix.sh
report = log_manager.read_report('1769034791')
# Only test_matrix results present

# Day 2: Run pytest tests
# Reads existing report, adds pattern results

# Day 3: Run notebook
# All results available for comparison
```

## CI/CD Integration

```yaml
# .github/workflows/test.yml
name: CockroachDB CDC Tests

on: [push, pull_request]

jobs:
  cdc-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      # Step 1: Generate test data (if not cached)
      - name: Generate CDC test data
        run: |
          cd sources/cockroachdb/scripts
          ./test_cdc_matrix.sh json  # Test JSON only for speed
      
      # Step 2: Run pytest validation
      - name: Run CDC pattern tests
        run: |
          pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py \
            -k json \
            -v \
            --junitxml=cdc-test-results.xml
      
      # Step 3: Upload results
      - name: Upload test results
        uses: actions/upload-artifact@v3
        with:
          name: cdc-test-results
          path: cdc-test-results.xml
```

## Future Enhancements

### 1. Notebook Integration
Create `test_all_cdc_scenarios.ipynb`:
- Loop through all 8 scenarios
- Run both patterns
- Generate visual report
- Save to shared log

### 2. Performance Tracking
```python
# Track performance over time
for test_run in log_manager.list_test_runs():
    report = log_manager.read_report(test_run)
    print(f"{test_run}: {report.total_duration_seconds}s")
    
# Detect regressions
if report.total_duration_seconds > baseline * 1.5:
    alert("Performance regression detected!")
```

### 3. Schema Evolution Tests
```python
# Test schema changes don't break ingestion
result = CDCTestResult(
    scenario_name="test-schema-evolution",
    ...
)
```

## Summary

The CDC test framework extends the existing community connector test suite with:

1. **Shared Logging**: Unity Catalog Volume-based logs accessible from bash + Python
2. **Pytest Integration**: Standard test framework with parametrized CDC tests
3. **Pattern Validation**: Tests both Autoloader and Iterator patterns
4. **Cross-Tool Validation**: Bash script + pytest + notebook share results
5. **Historical Tracking**: Test runs logged with timestamps for trend analysis

This architecture maintains compatibility with the existing test framework while adding comprehensive CDC integration testing capabilities.
