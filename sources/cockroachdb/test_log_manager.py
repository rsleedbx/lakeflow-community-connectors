"""
Shared test log management for CockroachDB CDC tests.

Provides unified interface for writing/reading test results to/from Unity Catalog Volume.
Designed to be used by both test_cdc_matrix.sh and test notebooks.

Compatible with the existing LakeflowConnect test framework (test_suite.py).
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, field, asdict


@dataclass
class CDCTestResult:
    """
    Individual CDC scenario test result.
    Compatible with test_suite.TestResult structure but CDC-specific.
    """
    scenario_name: str
    format: str  # "json" or "parquet"
    table: str  # "usertable" or "simple_test"
    split_option: str  # "with_split" or "no_split"
    
    # Test matrix results (from test_cdc_matrix.sh)
    test_matrix_unique_keys: int = 0
    test_matrix_snapshot_rows: int = 0
    test_matrix_insert_rows: int = 0
    test_matrix_update_rows: int = 0
    test_matrix_delete_rows: int = 0
    test_matrix_status: str = ""  # "SUCCESS", "PARTIAL", "FAILED"
    
    # Autoloader results (from notebook)
    autoloader_row_count: int = 0
    autoloader_match_expected: bool = False
    autoloader_duration_seconds: float = 0.0
    
    # Iterator results (from notebook)
    iterator_row_count: int = 0
    iterator_match_expected: bool = False
    iterator_duration_seconds: float = 0.0
    
    # Expected values (from workload params)
    expected_final_rows: int = 0
    
    # Validation results
    validation_status: str = "UNKNOWN"  # "PASS", "FAIL", "ERROR"
    validation_errors: List[str] = field(default_factory=list)
    
    # Metadata
    volume_path: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


@dataclass
class CDCTestReport:
    """
    Complete CDC test report for all scenarios.
    Compatible with test_suite.TestReport structure.
    """
    test_run_id: str
    run_timestamp: str
    test_runner: str  # "test_cdc_matrix.sh", "notebook", "pytest"
    filter_format: Optional[str] = None
    
    # Results
    scenario_results: List[CDCTestResult] = field(default_factory=list)
    
    # Summary
    total_scenarios: int = 0
    passed_scenarios: int = 0
    failed_scenarios: int = 0
    error_scenarios: int = 0
    
    autoloader_pass_count: int = 0
    autoloader_total_count: int = 0
    iterator_pass_count: int = 0
    iterator_total_count: int = 0
    
    total_duration_seconds: float = 0.0
    
    def success_rate(self) -> float:
        """Calculate overall success rate"""
        if self.total_scenarios == 0:
            return 0.0
        return (self.passed_scenarios / self.total_scenarios) * 100
    
    def autoloader_success_rate(self) -> float:
        """Calculate Autoloader pattern success rate"""
        if self.autoloader_total_count == 0:
            return 0.0
        return (self.autoloader_pass_count / self.autoloader_total_count) * 100
    
    def iterator_success_rate(self) -> float:
        """Calculate Iterator pattern success rate"""
        if self.iterator_total_count == 0:
            return 0.0
        return (self.iterator_pass_count / self.iterator_total_count) * 100


class TestLogManager:
    """
    Manages CDC test result logs stored in Unity Catalog Volume.
    
    Log structure: /Volumes/{catalog}/{schema}/{volume}/test_logs/{timestamp}.json
    
    This manager bridges test_cdc_matrix.sh (bash) and test notebooks (Python),
    providing a unified logging interface compatible with the existing test framework.
    """
    
    def __init__(self, catalog: str, schema: str, volume: str, dbutils=None):
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.dbutils = dbutils
        self.log_base_path = f"/Volumes/{catalog}/{schema}/{volume}/test_logs"
        
    def _ensure_log_dir(self):
        """Create test_logs directory if it doesn't exist."""
        if self.dbutils:
            try:
                self.dbutils.fs.mkdirs(self.log_base_path)
            except Exception:
                pass  # Directory might already exist
        else:
            # Local filesystem fallback
            os.makedirs(self.log_base_path, exist_ok=True)
    
    def get_log_path(self, test_run_id: str) -> str:
        """Get full path for a test run log."""
        return f"{self.log_base_path}/{test_run_id}.json"
    
    def create_report(
        self,
        test_run_id: str,
        test_runner: str = "unknown",
        filter_format: Optional[str] = None
    ) -> CDCTestReport:
        """
        Create a new CDC test report.
        
        Args:
            test_run_id: Unix timestamp or unique identifier
            test_runner: Name of the tool creating the log
            filter_format: Optional format filter ("json" or "parquet")
            
        Returns:
            Empty CDCTestReport
        """
        return CDCTestReport(
            test_run_id=test_run_id,
            run_timestamp=datetime.utcnow().isoformat() + "Z",
            test_runner=test_runner,
            filter_format=filter_format
        )
    
    def add_scenario_result(
        self,
        report: CDCTestReport,
        result: CDCTestResult
    ):
        """
        Add a scenario result to the report.
        
        Args:
            report: CDCTestReport to update
            result: CDCTestResult to add
        """
        report.scenario_results.append(result)
    
    def compute_summary(self, report: CDCTestReport):
        """
        Compute summary statistics for the test report.
        Updates report.total_scenarios, passed_scenarios, etc.
        """
        report.total_scenarios = len(report.scenario_results)
        report.passed_scenarios = 0
        report.failed_scenarios = 0
        report.error_scenarios = 0
        
        report.autoloader_pass_count = 0
        report.autoloader_total_count = 0
        report.iterator_pass_count = 0
        report.iterator_total_count = 0
        
        report.total_duration_seconds = 0.0
        
        for result in report.scenario_results:
            # Count validation status
            if result.validation_status == "PASS":
                report.passed_scenarios += 1
            elif result.validation_status == "FAIL":
                report.failed_scenarios += 1
            else:
                report.error_scenarios += 1
            
            # Count Autoloader results
            if result.autoloader_row_count > 0:
                report.autoloader_total_count += 1
                if result.autoloader_match_expected:
                    report.autoloader_pass_count += 1
                report.total_duration_seconds += result.autoloader_duration_seconds
            
            # Count Iterator results
            if result.iterator_row_count > 0:
                report.iterator_total_count += 1
                if result.iterator_match_expected:
                    report.iterator_pass_count += 1
                report.total_duration_seconds += result.iterator_duration_seconds
    
    def validate_scenario(self, result: CDCTestResult):
        """
        Validate a scenario result and set validation_status.
        
        Checks:
        - Expected vs test_matrix unique_keys
        - Expected vs autoloader row_count
        - Expected vs iterator row_count
        - Autoloader vs iterator
        """
        errors = []
        
        expected = result.expected_final_rows
        
        # Validate test_matrix
        if result.test_matrix_unique_keys != expected:
            errors.append(
                f"test_matrix: expected {expected}, got {result.test_matrix_unique_keys}"
            )
        
        # Validate autoloader
        if result.autoloader_row_count > 0 and result.autoloader_row_count != expected:
            errors.append(
                f"autoloader: expected {expected}, got {result.autoloader_row_count}"
            )
        
        # Validate iterator
        if result.iterator_row_count > 0 and result.iterator_row_count != expected:
            errors.append(
                f"iterator: expected {expected}, got {result.iterator_row_count}"
            )
        
        # Validate autoloader vs iterator
        if (
            result.autoloader_row_count > 0
            and result.iterator_row_count > 0
            and result.autoloader_row_count != result.iterator_row_count
        ):
            errors.append(
                f"autoloader ({result.autoloader_row_count}) != iterator ({result.iterator_row_count})"
            )
        
        # Set validation status
        if not errors:
            result.validation_status = "PASS"
        else:
            result.validation_status = "FAIL"
        
        result.validation_errors = errors
    
    def write_report(self, report: CDCTestReport):
        """
        Write CDC test report to Unity Catalog Volume.
        
        Args:
            report: CDCTestReport to write
        """
        self._ensure_log_dir()
        log_path = self.get_log_path(report.test_run_id)
        
        # Convert to dict for JSON serialization
        report_dict = asdict(report)
        json_str = json.dumps(report_dict, indent=2)
        
        if self.dbutils:
            # Databricks environment
            self.dbutils.fs.put(log_path, json_str, overwrite=True)
        else:
            # Local filesystem
            with open(log_path, 'w') as f:
                f.write(json_str)
    
    def read_report(self, test_run_id: str) -> Optional[CDCTestReport]:
        """
        Read CDC test report from Unity Catalog Volume.
        
        Args:
            test_run_id: Test run identifier
            
        Returns:
            CDCTestReport or None if not found
        """
        log_path = self.get_log_path(test_run_id)
        
        try:
            if self.dbutils:
                # Databricks environment
                content = self.dbutils.fs.head(log_path, maxBytes=10*1024*1024)  # 10MB limit
                report_dict = json.loads(content)
            else:
                # Local filesystem
                with open(log_path, 'r') as f:
                    report_dict = json.load(f)
            
            # Convert dict back to dataclass
            # Convert scenario_results list to CDCTestResult objects
            scenario_results = [
                CDCTestResult(**scenario) for scenario in report_dict.pop("scenario_results", [])
            ]
            
            report = CDCTestReport(**report_dict)
            report.scenario_results = scenario_results
            
            return report
            
        except Exception as e:
            print(f"⚠️  Failed to read log: {e}")
            return None
    
    def list_test_runs(self) -> List[str]:
        """
        List all available test run IDs (timestamps).
        
        Returns:
            List of test run IDs sorted by timestamp (newest first)
        """
        try:
            if self.dbutils:
                files = self.dbutils.fs.ls(self.log_base_path)
                run_ids = [
                    os.path.basename(f.path).replace('.json', '')
                    for f in files
                    if f.path.endswith('.json')
                ]
            else:
                run_ids = [
                    f.replace('.json', '')
                    for f in os.listdir(self.log_base_path)
                    if f.endswith('.json')
                ]
            
            # Sort by timestamp (newest first)
            return sorted(run_ids, reverse=True)
        except Exception:
            return []
    
    def print_report(self, report: CDCTestReport, show_details: bool = True):
        """
        Print CDC test report in a format similar to test_suite.py.
        
        Args:
            report: CDCTestReport to print
            show_details: Whether to show scenario details
        """
        print(f"\n{'=' * 80}")
        print(f"COCKROACHDB CDC TEST REPORT")
        print(f"{'=' * 80}")
        print(f"Test Run ID: {report.test_run_id}")
        print(f"Test Runner: {report.test_runner}")
        print(f"Timestamp: {report.run_timestamp}")
        if report.filter_format:
            print(f"Format Filter: {report.filter_format}")
        
        print(f"\n📊 SUMMARY:")
        print(f"  Total Scenarios: {report.total_scenarios}")
        print(f"  Passed: {report.passed_scenarios}")
        print(f"  Failed: {report.failed_scenarios}")
        print(f"  Errors: {report.error_scenarios}")
        print(f"  Success Rate: {report.success_rate():.1f}%")
        
        print(f"\n📈 PATTERN SUCCESS RATES:")
        print(f"  Autoloader: {report.autoloader_pass_count}/{report.autoloader_total_count} ({report.autoloader_success_rate():.1f}%)")
        print(f"  Iterator: {report.iterator_pass_count}/{report.iterator_total_count} ({report.iterator_success_rate():.1f}%)")
        
        print(f"\n⏱  Total Duration: {report.total_duration_seconds:.1f}s")
        
        if show_details:
            print(f"\n{'=' * 80}")
            print("SCENARIO RESULTS:")
            print(f"{'=' * 80}\n")
            
            for result in report.scenario_results:
                status_symbol = {
                    "PASS": "✅",
                    "FAIL": "❌",
                    "ERROR": "❗",
                    "UNKNOWN": "❓"
                }.get(result.validation_status, "❓")
                
                print(f"{status_symbol} {result.scenario_name}")
                print(f"   Format: {result.format}, Table: {result.table}, Split: {result.split_option}")
                print(f"   Expected: {result.expected_final_rows} rows")
                
                if result.test_matrix_unique_keys > 0:
                    match_symbol = "✓" if result.test_matrix_unique_keys == result.expected_final_rows else "✗"
                    print(f"   test_matrix: {result.test_matrix_unique_keys} rows {match_symbol}")
                
                if result.autoloader_row_count > 0:
                    match_symbol = "✓" if result.autoloader_match_expected else "✗"
                    print(f"   autoloader: {result.autoloader_row_count} rows {match_symbol} ({result.autoloader_duration_seconds:.1f}s)")
                
                if result.iterator_row_count > 0:
                    match_symbol = "✓" if result.iterator_match_expected else "✗"
                    print(f"   iterator: {result.iterator_row_count} rows {match_symbol} ({result.iterator_duration_seconds:.1f}s)")
                
                if result.validation_errors:
                    print(f"   ⚠️  Validation errors:")
                    for error in result.validation_errors:
                        print(f"      - {error}")
                
                print()
        
        print(f"{'=' * 80}\n")
