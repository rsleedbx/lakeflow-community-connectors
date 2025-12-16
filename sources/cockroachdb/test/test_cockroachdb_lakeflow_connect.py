import pytest
from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.cockroachdb.cockroachdb import LakeflowConnect


def test_cockroachdb_connector():
    """Test the CockroachDB connector using the test suite.
    
    Prerequisites:
        1. CockroachDB cluster running locally (./local_setup.sh start)
        2. Rangefeeds enabled (SET CLUSTER SETTING kv.rangefeed.enabled = true)
        3. Test data loaded (YCSB workload via ./local_setup.sh start)
    
    Run:
        pytest sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py -v
    """
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create tester with the config
    tester = LakeflowConnectTester(config, table_config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )

