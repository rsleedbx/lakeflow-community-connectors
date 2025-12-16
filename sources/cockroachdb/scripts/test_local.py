#!/usr/bin/env python3
"""
Simple test script for local CockroachDB connector testing.

Prerequisites:
    1. pip install -r requirements.txt
    2. ./local_setup.sh start    (includes rangefeed enablement)

Usage:
    python test_local.py                    # Default: YCSB workload, 120s data generation
    python test_local.py --duration 60      # Generate data for 60s
    python test_local.py --no-data          # Skip data generation (tests will timeout)
    python test_local.py --diagnostic       # Run changefeed diagnostic first
    python test_local.py --no-cleanup       # Don't kill existing processes

Features:
    • Fast data generation: Uses CockroachDB built-in YCSB workload (~5,000 ops/sec)
    • Automatic cleanup: Kills any existing test processes
    • Optional diagnostic: Tests if changefeeds work before running full tests
    • Ctrl+C support: Press Ctrl+C to interrupt tests at any time

Requirements:
    • CockroachDB CLI installed: brew install cockroachdb/tap/cockroach
    • Or see: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html

Troubleshooting:
    If tests hang:
      1. Run: python test_local.py --diagnostic
      2. Check: ./check_setup.sh
      3. Read: DEBUGGING_HANGS.md

Technical Note:
    Uses threading for timeouts instead of signal.SIGALRM to ensure
    Ctrl+C (SIGINT) works properly on all platforms.
"""

import json
import os
import sys
import subprocess
import time
import argparse
import threading
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sources.cockroachdb.cockroachdb import LakeflowConnect


# Workload configuration mapping
# Note: Only workloads that work with the default 'ycsb' database are included
# See learnings/WORKLOAD_TESTING_SUMMARY.md for full workload testing details
WORKLOAD_CONFIG = {
    "ycsb": {
        "name": "YCSB",
        "init_cmd": "cockroach workload init ycsb --drop 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "run_cmd": "cockroach workload run ycsb --duration={duration}s 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "default_table": "usertable",
        "tables": ["usertable"],
        "ops_per_sec": "~5,000",
    },
    "tpcc": {
        "name": "TPC-C",
        "init_cmd": "cockroach workload init tpcc --drop --warehouses=1 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "run_cmd": "cockroach workload run tpcc --duration={duration}s --warehouses=1 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "default_table": "warehouse",
        "tables": ["warehouse", "district", "customer", "orders", "new_order", "order_line", "stock", "item", "history"],
        "ops_per_sec": "~1,000",
    },
    "kv": {
        "name": "Key-Value",
        "init_cmd": "cockroach workload init kv --drop 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "run_cmd": "cockroach workload run kv --duration={duration}s 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "default_table": "kv",
        "tables": ["kv"],
        "ops_per_sec": "~10,000",
    },
    "movr": {
        "name": "MovR (Multi-region)",
        "init_cmd": "cockroach workload init movr --drop 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "run_cmd": "cockroach workload run movr --duration={duration}s 'postgresql://root@localhost:26257/ycsb?sslmode=disable'",
        "default_table": "users",
        "tables": ["users", "vehicles", "rides", "promo_codes", "user_promo_codes", "vehicle_location_histories"],
        "ops_per_sec": "~2,000",
    },
}


def check_rangefeeds_enabled():
    """Check if rangefeeds are enabled (required for CDC)."""
    print("\n🔍 Checking rangefeed setting...")
    
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=26257,
            database="ycsb",
            user="root",
            password="",
            sslmode="disable"
        )
        cur = conn.cursor()
        cur.execute("SHOW CLUSTER SETTING kv.rangefeed.enabled;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        # Check if result contains 'true' (case-insensitive)
        if result and str(result[0]).lower().strip() == 'true':
            print("✅ Rangefeeds are enabled")
            return True
        else:
            print(f"❌ Rangefeeds are NOT enabled (current value: {result[0] if result else 'unknown'})")
            print("\n⚠️  Changefeeds require rangefeeds to be enabled!")
            print("\nTo fix this, run:")
            print("  ./enable_rangefeeds.sh")
            print("\nOr manually via SQL:")
            print("  cockroach sql --insecure -e \"SET CLUSTER SETTING kv.rangefeed.enabled = true;\"")
            return False
    except Exception as e:
        print(f"⚠️  Could not check rangefeed setting: {e}")
        print("This is okay - will proceed with tests")
        return True  # Don't block tests if we can't check


def test_connection():
    """Test basic connection to CockroachDB."""
    print("🔌 Testing connection to local CockroachDB...")
    
    options = {
        "host": "localhost",
        "port": "26257",
        "database": "ycsb",
        "user": "root",
        "password": "",
        "sslmode": "disable",
        "schema": "public"
    }
    
    try:
        connector = LakeflowConnect(options)
        print("✅ Connection successful!")
        return connector
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)


def test_list_tables(connector):
    """Test table discovery."""
    print("\n📋 Listing tables...")
    
    try:
        tables = connector.list_tables()
        print(f"✅ Found {len(tables)} tables:")
        for table in tables:
            print(f"   - {table}")
        return tables
    except Exception as e:
        print(f"❌ Failed to list tables: {e}")
        return []


def test_get_schema(connector, table_name):
    """Test schema retrieval."""
    print(f"\n📊 Getting schema for '{table_name}'...")
    
    try:
        schema = connector.get_table_schema(table_name, {})
        print(f"✅ Schema retrieved for '{table_name}':")
        for field in schema.fields:
            print(f"   - {field.name}: {field.dataType} (nullable: {field.nullable})")
        return schema
    except Exception as e:
        print(f"❌ Failed to get schema for '{table_name}': {e}")
        return None


def test_get_metadata(connector, table_name):
    """Test metadata retrieval."""
    print(f"\n📝 Getting metadata for '{table_name}'...")
    
    try:
        metadata = connector.read_table_metadata(table_name, {})
        print(f"✅ Metadata retrieved for '{table_name}':")
        print(f"   - Primary keys: {metadata.get('primary_keys')}")
        print(f"   - Cursor field: {metadata.get('cursor_field')}")
        print(f"   - Ingestion type: {metadata.get('ingestion_type')}")
        return metadata
    except Exception as e:
        print(f"❌ Failed to get metadata for '{table_name}': {e}")
        return None


def test_read_table(connector, table_name, batch_size=10, test_cdc_mode=False):
    """Test reading table data via changefeed.
    
    Args:
        connector: LakeflowConnect instance
        table_name: Table to read from
        batch_size: Number of records to fetch
        test_cdc_mode: If True, test streaming CDC mode with timestamps
    """
    mode_name = "CDC streaming" if test_cdc_mode else "snapshot"
    print(f"\n📖 Reading data from '{table_name}' (batch size: {batch_size}, mode: {mode_name})...")
    
    if test_cdc_mode:
        print("   (Using initial_scan='yes' for streaming CDC with timestamps)")
    else:
        print("   (Using initial_scan='only' for snapshot mode)")
    
    try:
        if test_cdc_mode:
            # Streaming CDC mode: includes timestamps and continues streaming
            table_options = {
                "batch_size": str(batch_size),
                "resolved_interval": "1s",  # Short interval for faster testing
                "initial_scan": "yes"  # Return existing rows then continue streaming
            }
        else:
            # Snapshot mode: returns existing data and stops
            table_options = {
                "batch_size": str(batch_size),
                "resolved_interval": "1s",
                "initial_scan": "only"  # Return existing rows and stop
            }
        
        # Use a simple timeout with threading (doesn't interfere with Ctrl+C)
        records = []
        end_offset = {}
        timeout_occurred = threading.Event()
        
        # Shorter timeout for CDC streaming mode (it waits for new events after initial scan)
        timeout_seconds = 10 if test_cdc_mode else 30
        
        def read_with_timeout():
            nonlocal records, end_offset
            try:
                records_iter, end_offset = connector.read_table(
                    table_name,
                    start_offset={},
                    table_options=table_options
                )
                records = list(records_iter)
            except Exception:
                raise
        
        # Run in thread with timeout
        thread = threading.Thread(target=read_with_timeout, daemon=True)
        thread.start()
        thread.join(timeout=timeout_seconds)
        
        if thread.is_alive():
            # Timeout occurred
            if test_cdc_mode:
                raise TimeoutError(f"CDC streaming timed out after {timeout_seconds}s (expected - waits for new events)")
            else:
                raise TimeoutError(f"Changefeed read timed out after {timeout_seconds} seconds")
        
        if records:
            print(f"✅ Read {len(records)} records")
            print(f"   End offset: {end_offset}")
            
            # Count operations by type
            operation_counts = {}
            for record in records:
                op = record.get("_cdc_operation", "UNKNOWN")
                operation_counts[op] = operation_counts.get(op, 0) + 1
            
            print(f"\n   📊 Operation Statistics:")
            for op, count in sorted(operation_counts.items()):
                print(f"      {op}: {count}")
            
            print(f"\n   First record (sample):")
            first_record = records[0]
            for key, value in list(first_record.items())[:5]:  # Show first 5 fields
                print(f"      {key}: {value}")
            if len(first_record) > 5:
                print(f"      ... ({len(first_record) - 5} more fields)")
            
            # Verify CDC metadata
            if test_cdc_mode:
                has_timestamp = first_record.get("_cdc_updated") is not None
                if has_timestamp:
                    print(f"\n   ✅ CDC mode verified: _cdc_updated = {first_record.get('_cdc_updated')[:30]}...")
                else:
                    print(f"\n   ⚠️  CDC mode: _cdc_updated is None (unexpected in streaming mode)")
        else:
            print(f"⚠️  No records received")
            print(f"   The table may be empty or initial_scan returned nothing.")
        
        return records, end_offset
    except KeyboardInterrupt:
        print(f"\n🛑 Test interrupted by user")
        raise  # Re-raise to propagate to main handler
    except TimeoutError as e:
        print(f"⚠️  {e}")
        if test_cdc_mode:
            print(f"   This is EXPECTED for CDC streaming - it waits for new events after initial scan.")
            print(f"   In production, the changefeed would continue streaming as new data arrives.")
            print(f"   Records received before timeout: {len(records)}")
            if records:
                # Count operations by type
                operation_counts = {}
                for record in records:
                    op = record.get("_cdc_operation", "UNKNOWN")
                    operation_counts[op] = operation_counts.get(op, 0) + 1
                
                print(f"\n   📊 Operation Statistics (before timeout):")
                for op, count in sorted(operation_counts.items()):
                    print(f"      {op}: {count}")
                return records, end_offset
        else:
            print(f"   This shouldn't happen with initial_scan='only'.")
            print(f"   Check if rangefeeds are enabled: ./check_setup.sh")
        return [], {}
    except Exception as e:
        print(f"❌ Failed to read table: {e}")
        import traceback
        traceback.print_exc()
        return [], {}


def test_changefeed_with_updates(connector, table_name="events"):
    """Test changefeed with live updates."""
    print(f"\n🔄 Testing changefeed with live updates on '{table_name}'...")
    print("   (Will insert a new record, then try to capture it via changefeed)")
    
    try:
        # First, insert a test record
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=26257,
            database="ycsb",
            user="root",
            password="",
            sslmode="disable"
        )
        conn.set_session(autocommit=True)
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO events (event_type, user_id, data)
                VALUES ('test_event', 999, '{"test": true}'::JSONB)
                RETURNING id;
            """)
            inserted_id = cur.fetchone()[0]
            print(f"   Inserted test record with id: {inserted_id}")
        
        conn.close()
        
        # Now read from changefeed (with timeout using threading)
        # Use initial_scan='yes' to capture both existing + new records
        table_options = {
            "batch_size": "50",  # Larger batch to capture more operations
            "include_diff": "false",
            "initial_scan": "yes",  # Return existing rows then continue streaming
            "resolved_interval": "1s"
        }
        
        records = []
        end_offset = {}
        
        def read_with_timeout():
            nonlocal records, end_offset
            try:
                records_iter, end_offset = connector.read_table(
                    table_name,
                    start_offset={},
                    table_options=table_options
                )
                records = list(records_iter)
            except Exception:
                raise
        
        # Run in thread with timeout (shorter since we're using streaming mode)
        thread = threading.Thread(target=read_with_timeout, daemon=True)
        thread.start()
        thread.join(timeout=5)  # 5 second timeout (enough for initial scan)
        
        if thread.is_alive():
            # Timeout is expected with initial_scan='yes' - it waits for more events
            print(f"⚠️  Changefeed timed out after initial scan (expected with streaming mode)")
            print(f"   Checking for records received before timeout...")
        
        if records:
            print(f"✅ Changefeed captured {len(records)} events")
            
            # Count operations by type
            operation_counts = {}
            for record in records:
                op = record.get("_cdc_operation", "UNKNOWN")
                operation_counts[op] = operation_counts.get(op, 0) + 1
            
            print(f"\n   📊 Operation Statistics:")
            for op, count in sorted(operation_counts.items()):
                print(f"      {op}: {count}")
            
            # Look for our test record
            found = False
            for record in records:
                if record.get("id") == inserted_id:
                    found = True
                    print(f"\n   ✅ Found our test record:")
                    print(f"      Operation: {record.get('_cdc_operation')}")
                    print(f"      Event type: {record.get('event_type')}")
                    print(f"      User ID: {record.get('user_id')}")
                    break
            
            if not found:
                print(f"\n   ℹ️  Test record not in this batch (cursor: {end_offset.get('cursor')})")
        
        if not records:
            print(f"⚠️  No events captured")
            print(f"   This can happen if the changefeed starts after the insert.")
        
        return records, end_offset
    except KeyboardInterrupt:
        print(f"\n🛑 Test interrupted by user")
        raise  # Re-raise to propagate to main handler
    except TimeoutError as e:
        print(f"⚠️  Changefeed timed out waiting for events")
        print(f"   This is normal - changefeeds wait for new changes.")
        return [], {}
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return [], {}


def run_all_tests(preferred_table=None, workload="ycsb"):
    """Run all tests.
    
    Args:
        preferred_table: Specific table to test (optional). If not specified,
                        uses workload's default table.
        workload: Workload type (ycsb, tpcc, kv, movr). Determines default table selection.
    """
    print("="*60)
    print("CockroachDB Connector - Local Testing")
    print("="*60)
    
    # Pre-flight check: Verify rangefeeds are enabled
    rangefeeds_ok = check_rangefeeds_enabled()
    if not rangefeeds_ok:
        print("\n" + "="*60)
        print("⚠️  Setup incomplete - rangefeeds not enabled")
        print("="*60)
        print("\n Some tests will fail. Please enable rangefeeds first.")
        print("Then run this test script again.\n")
        # Continue anyway to show which tests work without rangefeeds
    
    # Test 1: Connection
    connector = test_connection()
    
    # Test 2: List tables
    tables = test_list_tables(connector)
    
    if not tables:
        print("\n⚠️  No tables found. Did you run ./local_setup.sh start?")
        return
    
    # Test 3: Schema for main table
    # Select table based on priority: CLI arg > workload default > first available
    workload_cfg = WORKLOAD_CONFIG.get(workload, WORKLOAD_CONFIG["ycsb"])
    workload_default_table = workload_cfg["default_table"]
    
    if preferred_table:
        if preferred_table in tables:
            test_table = preferred_table
            print(f"\n📋 Using user-specified table: '{test_table}'")
        else:
            print(f"\n⚠️  Specified table '{preferred_table}' not found. Available tables: {tables}")
            print(f"   Falling back to workload default...")
            test_table = workload_default_table if workload_default_table in tables else tables[0]
            print(f"   Using '{test_table}' as primary test table")
    elif workload_default_table in tables:
        test_table = workload_default_table
        print(f"\n📋 Using '{test_table}' as primary test table ({workload_cfg['name']} workload default)")
    elif tables:
        test_table = tables[0]
        print(f"\n📋 Using '{test_table}' as primary test table (first available)")
        print(f"   ⚠️  Workload default '{workload_default_table}' not found")
    else:
        test_table = workload_default_table  # Fallback
        print(f"\n📋 Using '{test_table}' as primary test table (fallback)")
    
    test_get_schema(connector, test_table)
    
    # Test 4: Metadata
    test_get_metadata(connector, test_table)
    
    # Test 5a: Read data (snapshot mode)
    test_read_table(connector, test_table, batch_size=50, test_cdc_mode=False)
    
    # Test 5b: Read data (streaming CDC mode with timestamps) - use workload's main table
    if test_table in tables:
        print("\n" + "="*60)
        print(f"Testing CDC Streaming Mode (with {workload_cfg['name']} workload data)")
        print("="*60)
        test_read_table(connector, test_table, batch_size=100, test_cdc_mode=True)
    
    # Test 6: Changefeed with updates (if events table exists)
    if "events" in tables:
        test_changefeed_with_updates(connector, "events")
    
    print("\n" + "="*60)
    print("✅ All tests completed!")
    print("="*60)
    print("\n💡 Optional manual exploration (not required):")
    print("  - View Admin UI: http://localhost:8080")
    print("  - Run continuous workload: ./local_setup.sh workload")
    print("  - Watch changefeed live: ./local_setup.sh changefeed")


def start_data_generator(duration, workload="ycsb"):
    """Start CockroachDB workload data generation in the background.
    
    Args:
        duration: How long to generate data (seconds)
        workload: Workload type (ycsb, tpcc, kv, movr)
    
    Returns:
        subprocess.Popen object or None if failed
    """
    workload_cfg = WORKLOAD_CONFIG.get(workload, WORKLOAD_CONFIG["ycsb"])
    
    print(f"\n🚀 Starting CockroachDB {workload_cfg['name']} workload for {duration} seconds...")
    print(f"   (Generates {workload_cfg['ops_per_sec']} ops/sec)")
    
    # Use the run_cmd from config and substitute duration
    cmd_string = workload_cfg["run_cmd"].format(duration=duration)
    
    # Parse the command string into list for Popen
    # Note: This is a simple split - works for our use case
    cmd_parts = cmd_string.split()
    
    try:
        # Start the workload as a background process
        process = subprocess.Popen(
            cmd_parts,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Give it a moment to start
        time.sleep(2)
        
        # Check if it started successfully
        if process.poll() is not None:
            print("❌ Failed to start workload")
            print("   Tests will run without live data generation (may timeout)")
            return None
        
        print(f"✅ {workload_cfg['name']} workload started (PID: {process.pid})")
        print(f"   Will run for ~{(duration + 59) // 60} minute(s)")
        print(f"   Generating INSERT/UPDATE/DELETE operations on {workload_cfg['name']} tables\n")
        return process
        
    except FileNotFoundError:
        print("❌ 'cockroach' command not found")
        print("   Install: brew install cockroachdb/tap/cockroach (macOS)")
        print("   Or see: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html")
        print("   Tests will run without live data generation (may timeout)")
        return None
    except Exception as e:
        print(f"❌ Could not start workload: {e}")
        print("   Tests will run without live data generation (may timeout)")
        return None


def cleanup_existing_processes():
    """Kill any existing test_local.py and cockroach workload processes."""
    import signal as sig
    
    killed_any = False
    
    try:
        # Kill existing test_local.py processes (excluding current process)
        current_pid = os.getpid()
        result = subprocess.run(
            ["pgrep", "-f", "python.*test_local.py"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pids = [int(pid) for pid in result.stdout.strip().split('\n') if pid]
            for pid in pids:
                if pid != current_pid:
                    try:
                        os.kill(pid, sig.SIGTERM)
                        killed_any = True
                        print(f"   Killed existing test_local.py (PID: {pid})")
                    except ProcessLookupError:
                        pass
        
        # Kill existing cockroach workload processes
        result = subprocess.run(
            ["pgrep", "-f", "cockroach workload run"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pids = [int(pid) for pid in result.stdout.strip().split('\n') if pid]
            for pid in pids:
                try:
                    os.kill(pid, sig.SIGTERM)
                    killed_any = True
                    print(f"   Killed existing cockroach workload (PID: {pid})")
                except ProcessLookupError:
                    pass
        
        if killed_any:
            time.sleep(1)  # Give processes time to terminate
            print()
    
    except Exception as e:
        # Don't fail if cleanup fails (pgrep might not be available)
        pass


def run_diagnostic_test():
    """Run the diagnostic changefeed test."""
    print("="*60)
    print("Running Diagnostic Test")
    print("="*60)
    print("Testing if changefeeds work at all...")
    print()
    
    diagnostic_script = Path(__file__).parent / "test_changefeed_direct.py"
    
    if not diagnostic_script.exists():
        print("⚠️  Diagnostic script not found, skipping...")
        print()
        return True
    
    try:
        result = subprocess.run(
            [sys.executable, str(diagnostic_script)],
            timeout=15,
            capture_output=False
        )
        
        if result.returncode == 0:
            print()
            print("✅ Diagnostic test passed! Proceeding with full tests...")
            print()
            return True
        else:
            print()
            print("⚠️  Diagnostic test failed, but continuing anyway...")
            print()
            return False
    
    except subprocess.TimeoutExpired:
        print()
        print("❌ Diagnostic test timed out!")
        print("   This means changefeeds are hanging.")
        print("   See DEBUGGING_HANGS.md for troubleshooting.")
        print()
        return False
    except KeyboardInterrupt:
        print("\n🛑 Diagnostic interrupted")
        raise
    except Exception as e:
        print(f"⚠️  Could not run diagnostic: {e}")
        print()
        return False


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Test CockroachDB connector with live data generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_local.py                              # Default: YCSB workload, 120s
  python test_local.py --duration 60                # YCSB workload for 60s
  python test_local.py --workload tpcc              # TPC-C workload (tests 'warehouse' table)
  python test_local.py --workload tpcc --table orders  # TPC-C with specific table
  python test_local.py --workload kv                # Key-Value workload (tests 'kv' table)
  python test_local.py --table events               # YCSB workload but test 'events' table
  python test_local.py --no-data                    # No data generation (tests timeout)
  python test_local.py --diagnostic                 # Run diagnostic test first

Supported Workloads:
  ycsb  - YCSB benchmark (~5,000 ops/sec, table: usertable)
  tpcc  - TPC-C benchmark (~1,000 ops/sec, tables: warehouse, district, orders, etc.)
  kv    - Key-Value (~10,000 ops/sec, table: kv)
  movr  - MovR multi-region (~2,000 ops/sec, tables: users, vehicles, rides, etc.)

Note: See learnings/WORKLOAD_TESTING_SUMMARY.md for other workloads (bank, tpch, etc.)
  
Requirements:
  CockroachDB CLI must be installed:
    macOS:  brew install cockroachdb/tap/cockroach
    Linux:  https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html
        """
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Duration for data generation in seconds (default: 120)"
    )
    parser.add_argument(
        "--no-data",
        action="store_true",
        help="Skip data generation (tests may timeout)"
    )
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help="Run diagnostic changefeed test first"
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't kill existing test processes before starting"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Specific table to test (overrides --workload default)"
    )
    parser.add_argument(
        "--workload",
        type=str,
        choices=["ycsb", "tpcc", "kv", "movr"],
        default="ycsb",
        help="CockroachDB workload type (default: ycsb). Also sets default test table."
    )
    
    args = parser.parse_args()
    
    data_generator_process = None
    
    try:
        # Step 1: Cleanup existing processes
        if not args.no_cleanup:
            print("🧹 Cleaning up existing processes...")
            cleanup_existing_processes()
        
        # Step 2: Run diagnostic test if requested
        if args.diagnostic:
            diagnostic_passed = run_diagnostic_test()
            if not diagnostic_passed:
                print("⚠️  Diagnostic test had issues, but continuing anyway...")
                print("   Press Ctrl+C to abort, or wait to continue...")
                time.sleep(3)
        
        # Step 3: Start data generator unless --no-data flag is set
        if not args.no_data:
            data_generator_process = start_data_generator(args.duration, args.workload)
        else:
            print("\n⚠️  Running tests WITHOUT data generation")
            print("   Changefeed tests will likely timeout (this is expected)\n")
        
        # Step 4: Run the tests
        run_all_tests(preferred_table=args.table, workload=args.workload)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Tests interrupted by user (Ctrl+C)")
        print("   Cleaning up...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up data generator process (always runs, even on Ctrl+C)
        if data_generator_process and data_generator_process.poll() is None:
            try:
                print(f"\n🛑 Stopping data generator (PID: {data_generator_process.pid})...")
                data_generator_process.terminate()
                data_generator_process.wait(timeout=5)
                print("✅ Data generator stopped cleanly")
            except subprocess.TimeoutExpired:
                print("⚠️  Data generator didn't stop gracefully, forcing...")
                data_generator_process.kill()
                data_generator_process.wait(timeout=1)
                print("✅ Data generator killed")
            except Exception as e:
                print(f"⚠️  Error stopping data generator: {e}")

