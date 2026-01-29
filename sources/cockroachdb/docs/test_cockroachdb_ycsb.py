"""
Test script for cockroachdb_ycsb.py module

This script demonstrates all functions in the cockroachdb_ycsb module.
Run this to verify the extraction is working correctly.

Usage:
    python test_cockroachdb_ycsb.py
"""

import sys
from cockroachdb_ycsb import (
    create_ycsb_table,
    get_table_stats,
    insert_ycsb_snapshot,
    run_ycsb_workload,
    insert_ycsb_snapshot_with_random_nulls
)

def test_ycsb_module():
    """
    Test all YCSB functions.
    
    NOTE: This requires a CockroachDB connection to be configured.
    Update the connection parameters below.
    """
    try:
        import pg8000
        import ssl
    except ImportError as e:
        print(f"❌ Missing required module: {e}")
        print("   Install with: pip install pg8000")
        sys.exit(1)
    
    # ==========================================================================
    # CONFIGURATION - Update these values for your environment
    # ==========================================================================
    CRDB_HOST = "your-cluster.cockroachlabs.cloud"
    CRDB_PORT = 26257
    CRDB_USER = "your_user"
    CRDB_PASSWORD = "your_password"
    CRDB_DATABASE = "defaultdb"
    
    TEST_TABLE_1 = "test_ycsb_single_cf"
    TEST_TABLE_2 = "test_ycsb_multi_cf"
    TEST_TABLE_3 = "test_ycsb_nulls"
    
    # ==========================================================================
    # Connect to CockroachDB
    # ==========================================================================
    print("=" * 80)
    print("COCKROACHDB YCSB MODULE TEST")
    print("=" * 80)
    print()
    print(f"📡 Connecting to CockroachDB...")
    print(f"   Host: {CRDB_HOST}")
    print(f"   Database: {CRDB_DATABASE}")
    print()
    
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        host = CRDB_HOST.split(':')[0] if ':' in CRDB_HOST else CRDB_HOST
        
        conn = pg8000.connect(
            user=CRDB_USER,
            password=CRDB_PASSWORD,
            host=host,
            port=CRDB_PORT,
            database=CRDB_DATABASE,
            ssl_context=ssl_context
        )
        
        print("✅ Connected successfully!")
        print()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print()
        print("💡 Update the connection parameters in this script and try again.")
        sys.exit(1)
    
    try:
        # ======================================================================
        # TEST 1: Create table (single column family)
        # ======================================================================
        print("=" * 80)
        print("TEST 1: Create YCSB table (single column family)")
        print("=" * 80)
        print()
        
        create_ycsb_table(
            conn=conn,
            table_name=TEST_TABLE_1,
            column_family_mode="single_cf"
        )
        print()
        
        # ======================================================================
        # TEST 2: Insert snapshot
        # ======================================================================
        print("=" * 80)
        print("TEST 2: Insert snapshot data")
        print("=" * 80)
        print()
        
        insert_ycsb_snapshot(
            conn=conn,
            table_name=TEST_TABLE_1,
            snapshot_count=10
        )
        print()
        
        # ======================================================================
        # TEST 3: Get table stats
        # ======================================================================
        print("=" * 80)
        print("TEST 3: Get table statistics")
        print("=" * 80)
        print()
        
        stats = get_table_stats(conn, TEST_TABLE_1)
        print(f"📊 Table stats: {TEST_TABLE_1}")
        print(f"   Min key: {stats['min_key']}")
        print(f"   Max key: {stats['max_key']}")
        print(f"   Count:   {stats['count']}")
        print(f"   Empty:   {stats['is_empty']}")
        print()
        
        # ======================================================================
        # TEST 4: Run workload
        # ======================================================================
        print("=" * 80)
        print("TEST 4: Run YCSB workload (INSERT/UPDATE/DELETE)")
        print("=" * 80)
        print()
        
        result = run_ycsb_workload(
            conn=conn,
            table_name=TEST_TABLE_1,
            insert_count=5,
            update_count=3,
            delete_count=2
        )
        print()
        print(f"📊 Workload result:")
        print(f"   Before: {result['before']['count']} rows")
        print(f"   After:  {result['after']['count']} rows")
        print(f"   Net change: {result['after']['count'] - result['before']['count']:+d}")
        print()
        
        # ======================================================================
        # TEST 5: Create table (multiple column families)
        # ======================================================================
        print("=" * 80)
        print("TEST 5: Create YCSB table (multiple column families)")
        print("=" * 80)
        print()
        
        create_ycsb_table(
            conn=conn,
            table_name=TEST_TABLE_2,
            column_family_mode="multi_cf"
        )
        print()
        
        # ======================================================================
        # TEST 6: Insert snapshot with random NULLs
        # ======================================================================
        print("=" * 80)
        print("TEST 6: Insert snapshot with random NULLs (NEW FEATURE)")
        print("=" * 80)
        print()
        
        create_ycsb_table(
            conn=conn,
            table_name=TEST_TABLE_3,
            column_family_mode="multi_cf"
        )
        
        insert_ycsb_snapshot_with_random_nulls(
            conn=conn,
            table_name=TEST_TABLE_3,
            snapshot_count=10,
            null_probability=0.4,
            columns_to_randomize=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'],
            seed=42  # For reproducibility
        )
        print()
        
        # ======================================================================
        # SUMMARY
        # ======================================================================
        print("=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        print()
        print("📋 Summary:")
        print(f"   ✅ create_ycsb_table() - Working (both single_cf and multi_cf)")
        print(f"   ✅ insert_ycsb_snapshot() - Working")
        print(f"   ✅ get_table_stats() - Working")
        print(f"   ✅ run_ycsb_workload() - Working")
        print(f"   ✅ insert_ycsb_snapshot_with_random_nulls() - Working")
        print()
        print("💡 Next steps:")
        print("   1. Refactor the notebook to use these functions (see NOTEBOOK_REFACTORING_GUIDE.md)")
        print("   2. Test NULL scenario in CDC ingestion (see YCSB_MODULE_SUMMARY.md)")
        print()
        print(f"🧹 Cleanup:")
        print(f"   Run these commands to drop the test tables:")
        print(f"      DROP TABLE IF EXISTS {TEST_TABLE_1} CASCADE;")
        print(f"      DROP TABLE IF EXISTS {TEST_TABLE_2} CASCADE;")
        print(f"      DROP TABLE IF EXISTS {TEST_TABLE_3} CASCADE;")
        print()
        
    except Exception as e:
        print()
        print("=" * 80)
        print(f"❌ TEST FAILED")
        print("=" * 80)
        print(f"   Error: {e}")
        print()
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()
        print("🔌 Connection closed")


if __name__ == "__main__":
    test_ycsb_module()
