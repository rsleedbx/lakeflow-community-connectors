#!/usr/bin/env python3
"""
Setup a small test table for fast CDC testing.

Creates a table with:
- 100 rows (much smaller than usertable's ~10,000)
- Single column family (vs usertable's 11)
- Simple structure for easy verification
- Total: ~100 events for initial scan (vs ~110,000)

This allows CDC testing to complete in seconds instead of minutes.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from cockroachdb import LakeflowConnect
import json


def main():
    # Load credentials
    crdb_json = os.path.join(os.path.dirname(__file__), '../.env/cockroachdb_credentials.json')
    
    with open(crdb_json) as f:
        crdb_creds = json.load(f)
    
    # Create connector
    print("🔧 Setting up small test table for CDC...")
    print()
    
    # Convert URL to token+base_url format
    url = crdb_creds['cockroachdb_url']
    url_without_scheme = url.replace('postgresql://', '', 1)
    token_part, host_part = url_without_scheme.split('@', 1)
    
    connector = LakeflowConnect({
        'token': token_part,
        'base_url': f"postgresql://{host_part}",
        'catalog': 'ycsb',
        'schema': 'public'
    })
    
    # Drop table if exists
    print("1. Dropping existing cdc_test table (if any)...")
    try:
        connector.execute_sql("DROP TABLE IF EXISTS cdc_test CASCADE")
        print("   ✅ Dropped")
    except Exception as e:
        print(f"   ⚠️  {e}")
    
    print()
    
    # Create small test table (single column family, 100 rows)
    print("2. Creating cdc_test table...")
    connector.execute_sql("""
        CREATE TABLE cdc_test (
            id INT PRIMARY KEY,
            name STRING,
            value INT,
            data STRING,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    print("   ✅ Table created")
    print()
    
    # Insert 100 rows
    print("3. Inserting 100 test rows...")
    connector.execute_sql("""
        INSERT INTO cdc_test (id, name, value, data)
        SELECT 
            i,
            'test_' || i::string,
            i * 10,
            'initial_data_' || i::string
        FROM generate_series(1, 100) AS i
    """, commit=True)
    print("   ✅ 100 rows inserted")
    print()
    
    # Verify
    count = connector.get_table_row_count('cdc_test')
    print(f"4. Verification:")
    print(f"   ✅ Table has {count} rows")
    print()
    
    print("✅ Setup complete!")
    print()
    print("📊 Table details:")
    print("   - Name: cdc_test")
    print("   - Rows: 100")
    print("   - Column families: 1 (single)")
    print("   - Expected CDC events: ~100 for initial scan")
    print("   - Expected scan time: 5-10 seconds")
    print()
    print("🚀 Ready for CDC testing:")
    print("   ./test_azure_cdc_small.sh parquet manual")
    print("   ./test_azure_cdc_small.sh json manual")


if __name__ == '__main__':
    main()

