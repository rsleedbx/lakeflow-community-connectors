#!/usr/bin/env bash

# Test script for CockroachDB dual-mode connector
# Tests both direct and Azure Parquet modes

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_NAME="cockroachdb"

echo "════════════════════════════════════════════════════════════════"
echo "🧪 Testing CockroachDB Dual-Mode Connector"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check environment
echo "📋 Environment Check:"
echo "   Git root: $(git rev-parse --show-toplevel)"
echo "   Source dir: $SCRIPT_DIR"
echo ""

# Test 1: Direct Mode (No Azure credentials)
echo "════════════════════════════════════════════════════════════════"
echo "Test 1: Direct Mode (No Azure Credentials)"
echo "════════════════════════════════════════════════════════════════"
echo ""

python3 << 'EOF'
import sys
sys.path.insert(0, 'sources/cockroachdb')
from cockroachdb import LakeflowConnect

# Test with only CockroachDB credentials
options = {
    'host': 'battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud',
    'port': '26257',
    'database': 'ycsb',
    'user': 'rslee',
    'password': 'test',  # Not real
    'sslmode': 'require'
}

print("Creating connector with CockroachDB credentials only...")
connector = LakeflowConnect(options)

print(f"\n✅ Mode detected: {connector.mode}")

if connector.mode == "direct":
    print("✅ TEST PASSED: Direct mode correctly detected")
else:
    print(f"❌ TEST FAILED: Expected 'direct', got '{connector.mode}'")
    sys.exit(1)

EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Test 1 PASSED: Direct mode detection"
else
    echo ""
    echo "❌ Test 1 FAILED"
    exit 1
fi

echo ""
echo ""

# Test 2: Azure Parquet Mode (With Azure credentials)
echo "════════════════════════════════════════════════════════════════"
echo "Test 2: Azure Parquet Mode (With Azure Credentials)"
echo "════════════════════════════════════════════════════════════════"
echo ""

python3 << 'EOF'
import sys
sys.path.insert(0, 'sources/cockroachdb')
from cockroachdb import LakeflowConnect

# Test with CockroachDB + Azure credentials
options = {
    'host': 'battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud',
    'port': '26257',
    'database': 'ycsb',
    'user': 'rslee',
    'password': 'test',  # Not real
    'sslmode': 'require',
    
    # Azure credentials (triggers Azure Parquet mode)
    'azure_account_name': 'testaccount',
    'azure_account_key': 'test_key',
    'azure_container': 'changefeed-events'
}

print("Creating connector with CockroachDB + Azure credentials...")
connector = LakeflowConnect(options)

print(f"\n✅ Mode detected: {connector.mode}")

if connector.mode == "azure_parquet":
    print("✅ TEST PASSED: Azure Parquet mode correctly detected")
    print(f"   Account: {connector.azure_account_name}")
    print(f"   Container: {connector.azure_container}")
    print(f"   Path Prefix: {connector.azure_path_prefix}")
else:
    print(f"❌ TEST FAILED: Expected 'azure_parquet', got '{connector.mode}'")
    sys.exit(1)

EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Test 2 PASSED: Azure Parquet mode detection"
else
    echo ""
    echo "❌ Test 2 FAILED"
    exit 1
fi

echo ""
echo ""

# Test 3: Partial Azure credentials (should fall back to direct mode)
echo "════════════════════════════════════════════════════════════════"
echo "Test 3: Partial Azure Credentials (Fallback to Direct)"
echo "════════════════════════════════════════════════════════════════"
echo ""

python3 << 'EOF'
import sys
sys.path.insert(0, 'sources/cockroachdb')
from cockroachdb import LakeflowConnect

# Test with only partial Azure credentials (missing container)
options = {
    'host': 'battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud',
    'port': '26257',
    'database': 'ycsb',
    'user': 'rslee',
    'password': 'test',  # Not real
    'sslmode': 'require',
    
    # Incomplete Azure credentials (missing container)
    'azure_account_name': 'testaccount',
    'azure_account_key': 'test_key'
    # Missing: azure_container
}

print("Creating connector with incomplete Azure credentials...")
connector = LakeflowConnect(options)

print(f"\n✅ Mode detected: {connector.mode}")

if connector.mode == "direct":
    print("✅ TEST PASSED: Correctly fell back to direct mode")
else:
    print(f"❌ TEST FAILED: Expected 'direct', got '{connector.mode}'")
    sys.exit(1)

EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Test 3 PASSED: Fallback to direct mode"
else
    echo ""
    echo "❌ Test 3 FAILED"
    exit 1
fi

echo ""
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ ALL TESTS PASSED"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Summary:"
echo "  ✅ Direct mode detection: PASSED"
echo "  ✅ Azure Parquet mode detection: PASSED"
echo "  ✅ Fallback to direct mode: PASSED"
echo ""
echo "The dual-mode connector is working correctly!"
echo "════════════════════════════════════════════════════════════════"


