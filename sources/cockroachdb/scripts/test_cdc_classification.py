#!/usr/bin/env python3
"""
Test CDC Classification Logic Locally (No PySpark Required)

Tests the JSON-to-operation classification logic using pure Python.
This simulates what Spark would do.

Usage:
    python test_cdc_classification.py <json_file>
"""

import json
import sys
from pathlib import Path

def classify_cdc_operation(record, snapshot_cutoff_double):
    """
    Classify CDC operation using the same logic as Spark.
    
    This simulates:
        after_empty = (to_json(after) == "null") | (to_json(after) == "{}")
        before_empty = (to_json(before) == "null") | (to_json(before) == "{}")
    """
    after = record.get('after')
    before = record.get('before')
    updated = record.get('updated')
    
    # Simulate F.to_json() behavior
    # CRITICAL: Spark's F.to_json() returns SQL NULL (not string "null") when input is NULL!
    # We use None to represent SQL NULL in Python
    if after is None:
        after_json = None  # Simulates SQL NULL
    else:
        after_json = json.dumps(after)
    
    if before is None:
        before_json = None  # Simulates SQL NULL
    else:
        before_json = json.dumps(before)
    
    # Check if empty (matching Spark logic exactly)
    # Spark: F.col("_after_json").isNull() | (F.col("_after_json") == "null") | (F.col("_after_json") == "{}")
    after_empty = (after_json is None) or (after_json == "null") or (after_json == "{}")
    after_not_empty = not after_empty
    before_empty = (before_json is None) or (before_json == "null") or (before_json == "{}")
    before_not_empty = not before_empty
    
    # Cast updated to double
    try:
        updated_double = float(updated) if updated else None
    except (ValueError, TypeError):
        updated_double = None
    
    # Apply classification logic (same as Spark)
    if after_not_empty and before_empty and updated_double is not None and updated_double <= snapshot_cutoff_double:
        return "SNAPSHOT"
    elif after_not_empty and before_empty and updated_double is not None and updated_double > snapshot_cutoff_double:
        return "INSERT"
    elif after_not_empty and before_not_empty:
        return "UPDATE"
    elif after_empty and before_not_empty:
        return "DELETE"
    else:
        return "UNKNOWN"

def analyze_file(file_path, snapshot_cutoff):
    """Analyze a JSON CDC file and classify all records."""
    
    print("=" * 80)
    print(f"CDC CLASSIFICATION TEST")
    print("=" * 80)
    print(f"File: {file_path}")
    print(f"Snapshot cutoff: {snapshot_cutoff}")
    print()
    
    # Convert snapshot cutoff to double
    try:
        snapshot_cutoff_double = float(snapshot_cutoff)
    except (ValueError, TypeError):
        print(f"❌ Invalid snapshot cutoff: {snapshot_cutoff}")
        return
    
    # Read and classify records
    operations = {
        'SNAPSHOT': [],
        'INSERT': [],
        'UPDATE': [],
        'DELETE': [],
        'UNKNOWN': []
    }
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            
            try:
                record = json.loads(line)
                operation = classify_cdc_operation(record, snapshot_cutoff_double)
                operations[operation].append((line_num, record))
            except json.JSONDecodeError as e:
                print(f"⚠️  Line {line_num}: JSON decode error: {e}")
                continue
    
    # Show results
    print("📊 Operation Counts:")
    print()
    total = sum(len(ops) for ops in operations.values())
    for op, records in operations.items():
        count = len(records)
        pct = (count / total * 100) if total > 0 else 0
        
        if op == 'UNKNOWN' and count > 0:
            status = "❌"
        else:
            status = "✅"
        
        print(f"   {status} {op:12s}: {count:5d} ({pct:5.1f}%)")
    
    print(f"\n   Total: {total:,} records")
    print()
    
    # Show sample UNKNOWN records if any
    if operations['UNKNOWN']:
        print("🔍 Sample UNKNOWN Records (investigating issue):")
        print()
        for i, (line_num, record) in enumerate(operations['UNKNOWN'][:3]):
            after = record.get('after')
            before = record.get('before')
            updated = record.get('updated')
            
            after_json = json.dumps(after) if after is not None else "null"
            before_json = json.dumps(before) if before is not None else "null"
            
            print(f"   Record #{line_num}:")
            print(f"      after_json:  {repr(after_json[:100])}...")
            print(f"      before_json: {repr(before_json[:100])}...")
            print(f"      updated:     {updated}")
            print(f"      after type:  {type(after).__name__}")
            print(f"      before type: {type(before).__name__}")
            print()
            
            # Show the actual conditions
            after_empty = (after_json == "null") or (after_json == "{}")
            before_empty = (before_json == "null") or (before_json == "{}")
            try:
                updated_double = float(updated) if updated else None
            except:
                updated_double = None
            
            print(f"      Conditions:")
            print(f"         after_empty:  {after_empty}")
            print(f"         before_empty: {before_empty}")
            print(f"         updated <= cutoff: {updated_double <= snapshot_cutoff_double if updated_double else 'N/A'}")
            print(f"         updated > cutoff:  {updated_double > snapshot_cutoff_double if updated_double else 'N/A'}")
            print()
    
    # Show sample of each operation type
    print("📋 Sample Records by Operation:")
    print()
    for op in ['SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE']:
        if operations[op]:
            line_num, record = operations[op][0]
            after = record.get('after')
            before = record.get('before')
            updated = record.get('updated')
            
            print(f"   {op}:")
            print(f"      Line: {line_num}")
            print(f"      after:  {'<dict>' if isinstance(after, dict) else repr(after)}")
            print(f"      before: {'<dict>' if isinstance(before, dict) else repr(before)}")
            print(f"      updated: {updated}")
            print()

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_cdc_classification.py <json_file> [snapshot_cutoff]")
        print()
        print("Example:")
        print("  python test_cdc_classification.py .cache/cdc_test_data/json/.../file.ndjson 1767823531335790125.0")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Auto-detect snapshot cutoff if not provided
    if len(sys.argv) > 2:
        snapshot_cutoff = sys.argv[2]
    else:
        # Read first file and get max updated timestamp
        print("🔍 Auto-detecting snapshot cutoff from file...")
        max_updated = None
        with open(file_path, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    updated = record.get('updated')
                    if updated:
                        if max_updated is None or str(updated) > str(max_updated):
                            max_updated = updated
                except:
                    continue
        
        if max_updated:
            snapshot_cutoff = str(max_updated)
            print(f"   ✅ Detected: {snapshot_cutoff}")
            print()
        else:
            print("   ❌ Could not detect snapshot cutoff")
            print("   Please provide it as second argument")
            sys.exit(1)
    
    analyze_file(file_path, snapshot_cutoff)

if __name__ == "__main__":
    main()

