# Local Testing for pg8000 Connection

## Quick Start

```bash
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb

# Make sure $COCKROACHDB_URL is set (you already have this)
echo $COCKROACHDB_URL

# Run the test
bash test_pg8000_simple.sh
```

## What the Test Does

The test script will:
1. ✅ Connect to CockroachDB using pg8000 (same as Databricks pipeline)
2. ✅ Test queries WITHOUT parameters
3. ❌ Test queries with `%s` placeholders (psycopg2 style) - should FAIL
4. ✅ Test queries with `$1` placeholders (pg8000 style) - should PASS
5. ✅ Test queries with tuple unpacking `*params` - should PASS

## Expected Output

You should see which parameter style works correctly with pg8000.

## Current Bug Analysis

The Databricks error shows:
```
IndexError: list index out of range
```

This happens in `make_vals(params)` when pg8000 tries to extract parameters.

**Possible Issues:**
1. **Parameter count mismatch**: We convert `%s` to `$1`, but the parameter count doesn't match
2. **Incorrect unpacking**: We do `*params` but pg8000 expects something else
3. **Query parsing issue**: pg8000 can't parse our converted SQL

## What to Look For

Run the test and check:
- Does Test 3 (with `$1`) work?
- Does Test 5 (with `*params` unpacking) work?
- If both work, the bug is in our conversion logic
- If Test 5 fails, we need to change how we pass parameters

## Debug Output

The test will show exactly what pg8000 expects for each case.


