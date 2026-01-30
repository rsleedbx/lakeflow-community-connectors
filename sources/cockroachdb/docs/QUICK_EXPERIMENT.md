# Quick Experiment - Azure Path Fix

## Problem
CockroachDB changefeed writes files to:
```
parquet/db/schema/table/
```

But we need:
```
parquet/db/schema/table/target_table/
```

---

## Quick Test (Add to Notebook)

```python
from cockroachdb_experiments import test_azure_path_prefix

results = test_azure_path_prefix(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    column_family_mode, get_cockroachdb_connection, storage_account_key_encoded
)

# Check result
print(f"\n{'✅ SUCCESS' if results['success'] else '❌ FAILED'}")
print(f"Job ID: {results['test_job_id']}")
```

---

## What It Tests

Tests if CockroachDB supports `path_prefix` query parameter for Azure:

**Current (not working)**:
```
azure://container/parquet/db/schema/table/target/?AZURE_ACCOUNT_NAME=...
```

**Test (S3-style)**:
```
azure://container/?path_prefix=parquet/db/schema/table/target&AZURE_ACCOUNT_NAME=...
```

---

## Results

- **✅ If SUCCESS**: Files appear at full path → Use `path_prefix` permanently
- **❌ If FAILED**: Files at short path → Try alternative solutions
- **⚠️ If NONE**: Wait longer or check changefeed status

See `EXPERIMENT_USAGE.md` for detailed instructions.

---

## Files Created

1. **`cockroachdb_experiments.py`** - Test functions
2. **`EXPERIMENT_USAGE.md`** - Detailed usage guide
3. **`AZURE_PATH_TEST_PLAN.md`** - Alternative test strategies
4. **`TEST_AZURE_PATH_PREFIX.md`** - Original test documentation
5. **`QUICK_EXPERIMENT.md`** - This file (quick reference)

---

## Date
2026-01-30
