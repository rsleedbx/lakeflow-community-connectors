# CDC Operation Detection Logic - Documented Duplication

## Overview

The CDC operation detection logic exists in **two places** in the codebase:

1. **Production Path** (Spark Streaming): `_add_cdc_metadata_to_dataframe()` - Line ~1098
2. **Test Path** (Analysis): `analyze_azure_changefeed_files()` - Lines ~3116 (Parquet) and ~3283 (JSON)

**This duplication is intentional and documented to avoid premature abstraction.**

## Why Duplication is Acceptable Here

1. **Different Contexts**: 
   - Production uses Spark SQL DataFrame transformations
   - Test path uses Python dictionaries and Azure blob iteration
   
2. **Different Trade-offs**:
   - Production optimized for distributed processing
   - Test path optimized for direct file analysis and debugging
   
3. **Stability**: Both implementations are stable and well-tested

4. **Clarity**: Keeping them separate is actually MORE readable than a complex shared abstraction

## The Rules (MUST be kept in sync!)

### Core CDC Detection Logic:

#### For Parquet Format:
```
Event Type 'c' + timestamp <= cutoff → SNAPSHOT
Event Type 'c' + timestamp > cutoff  → UPDATE
Event Type 'd'                       → DELETE
Event Type 'i'                       → INSERT (rare in Parquet)
```

#### For JSON Format (Wrapped Envelope):
```
after && !before + timestamp <= cutoff → SNAPSHOT
after && !before + timestamp > cutoff  → INSERT
after && before                        → UPDATE
!after && before                       → DELETE
```

## Where to Find the Code

### 1. Production Path - Spark Streaming

**File**: `cockroachdb.py`  
**Function**: `_add_cdc_metadata_to_dataframe()`  
**Line**: ~1098

```python
# Parquet format detection (Spark SQL)
if snapshot_cutoff:
    df = df.withColumn("_cdc_operation",
        F.when(
            (F.col("__crdb__event_type") == "c") & 
            (F.col("__crdb__updated") <= F.lit(snapshot_cutoff)),
            F.lit("SNAPSHOT")
        )
        .when(
            (F.col("__crdb__event_type") == "c") & 
            (F.col("__crdb__updated") > F.lit(snapshot_cutoff)),
            F.lit("UPDATE")
        )
        .when(F.col("__crdb__event_type") == "i", F.lit("INSERT"))
        .when(F.col("__crdb__event_type") == "d", F.lit("DELETE"))
        .otherwise(F.lit("UNKNOWN"))
    )
```

### 2. Test Path - Parquet Analysis

**File**: `cockroachdb.py`  
**Function**: `analyze_azure_changefeed_files()`  
**Line**: ~3116

```python
# Parquet format detection (Python)
if event_type == 'c':
    if snapshot_cutoff and event_timestamp:
        if event_timestamp <= snapshot_cutoff:
            cdc_operation = 'SNAPSHOT'
        else:
            cdc_operation = 'UPDATE'
    else:
        cdc_operation = 'SNAPSHOT'
elif event_type == 'd':
    cdc_operation = 'DELETE'
```

### 3. Test Path - JSON Analysis

**File**: `cockroachdb.py`  
**Function**: `analyze_azure_changefeed_files()`  
**Line**: ~3283

```python
# JSON format detection (Python)
if after and not before:
    if snapshot_cutoff and event_timestamp:
        if event_timestamp <= snapshot_cutoff:
            cdc_operation = 'SNAPSHOT'
        else:
            cdc_operation = 'INSERT'
    else:
        cdc_operation = 'SNAPSHOT'
    row_data = after
elif after and before:
    cdc_operation = 'UPDATE'
    row_data = after
elif before and not after:
    cdc_operation = 'DELETE'
    row_data = before
```

## How to Maintain This Code

### When Making Changes:

**⚠️ CRITICAL: If you modify CDC detection logic in ONE place, you MUST update ALL THREE locations!**

1. ✅ Update the production Spark path (line ~1098)
2. ✅ Update the test Parquet analysis (line ~3116)
3. ✅ Update the test JSON analysis (line ~3283)
4. ✅ Run full test matrix to verify consistency
5. ✅ Update this documentation if rules change

### Verification Checklist:

- [ ] All three code locations implement the same business rules
- [ ] Test results match expectations (snapshot/insert/update/delete counts)
- [ ] Comments at each location reference the other locations
- [ ] Documentation is up to date

## Why Not Refactor?

The proposed refactoring (see `CODE_DEDUP_REFACTORING_PLAN.md`) would:

**Pros**:
- ✅ Single source of truth
- ✅ Guaranteed consistency

**Cons**:
- ❌ Adds complexity (handling both Spark SQL and Python dict contexts)
- ❌ Makes debugging harder (indirection)
- ❌ Risk of regression during migration
- ❌ Questionable value (code is stable, bugs are rare)

**Decision**: Keep the duplication, document it clearly, and cross-reference the locations.

## Testing Strategy

The test path **validates** that production behavior is correct by:

1. **Reading the same source files** (Azure blob storage)
2. **Applying the same CDC detection rules** (duplicated but documented)
3. **Comparing results** (snapshot/insert/update/delete counts must match)

If production and test paths diverge, tests will fail, alerting us to the inconsistency.

## Future Considerations

If CDC detection becomes more complex (e.g., adding new event types, complex state machines), revisit the refactoring decision. For now, documented duplication is the pragmatic choice.

## Related Documentation

- `CODE_DEDUP_REFACTORING_PLAN.md` - Original refactoring proposal (deferred)
- `JSON_INSERT_DETECTION_FIX.md` - How INSERT detection was added to both paths
- `INSERT_DETECTION_BUG_FIX.md` - Original production path fix

## Summary

**This is intentional, documented technical debt.** The duplication is acceptable because:
- The logic is stable
- The contexts are different (Spark SQL vs Python)
- The risk of divergence is mitigated by:
  - Clear cross-reference comments in the code
  - This documentation
  - Comprehensive test coverage

**If you change CDC detection logic, update ALL THREE locations!**


