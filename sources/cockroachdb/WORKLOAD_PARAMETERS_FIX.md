# Workload Parameters Fix - Eliminating Hardcoded Assumptions

## Problem

The `test_cdc_matrix.sh` script had hardcoded expected values for validation that were duplicated across the script and disconnected from the actual workload generation parameters.

### Before (Issues)

1. **Hardcoded expected values** (lines 501-504):
   ```bash
   local expected_unique_keys=9950  # Hardcoded!
   if [[ "$base_table" == "simple_test" ]]; then
       expected_unique_keys=950     # Hardcoded!
   fi
   ```

2. **Hardcoded workload sizes** scattered throughout:
   - Initial rows: `--rows 1000` and `--rows 10000`
   - Updates: `--rows 400`
   - Inserts: `--rows 50`
   - Deletes: `--rows 100`

3. **Calculation not explicit**: Expected values weren't calculated, making it hard to verify correctness

**Problems:**
- ❌ Values could get out of sync if workload changes
- ❌ Magic numbers with no explanation
- ❌ High maintenance burden
- ❌ Easy to introduce bugs

## Solution

Centralized all workload parameters as constants at the top of the script and made all calculations explicit.

### After (Fixed)

**1. Constants at top of script (lines 18-24):**
```bash
# Workload parameters (used for both test execution and validation)
# These must match the values used in generate-*-sql commands
SIMPLE_TEST_INITIAL_ROWS=1000
USERTABLE_INITIAL_ROWS=10000
WORKLOAD_UPDATE_COUNT=400
WORKLOAD_INSERT_COUNT=50
WORKLOAD_DELETE_COUNT=100
```

**2. Calculated expected values (lines 508-521):**
```bash
# Calculate expected values from workload parameters (not hardcoded!)
local initial_rows
if [[ "$base_table" == "simple_test" ]]; then
    initial_rows=$SIMPLE_TEST_INITIAL_ROWS
else
    initial_rows=$USERTABLE_INITIAL_ROWS
fi

# Expected unique keys = initial + inserts - deletes
# (updates don't change key count, they just modify existing rows)
local expected_unique_keys=$((initial_rows + WORKLOAD_INSERT_COUNT - WORKLOAD_DELETE_COUNT))

echo ""
echo "📊 Expected counts (from workload parameters):"
echo "  Initial rows: $initial_rows"
echo "  + Inserts: $WORKLOAD_INSERT_COUNT"
echo "  - Deletes: $WORKLOAD_DELETE_COUNT"
echo "  = Expected unique keys: $expected_unique_keys"
```

**3. All workload generation uses constants:**
```bash
# Table creation
--rows $SIMPLE_TEST_INITIAL_ROWS
--rows $USERTABLE_INITIAL_ROWS

# Updates
--rows $WORKLOAD_UPDATE_COUNT

# Inserts
--rows $WORKLOAD_INSERT_COUNT

# Deletes
--rows $WORKLOAD_DELETE_COUNT
```

## Benefits

### ✅ Single Source of Truth
- Change workload size in ONE place (top of script)
- All calculations and validations update automatically

### ✅ Self-Documenting
- Constants have descriptive names
- Calculation formula is explicit
- Comments explain the math

### ✅ Maintainability
- Easy to adjust workload sizes for different test scenarios
- No risk of forgetting to update validation logic
- Clear relationship between generation and validation

### ✅ Correctness
- Formula shows exactly what we expect:
  ```
  unique_keys = initial_rows + inserts - deletes
  ```
- Easy to verify the math is correct

## Example: Changing Workload Size

**Before:** Would need to change values in ~10 places throughout script

**After:** Change ONE constant:
```bash
# Want to test with larger workload?
USERTABLE_INITIAL_ROWS=100000  # Change here
WORKLOAD_UPDATE_COUNT=4000     # And here
WORKLOAD_INSERT_COUNT=500      # And here
WORKLOAD_DELETE_COUNT=1000     # And here

# Everything else updates automatically!
```

## Validation Example

### Simple Test
```
Initial rows: 1000
+ Inserts: 50
- Deletes: 100
= Expected unique keys: 950  ✅
```

### Usertable
```
Initial rows: 10000
+ Inserts: 50
- Deletes: 100
= Expected unique keys: 9950  ✅
```

## Files Modified

1. **test_cdc_matrix.sh**:
   - Lines 18-24: Added workload parameter constants
   - Lines 508-521: Calculate expected values from constants
   - Lines 600-641: Use constants for table creation
   - Lines 756-819: Use constants for workload execution

## Testing

Run the script as normal:
```bash
# Full test with new parameterized approach
./test_cdc_matrix.sh

# Validation mode also uses calculated expectations
./test_cdc_matrix.sh --validate-only
```

The script will now:
1. Show the calculation explicitly in output
2. Validate against calculated expectations
3. Make it clear if actual counts don't match formula

---

**Status:** ✅ Fixed  
**Date:** January 8, 2026  
**Impact:** Eliminated all hardcoded assumptions, improved maintainability  
**Breaking Changes:** None (values remain the same, just parameterized)

