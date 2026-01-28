# Changefeed Cleanup Fix

## Problem

Running `test_cdc_matrix.sh` multiple times created duplicate changefeeds:

```
📋 ACTIVE CHANGEFEEDS (Left Running for Testing)

Table: usertable
  Job ID: 1136881884802088961  ← Old run #1
  Job ID: 1138018945932951553  ← Old run #2
  Job ID: 1138018963419496449  ← Old run #3
  ...
  Job ID: 1139228888257986561  ← Latest run
  Job ID: IDs                  ← Parsing bug!

Total: 15 changefeeds ❌ (Expected: 4)

Table: simple_test
  Job ID: 1139226931475709953
  Job ID: 1139227360286736385
  Job ID: 1139229421807894529
  Job ID: 1139229888567345153
  Job ID: IDs                  ← Parsing bug!

Total: 4+ changefeeds ⚠️ (Expected: 4)
```

### Root Causes

1. **No cleanup between test runs** - Each run creates NEW changefeeds without canceling old ones
2. **Design to "leave running for testing"** - Good for notebooks, bad for repeated runs
3. **Parsing bug** - `grep "Job" | awk '{print $2}'` captured "Job IDs:" header text

---

## Solution

### 1. ✅ Auto-Cancel Old Changefeeds Before Tests

Added cleanup step BEFORE test execution:

```bash
# Cancel all existing changefeeds for test tables before starting
echo "🧹 Cleaning up old changefeeds from previous test runs..."
echo ""
for table in "${TABLES[@]}"; do
    echo "  Checking $table changefeeds..."
    old_jobs=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" find-changefeeds \
        --table "$table" \
        --json "$CRDB_JSON" 2>&1 | grep -oE "Job [0-9]+" | awk '{print $2}' || echo "")
    
    if [ -n "$old_jobs" ]; then
        job_count=$(echo "$old_jobs" | wc -w | tr -d ' ')
        echo "    Found $job_count old changefeed(s), cancelling..."
        for job_id in $old_jobs; do
            echo "      Cancelling Job $job_id..."
            python3 "$SCRIPTS_DIR/changefeed_helper.py" cancel-changefeed \
                --job-id "$job_id" \
                --json "$CRDB_JSON" >/dev/null 2>&1
        done
    else
        echo "    No old changefeeds found"
    fi
done
echo ""
echo "✅ Cleanup complete"
```

### 2. ✅ Fixed Parsing Bug

**Old (Broken):**
```bash
jobs=$(... | grep "Job" | awk '{print $2}' | tr -d ':')
# Captured: "Job IDs:" header → output "IDs"
```

**New (Fixed):**
```bash
jobs=$(... | grep -oE "Job [0-9]+" | awk '{print $2}')
# Only captures: "Job 1234567890" → output "1234567890"
```

### 3. ✅ Added Changefeed Count

Shows how many changefeeds exist per table:

```bash
Table: usertable (4 changefeeds)  ← Now shows count!
  Job ID: 1139228062534008833
  Job ID: 1139228888257986561
  Job ID: 1139225561274318849
  Job ID: 1139226385633116161
```

---

## Expected New Output

### Before First Test Run (Fresh State)

```
🧹 Cleaning up old changefeeds from previous test runs...

  Checking usertable changefeeds...
    No old changefeeds found
  Checking simple_test changefeeds...
    No old changefeeds found

✅ Cleanup complete
```

### Before Second Test Run (Cleanup Old Ones)

```
🧹 Cleaning up old changefeeds from previous test runs...

  Checking usertable changefeeds...
    Found 4 old changefeed(s), cancelling...
      Cancelling Job 1139228062534008833...
      Cancelling Job 1139228888257986561...
      Cancelling Job 1139225561274318849...
      Cancelling Job 1139226385633116161...
  Checking simple_test changefeeds...
    Found 4 old changefeed(s), cancelling...
      Cancelling Job 1139226931475709953...
      Cancelling Job 1139227360286736385...
      Cancelling Job 1139229421807894529...
      Cancelling Job 1139229888567345153...

✅ Cleanup complete
```

### After Test Completion

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 ACTIVE CHANGEFEEDS (Left Running for Testing)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Table: usertable (4 changefeeds)  ← Exactly 4! ✅
  Job ID: 1139230123456789012
  Job ID: 1139230234567890123
  Job ID: 1139230345678901234
  Job ID: 1139230456789012345

Table: simple_test (4 changefeeds)  ← Exactly 4! ✅
  Job ID: 1139230567890123456
  Job ID: 1139230678901234567
  Job ID: 1139230789012345678
  Job ID: 1139230890123456789

Total: 8 changefeeds (2 formats × 2 tables × 2 split options) ✅
```

---

## Benefits

### Before Fix
- ❌ 15+ duplicate changefeeds for usertable
- ❌ 4+ duplicate changefeeds for simple_test
- ❌ "Job ID: IDs" parsing error
- ❌ Wasted CockroachDB resources
- ❌ Confusing output

### After Fix
- ✅ Exactly 4 changefeeds for usertable
- ✅ Exactly 4 changefeeds for simple_test
- ✅ Clean, accurate output
- ✅ Auto-cleanup on each run
- ✅ Resource-efficient
- ✅ Shows changefeed counts

---

## Manual Cleanup (If Needed)

If you still have old changefeeds from before this fix:

```bash
# List all changefeeds for a table
python3 sources/cockroachdb/scripts/changefeed_helper.py find-changefeeds \
  --table usertable \
  --json sources/cockroachdb/.env/cockroachdb_credentials.json

# Cancel specific changefeed
python3 sources/cockroachdb/scripts/changefeed_helper.py cancel-changefeed \
  --job-id 1136881884802088961 \
  --json sources/cockroachdb/.env/cockroachdb_credentials.json

# Or cancel ALL changefeeds for usertable (nuclear option)
for job_id in $(python3 sources/cockroachdb/scripts/changefeed_helper.py find-changefeeds \
  --table usertable \
  --json sources/cockroachdb/.env/cockroachdb_credentials.json 2>&1 | \
  grep -oE "Job [0-9]+" | awk '{print $2}'); do
    echo "Cancelling $job_id..."
    python3 sources/cockroachdb/scripts/changefeed_helper.py cancel-changefeed \
      --job-id "$job_id" \
      --json sources/cockroachdb/.env/cockroachdb_credentials.json
done
```

---

## Testing

```bash
# Run test matrix twice to verify cleanup works
cd sources/cockroachdb/scripts

# First run
./test_cdc_matrix.sh
# Should show: "No old changefeeds found"
# Should end with: 8 changefeeds total

# Second run
./test_cdc_matrix.sh
# Should show: "Found 8 old changefeed(s), cancelling..."
# Should end with: 8 new changefeeds total (old ones cancelled)
```

---

## Related Fixes

This fix works together with:
1. ✅ **grep -P fix** (TEST_MATRIX_FIXES.md) - macOS compatibility
2. ✅ **JSON sync fix** (JSON_SYNC_FIX.md) - Multi-format support
3. ✅ **Parallel delete** (cockroachdb.py) - Fast checkpoint cleanup
4. ✅ **Real volume paths** - No more `{catalog}` placeholders

**All test matrix issues are now resolved!** 🎉


