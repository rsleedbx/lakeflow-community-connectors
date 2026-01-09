# JSON Double Count Investigation 🔬

**Date:** January 7, 2026  
**Issue:** `json_usertable_no_split` showing `upd=800` instead of expected `upd=400`  
**Status:** 🔍 **Under Investigation**

---

## Problem Statement

One specific test scenario shows exactly 2× the expected UPDATE count:

```
Test 2: json_usertable_no_split - snap=18644 ins=0 upd=800 del=200
                                                      ^^^^ Expected: 400
```

**Expected workload:**
- UPDATE 400 rows
- DELETE 100 rows  
- INSERT 50 rows

**Actual statistics:**
- Updates: 800 (exactly 2× expected) ❌
- Deletes: 200 (exactly 2× expected) ❌
- Inserts: 0 (bug fixed separately)

---

## Observations

### ✅ Working Correctly:

| Test | Format | Table | Split | Updates | Status |
|------|--------|-------|-------|---------|--------|
| Test 1 | JSON | usertable | **WITH** split | 400 | ✅ Correct |
| Test 3 | JSON | simple_test | WITH split | 400 | ✅ Correct |
| Test 4 | JSON | simple_test | **NO** split | 400 | ✅ Correct |
| Test 5 | Parquet | usertable | WITH split | 450 | ✅ Reasonable |
| Test 6 | Parquet | usertable | **NO** split | 450 | ✅ Reasonable |
| Test 7 | Parquet | simple_test | WITH split | 450 | ✅ Reasonable |
| Test 8 | Parquet | simple_test | **NO** split | 450 | ✅ Reasonable |

### ❌ Issue Specific To:

- **Format:** JSON only (not Parquet)
- **Table:** usertable only (not simple_test)  
- **Mode:** NO split only (not WITH split)
- **Pattern:** Exactly 2× expected for both updates AND deletes

---

## Key Differences

### usertable vs simple_test

**usertable** has TWO column families (from `cockroachdb.py` line 2600):
```sql
CREATE TABLE usertable (
    ycsb_key VARCHAR(255) PRIMARY KEY,
    field0 TEXT, field1 TEXT, ..., field9 TEXT,
    FAMILY pk (ycsb_key),           -- Family 1: Primary Key
    FAMILY data (field0-field9)      -- Family 2: Data columns
);
```

**simple_test** has NO explicit column families:
```sql
CREATE TABLE simple_test (
    id BIGINT PRIMARY KEY,
    name TEXT,
    value BIGINT
    -- Single default family
);
```

---

## Hypothesis

### CockroachDB Changefeed Behavior

**With `split_column_families=true`:**
- Explicitly outputs separate events/files per column family
- JSON: Emits to separate files (one per family)
- Deduplication works correctly (upd=400 ✅)

**With `split_column_families=false` (or unspecified):**
- **Parquet**: Emits fragments in same file, properly merged
- **JSON**: Hypothesis - Still emits 2 events per logical UPDATE?
  - One event for PK family
  - One event for data family
  - Both events have complete PK (ycsb_key)
  - Both pass the "has complete PK" filter (line 3249)
  - Result: Each UPDATE counted twice

### Why Deduplication Fails

Current deduplication logic (`_coalesce_events_by_key`, line 793):
1. Groups events by `_cdc_key` (primary key values)
2. Merges column values using last-non-null semantics
3. Returns ONE event per unique PK

**This SHOULD work** - so why doesn't it?

**Possible explanations:**
1. Events have slightly different PK values (typo/encoding issue)?
2. Events processed in separate batches (unlikely)?
3. Deduplication happens per-file instead of globally?
4. Events have different timestamps causing them to be seen as different?

---

## Diagnostic Steps

### Step 1: Run Diagnostic Script

Created: `sources/cockroachdb/scripts/diagnose_json_double_count.py`

This script samples JSON events and checks for:
- Multiple UPDATE events per primary key
- Different column sets (column family indicator)
- Timing differences between events
- Identical vs distinct event content

**Usage:**
```bash
cd sources/cockroachdb/scripts

# Load Azure credentials
source <(jq -r 'to_entries|map("azure_\(.key)=\(.value|tostring)")|.[]' \
    ../../../.env/cockroachdb_cdc_azure.json)

# Run diagnostic
python3 diagnose_json_double_count.py \
    "$azure_azure_storage_account" \
    "$azure_azure_storage_key" \
    "changefeed-events" \
    "json/defaultdb/public/test-json_usertable_no_split" \
    50
```

**What to look for:**
- "❌ Found X keys with multiple UPDATE events" → Confirms duplicate emission
- "COLUMN FAMILY FRAGMENTS DETECTED" → Shows events have different column sets
- "Average events per key: 2.00" → Confirms 2× count

---

### Step 2: Manual Azure Inspection

```bash
# List JSON files
az storage blob list \
    --account-name "$azure_azure_storage_account" \
    --account-key "$azure_azure_storage_key" \
    --container-name "changefeed-events" \
    --prefix "json/defaultdb/public/test-json_usertable_no_split/" \
    --query "[?ends_with(name, '.ndjson') || ends_with(name, '.json')].[name]" \
    --output table

# Download a sample file
az storage blob download \
    --account-name "$azure_azure_storage_account" \
    --account-key "$azure_azure_storage_key" \
    --container-name "changefeed-events" \
    --name "json/.../FILENAME.ndjson" \
    --file /tmp/sample.ndjson

# Count events per primary key
cat /tmp/sample.ndjson | jq -r '.after.ycsb_key' | sort | uniq -c | sort -rn | head -20
```

**Expected for double-count issue:**
```
  2 user00000001
  2 user00000002
  2 user00000003
  ...
```

**Expected for correct behavior:**
```
  1 user00000001
  1 user00000002
  1 user00000003
  ...
```

---

### Step 3: Compare Event Structure

```bash
# Extract 2 events for same key
cat /tmp/sample.ndjson | grep '"ycsb_key":"user00000001"' | head -2 > /tmp/two_events.json

# Compare column sets
echo "Event 1 columns:"
cat /tmp/two_events.json | head -1 | jq '.after | keys'

echo "Event 2 columns:"
cat /tmp/two_events.json | tail -1 | jq '.after | keys'
```

**If column family issue:**
```
Event 1 columns: ["ycsb_key"]                    # PK family only
Event 2 columns: ["field0", "field1", ..., "ycsb_key"]  # Data family
```

**If duplicate issue:**
```
Event 1 columns: ["ycsb_key", "field0", "field1", ...]  # Complete row
Event 2 columns: ["ycsb_key", "field0", "field1", ...]  # Duplicate!
```

---

## Code Analysis

### Current Deduplication Flow

**Location:** `cockroachdb.py` lines 3209-3290

```python
# Step 1: Collect all events from all files
all_events = []
for blob_name in data_blobs:
    # Read JSON file
    for line in content.strip().split('\n'):
        event_data = json.loads(line)
        
        # Classify operation
        if after and before:
            cdc_operation = 'UPDATE'
        
        # Extract PK
        cdc_key_pairs = [(pk_col, row_data[pk_col]) for pk_col in primary_key_columns]
        
        # Skip fragments without complete PK
        if len(cdc_key_pairs) < len(primary_key_columns):
            continue  # ← Should filter incomplete fragments
        
        # Add to collection
        event = {
            **row_data,
            '_cdc_key': cdc_key_pairs,
            '_cdc_operation': cdc_operation,
            ...
        }
        all_events.append(event)

# Step 2: Deduplicate by primary key
connector = LakeflowConnect({})
coalesced_events = connector._coalesce_events_by_key(all_events)  # ← Should merge duplicates

# Step 3: Count
for event in coalesced_events:
    total_stats[operation] += 1  # ← Should count deduplicated events only
```

**This logic SHOULD work!** So the issue must be:
1. Events aren't being grouped correctly by `_cdc_key`
2. `_coalesce_events_by_key()` has a bug for JSON format
3. Events are somehow seen as having different PKs

---

## Potential Fixes

### Option 1: Enhance Column Family Detection for JSON

If diagnostic shows events have different column sets:

```python
# In analyze_azure_changefeed_files(), after line 3244:

# For JSON with column families: Detect fragments by column set differences
if format_type == 'json' and len(cdc_key_pairs) == len(primary_key_columns):
    # Check if this looks like a column family fragment
    # (has PK but missing many expected data columns)
    expected_columns = set(row_data.keys())  # Get from first complete event
    actual_columns = set(row_data.keys())
    
    # If event has significantly fewer columns, it's likely a fragment
    if len(actual_columns) < len(expected_columns) * 0.5:
        if debug:
            print(f"   Skipping JSON column family fragment: {blob_name}")
        continue
```

### Option 2: Force Deduplication by Timestamp

If events are identical but duplicated:

```python
# Add timestamp to _cdc_key for finer-grained deduplication
event = {
    **row_data,
    '_cdc_key': cdc_key_pairs,
    '_cdc_updated': event_data.get('updated', ''),  # ← Use for deduplication
    '_cdc_operation': cdc_operation,
    ...
}

# Then in _coalesce_events_by_key(), group by PK + timestamp window
# to merge events that happen within same microsecond
```

### Option 3: Investigate CockroachDB Changefeed Configuration

Check if `json_usertable_no_split` changefeed was created with different options than expected:

```sql
-- Check changefeed options
SELECT job_id, description 
FROM [SHOW JOBS] 
WHERE job_type = 'CHANGEFEED' 
  AND description LIKE '%usertable%';
```

Look for any unexpected options that might cause duplicate emission.

---

## Expected Resolution

Once diagnosed:

1. **If column family fragments:** Add JSON-specific fragment detection logic
2. **If true duplicates:** Investigate CockroachDB behavior and add deduplication safeguards
3. **If PK mismatch:** Fix key extraction logic

**Target outcome:**
```
Test 2: json_usertable_no_split - snap=~10000 ins=50 upd=400 del=100
                                                          ^^^^ Fixed!
```

---

## Next Steps

1. ✅ Created diagnostic script
2. 🔄 Run diagnostic on actual Azure data
3. ⏳ Analyze results and determine root cause
4. ⏳ Implement targeted fix
5. ⏳ Re-run test matrix to verify

---

## Related Issues

- ✅ **INSERT detection bug** - Fixed (separate issue, lines 1112, 1120)
- ⏳ **JSON double count** - Under investigation (this document)
- ✅ **Parquet analysis** - Working correctly

---

**Investigation started:** January 7, 2026  
**Status:** Awaiting diagnostic results from user


