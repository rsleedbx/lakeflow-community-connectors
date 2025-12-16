# Bug Fix: Operation Detection (INSERT/UPDATE/DELETE)

## Issue
All changefeed events were incorrectly detected as `DELETE` operations, showing `_cdc_operation: DELETE` even for INSERT events.

```
📊 Operation Statistics:
   DELETE: 50  ❌ WRONG - should be INSERT
```

## Root Cause

**Two bugs were found:**

### 1. memoryview Object Handling

**Problem:** CockroachDB's `psycopg2` driver returns `bytea` columns as Python `memoryview` objects, not strings.

**Impact:** The JSON parsing code called `.strip()` on memoryview, which raised `AttributeError`, causing the exception handler to set `value = None`. When `value = None`, the operation detector classified it as DELETE.

**Code Before (Broken):**
```python
try:
    value = json.loads(value_json) if (value_json is not None and value_json.strip() != '') else None
except (json.JSONDecodeError, AttributeError):
    value = None  # ← memoryview.strip() raises AttributeError, so value becomes None
```

**Code After (Fixed):**
```python
def parse_json_column(json_data):
    """Parse JSON from string, bytes, or memoryview."""
    if json_data is None:
        return None
    
    # Convert memoryview or bytes to string
    if isinstance(json_data, memoryview):
        json_data = json_data.tobytes().decode('utf-8')
    elif isinstance(json_data, bytes):
        json_data = json_data.decode('utf-8')
    
    # Check for empty or whitespace-only strings
    if not isinstance(json_data, str) or json_data.strip() == '':
        return None
    
    try:
        return json.loads(json_data)
    except json.JSONDecodeError:
        return None

key = parse_json_column(key_json) or []
value = parse_json_column(value_json)
```

---

### 2. Wrong Column Index Mapping

**Problem:** CockroachDB changefeeds return a `table` column as the first column, but the code was assuming `key` was the first column.

**Cursor Format:** `['table', 'key', 'value']` or `['table', 'key', 'value', 'updated', 'topic']`

**Impact:** The code was reading:
- `row[0]` as `key` (but it's actually the table name!)
- `row[1]` as `value` (but it's actually the key!)
- `row[2]` as `updated` (but it's actually the value!)

This caused:
1. `key_json` to be the table name string (e.g., `"events"`)
2. `value_json` to be the key JSON (e.g., `[1133243426600321025]`)
3. The actual value (with `{"after": {...}}`) was never read

**Code Before (Broken):**
```python
if has_updated:
    # ❌ WRONG: Assuming row[0] is key
    key_json = row[0]
    value_json = row[1]
    updated = row[2]
    topic = row[3] if len(row) > 3 else None
else:
    key_json = row[0]
    value_json = row[1]
```

**Code After (Fixed):**
```python
if has_updated:
    # ✅ CORRECT: row[0] is table name, row[1] is key
    table_name = row[0]
    key_json = row[1]
    value_json = row[2]
    updated = row[3]
    topic = row[4] if len(row) > 4 else None
else:
    table_name = row[0]
    key_json = row[1]
    value_json = row[2]
```

---

## Test Results

### Before Fix ❌
```
📊 Operation Statistics:
   DELETE: 50

First record (sample):
   _cdc_key: []
   _cdc_updated: None
   _cdc_operation: DELETE
```

### After Fix ✅
```
📊 Operation Statistics:
   INSERT: 50

First record (sample):
   _cdc_key: [1133243426600321025]
   _cdc_updated: None
   _cdc_operation: INSERT
   created_at: 2025-12-16T18:14:08.05079Z
   data: {'ip': '192.168.1.1'}
   event_type: login
   id: 1133243426600321025
   user_id: 1
```

---

## How the Fix Was Discovered

1. **User Request:** "Print the number of INSERT/UPDATE/DELETE operations from each `read_table` call"
2. **Implementation:** Added operation statistics counter in `test_local.py`
3. **Unexpected Result:** All 50 records showed as DELETE (expected INSERT)
4. **Debug Investigation:**
   - Added `DEBUG_CHANGEFEED` environment variable
   - Printed `value_json` type: discovered `memoryview` instead of `str`
   - Printed cursor description: discovered `['table', 'key', 'value']` column order
   - Printed parsed values: confirmed wrong column mapping
5. **Root Cause:** Two separate bugs compounding the issue
6. **Fix:** Handle memoryview objects + correct column index mapping
7. **Verification:** Test now shows `INSERT: 50` ✅

---

## Impact

**Before:** 
- ❌ All operations incorrectly detected as DELETE
- ❌ No row data extracted (only empty keys)
- ❌ CDC functionality broken

**After:**
- ✅ Operations correctly detected (INSERT/UPDATE/DELETE)
- ✅ Full row data extracted with all columns
- ✅ CDC functionality working correctly

---

## Lessons Learned

1. **Database drivers have quirks:** `psycopg2` returns `bytea` as `memoryview`, not `str`
2. **Column order matters:** Always check cursor description, don't assume
3. **Debug output is invaluable:** The debug prints quickly identified both bugs
4. **Test with real data:** The operation statistics feature immediately surfaced the bug
5. **Compound bugs exist:** Two separate issues (memoryview + wrong indices) masked each other

---

## Files Changed

1. **`cockroachdb.py`**
   - Added `parse_json_column()` function to handle memoryview/bytes/string
   - Fixed column index mapping in `event_generator()`
   - Removed temporary debug output

2. **`test_local.py`**
   - Added operation statistics counter (INSERT/UPDATE/DELETE)
   - Displays counts after each `read_table()` call
   - Enhanced test output for better visibility

---

## Verification Commands

```bash
# Run test and verify operation statistics
python test_local.py --duration 30

# Expected output:
# 📊 Operation Statistics:
#    INSERT: 50

# Manual verification via SQL
cockroach sql --insecure -d ycsb -e \
  "EXPERIMENTAL CHANGEFEED FOR events 
   WITH initial_scan='only', split_column_families;" | head -5
```

---

## Future Improvements

1. **Add unit tests** for `parse_json_column()` with memoryview inputs
2. **Add integration test** that verifies operation detection accuracy
3. **Document psycopg2 quirks** in README for future contributors
4. **Consider using dict cursor** to avoid column index issues: `cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)`

---

## Related Issues

- GitHub Issue #16 (Pydantic/dataclass serialization)
- User request: "Print number of INSERT/UPDATE/DELETE from each read_table call"

This bug was discovered as a direct result of implementing the operation statistics feature requested by the user! 🎉

