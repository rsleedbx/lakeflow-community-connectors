# Fallback Removal - No Silent Failures

## 🎯 **Objective**

Remove all silent fallbacks that mask bugs. Instead of falling back to defaults, fail explicitly with clear error messages.

## 🐛 **Problems Found**

### 1. Config Loading with Silent Fallbacks (Lines 91-98)
**Before:**
```bash
UC_CATALOG=$(yq eval '.catalog' "$PIPELINE_JSON" 2>/dev/null || echo "main")
UC_SCHEMA=$(yq eval '.schema' "$PIPELINE_JSON" 2>/dev/null || echo "default")
UC_VOLUME=$(yq eval '.volume_name' "$PIPELINE_JSON" 2>/dev/null || echo "parquet_files")
```

**Problem:**
- If `yq` fails, silently uses default values
- Masks missing/corrupted config files
- Tests run with wrong configuration without warning

**Fixed:**
```bash
if [ ! -f "$PIPELINE_JSON" ]; then
    echo "❌ Error: Pipeline config not found: $PIPELINE_JSON"
    exit 1
fi

UC_CATALOG=$(yq eval '.catalog' "$PIPELINE_JSON" 2>/dev/null)
UC_SCHEMA=$(yq eval '.schema' "$PIPELINE_JSON" 2>/dev/null)
UC_VOLUME=$(yq eval '.volume_name' "$PIPELINE_JSON" 2>/dev/null)

# Validate required values (no silent fallbacks)
if [ -z "$UC_CATALOG" ] || [ "$UC_CATALOG" = "null" ]; then
    echo "❌ Error: Missing 'catalog' in $PIPELINE_JSON"
    exit 1
fi
```

---

### 2. Dual Parsing with yq/Python Fallback (Lines 323-327)
**Before:**
```bash
local snapshot_rows=$(echo "$json_line" | yq eval '.snapshot' - 2>/dev/null || \
    echo "$json_line" | python3 -c "import sys, json; print(json.load(sys.stdin).get('snapshot', 0))")
```

**Problem:**
- If `yq` fails, tries Python (why have two parsers?)
- Masks `yq` installation issues
- `yq` is not standard (requires separate install)

**Fixed:**
```bash
# Use jq (standard JSON tool, no fallback)
local snapshot_rows=$(echo "$json_line" | jq -r '.snapshot // 0')
```

**Why `jq`?**
- Standard JSON CLI tool
- Installed by default on most systems
- No need for Python one-liners
- Reliable, fast, purpose-built

---

### 3. Default Values Masking Parsing Failures (Lines 330-333)
**Before:**
```bash
snapshot_rows="${snapshot_rows:-0}"
insert_rows="${insert_rows:-0}"
update_rows="${update_rows:-0}"
delete_rows="${delete_rows:-0}"
```

**Problem:**
- If parsing fails (returns empty), silently uses `0`
- Masks JSON structure changes
- Reports incorrect statistics without warning

**Fixed:**
```bash
# Validate that all values are valid numbers (catch parsing errors)
if ! [[ "$snapshot_rows" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: Invalid snapshot_rows value: '$snapshot_rows'"
    return 1
fi
# ... same for all other fields
```

**Now:** Any parsing failure immediately stops the test with a clear error message.

---

### 4. Text Parsing Fallback (Lines 335-351)
**Before:**
```bash
else
    # Fallback: Parse formatted output (less reliable due to commas and emojis)
    echo "  ⚠️  Warning: JSON output not found, using text parsing (less reliable)"
    local snapshot_rows=$(echo "$analysis_output" | grep -iE "Snapshot Rows" | sed -E 's/[^0-9]//g')
    snapshot_rows="${snapshot_rows:-0}"
    # ... more text parsing
fi
```

**Problem:**
- If `JSON_STATS` is missing, falls back to fragile text parsing
- Masks bugs in `changefeed_helper.py`
- Text output format can change (emojis, commas, etc.)
- Unreliable, error-prone

**Fixed:**
```bash
if [ -z "$json_line" ]; then
    echo "❌ Error: JSON_STATS not found in changefeed_helper.py output"
    echo "Debug: Full output:"
    echo "$analysis_output"
    return 1
fi
```

**Now:** If `JSON_STATS` is missing, the test fails immediately. This forces us to fix `changefeed_helper.py`, not work around it.

---

## ✅ **Results**

### Before: Silent Failures
- Missing config → uses defaults → wrong test setup
- Parsing errors → returns `0` → incorrect statistics
- Missing JSON → uses text parsing → fragile
- **All bugs hidden**

### After: Explicit Errors
- Missing config → **FAIL** with clear error
- Parsing errors → **FAIL** with diagnostic output
- Missing JSON → **FAIL** immediately
- **All bugs exposed**

---

## 📋 **Summary**

| Issue | Before | After |
|-------|--------|-------|
| Missing config file | Use defaults silently | **FAIL**: Config not found |
| Missing config values | Use defaults silently | **FAIL**: Missing 'catalog' |
| Parsing failure | Return `0` | **FAIL**: Invalid value |
| Missing JSON_STATS | Text parsing fallback | **FAIL**: JSON not found |
| Tool availability | Try yq, then Python | Use `jq` only (standard) |

---

## 🎯 **Philosophy**

**"Fail Fast, Fail Loud"**
- Don't hide errors
- Don't work around bugs
- Don't silently use defaults
- Force proper fixes, not workarounds

**Result:** More reliable tests, easier debugging, no hidden bugs.


