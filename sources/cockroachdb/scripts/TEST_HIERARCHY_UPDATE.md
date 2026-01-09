# Test Matrix Hierarchy Update

## 🎯 Change Summary

Updated `test_cdc_matrix.sh` to use **production-style hierarchical paths** instead of flat paths. Tests now mirror production exactly.

## 📂 Before vs After

### Before (Flat Structure)
```
azure://changefeed-events/test-parquet_usertable_with_split/202512...parquet
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                          Flat test prefix

Volume:
/Volumes/catalog/schema/volume/test-parquet_usertable_with_split/202512...parquet
```

### After (Hierarchical Structure - Production Style)
```
azure://changefeed-events/parquet/defaultdb/public/test-parquet_usertable_with_split/202512...usertable...parquet
                          ^^^^^^  ^^^^^^^^  ^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                          format  catalog   schema  test scenario

Volume:
/Volumes/catalog/schema/volume/parquet/defaultdb/public/test-parquet_usertable_with_split/202512...usertable...parquet
```

## ✅ Benefits

### 1. **Mirrors Production Exactly**
- Tests use same path structure as production connector
- Validates the actual path structure that will be used in production
- No surprises when moving from test to production

### 2. **Organized by Format**
```
/Volumes/catalog/schema/volume/
  ├── json/
  │   └── defaultdb/
  │       └── public/
  │           ├── test-json_usertable_with_split/
  │           └── test-json_usertable_no_split/
  └── parquet/
      └── defaultdb/
          └── public/
              ├── test-parquet_usertable_with_split/
              └── test-parquet_usertable_no_split/
```

### 3. **Multi-Tenant Ready**
Can easily test multiple catalogs/schemas:
```
parquet/defaultdb/public/test-...
parquet/warehouse/staging/test-...
json/ecommerce/public/test-...
```

### 4. **Easy Filtering**
- Filter by format: `parquet/**`
- Filter by catalog: `*/defaultdb/**`
- Filter by test: `**/test-*`

### 5. **Self-Documenting**
Path reveals:
- ✅ Format (parquet vs json)
- ✅ Database/Catalog
- ✅ Schema
- ✅ Test scenario
- ✅ Table name (in filename)

## 🔧 Code Changes

### test_cdc_matrix.sh (line 100-110)

**Before:**
```bash
local path_prefix="test-${test_name}"
```

**After:**
```bash
# Use production-style hierarchical paths: format/catalog/schema/test-scenario
local catalog="defaultdb"
local schema="public"
local path_prefix="${format}/${catalog}/${schema}/test-${test_name}"
```

### Sync Command (line 318)

**Before:**
```bash
--prefix test-parquet_usertable_with_split
--subdir test-parquet_usertable_with_split
```

**After:**
```bash
--prefix parquet/defaultdb/public/test-parquet_usertable_with_split
--subdir parquet/defaultdb/public/test-parquet_usertable_with_split
```

Preserves full hierarchy in Unity Catalog Volume!

## 📝 Updated Notebook Usage

### Before
```python
# Cell 1
VOLUME_PATH = f"{base_volume}/test-parquet_usertable_with_split"
```

### After  
```python
# Cell 1
VOLUME_PATH = f"{base_volume}/parquet/defaultdb/public/test-parquet_usertable_with_split"
```

## 🧪 Test Examples

### Run Tests
```bash
# Tests now create hierarchical paths automatically
./sources/cockroachdb/scripts/test_cdc_matrix.sh
```

### Manual Sync
```bash
# Sync specific test (preserving hierarchy)
python3 sync_azure_to_volume_compact.py \
  --prefix parquet/defaultdb/public/test-parquet_usertable_with_split \
  --subdir parquet/defaultdb/public/test-parquet_usertable_with_split
```

### Cleanup
```bash
# Clean up by format
az storage blob delete-batch \
  --source changefeed-events \
  --pattern 'parquet/defaultdb/public/test-*'

az storage blob delete-batch \
  --source changefeed-events \
  --pattern 'json/defaultdb/public/test-*'
```

## 🎯 Production Alignment

Now tests use **exactly the same structure** as production:

| Component | Test | Production |
|-----------|------|------------|
| **Azure Path** | `parquet/defaultdb/public/test-...` | `parquet/defaultdb/public/` |
| **Format in path** | ✅ Yes | ✅ Yes |
| **Catalog in path** | ✅ Yes | ✅ Yes |
| **Schema in path** | ✅ Yes | ✅ Yes |
| **Table in filename** | ✅ Yes | ✅ Yes |

**Only difference:** Test adds `test-` prefix to scenario name for easy identification.

## 🚀 Migration

### For Existing Tests

If you have old test data in flat structure:

```bash
# Old location
/Volumes/catalog/schema/volume/test-parquet_usertable_with_split/

# New tests will create
/Volumes/catalog/schema/volume/parquet/defaultdb/public/test-parquet_usertable_with_split/

# Both can coexist - no migration needed
# Old data will be unused after running new tests
```

### Cleanup Old Data (Optional)

```bash
# List old flat structure
databricks fs ls dbfs:/Volumes/catalog/schema/volume/ | grep "^test-"

# Remove old test data
databricks fs rm -r dbfs:/Volumes/catalog/schema/volume/test-parquet_usertable_with_split
databricks fs rm -r dbfs:/Volumes/catalog/schema/volume/test-json_usertable_with_split
# ... etc
```

## 📚 Related Files

- `test_cdc_matrix.sh` - Test script (updated)
- `sync_azure_to_volume_compact.py` - Sync script (no changes needed - already supports hierarchy)
- `cockroachdb.py` - Connector (uses same hierarchy via `_setup_storage_paths()`)

## ✨ Summary

**One way to do things:** Tests now use production paths exactly. This ensures:
- ✅ Tests validate actual production structure
- ✅ No surprises when deploying to production  
- ✅ Consistent organization across test and production
- ✅ Self-documenting paths with format/catalog/schema hierarchy

---

**Status:** ✅ Complete - Tests now mirror production hierarchy

**Next Step:** Run `./test_cdc_matrix.sh` to generate new hierarchical test data


