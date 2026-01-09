# One-Off Utility Scripts

This directory contains standalone utility scripts for special operations that are run occasionally, not part of the regular workflow.

## 📁 Scripts

### `migrate_volume_to_hierarchy.py`

**Purpose:** Migrate existing Unity Catalog Volume files from flat structure to production-style hierarchical structure.

**When to Use:**
- You have existing files in flat structure: `/Volumes/.../volume/file.parquet`
- Want to move them to hierarchical: `/Volumes/.../volume/format/catalog/schema/scenario/file.parquet`

**Structure:**

```
Before (Flat):
/Volumes/catalog/schema/volume/
  └── 202512...usertable...parquet

After (Hierarchical):
/Volumes/catalog/schema/volume/
  └── parquet/
      └── defaultdb/
          └── public/
              └── existing-parquet-files/
                  └── 202512...usertable...parquet
```

**Usage in Databricks Notebook:**

```python
# Option 1: Run as Python file
%run ./oneoffs/migrate_volume_to_hierarchy

# Option 2: Copy/paste code and customize
# 1. Open migrate_volume_to_hierarchy.py
# 2. Copy configuration section
# 3. Update FORMAT, CATALOG, SCHEMA, SCENARIO
# 4. Run in notebook
```

**Configuration:**

```python
# Edit these in the script before running
FORMAT = "parquet"  # or "json"
CATALOG = "defaultdb"
SCHEMA = "public"
SCENARIO = "existing-parquet-files"  # Name for this batch of files
VOLUME_BASE = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files"
```

**What It Does:**

1. ✅ Creates hierarchical directory: `format/catalog/schema/scenario/`
2. ✅ Moves all matching files (`.parquet` or `.ndjson`)
3. ✅ Shows progress for each file
4. ✅ Provides summary and next steps

**Example Output:**

```
================================================================================
VOLUME MIGRATION - Production Hierarchy
================================================================================
Format:   parquet
Catalog:  defaultdb
Schema:   public
Scenario: existing-parquet-files

Source (flat):        dbfs:/Volumes/.../parquet_files/
Target (hierarchical): dbfs:/Volumes/.../parquet_files/parquet/defaultdb/public/existing-parquet-files/
================================================================================

📁 Creating target directory...
   ✅ Created: dbfs:/Volumes/.../parquet_files/parquet/defaultdb/public/existing-parquet-files/

📋 Listing source files (.parquet)...
   Found: 15 files

📦 Moving files to hierarchical structure...

  ✅ 202512191714242809831900000000000-...-usertable-1.parquet
  ✅ 202512191714242809831900000000000-...-usertable-2.parquet
  ...

================================================================================
MIGRATION COMPLETE
================================================================================
✅ Moved:   15 files

📂 Files now at:
   dbfs:/Volumes/.../parquet_files/parquet/defaultdb/public/existing-parquet-files/

📝 Update your notebooks to use:
   VOLUME_PATH = 'dbfs:/Volumes/.../parquet_files/parquet/defaultdb/public/existing-parquet-files'
```

**After Migration:**

Update your notebooks:

```python
# Old
VOLUME_PATH = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files"

# New (with hierarchy)
VOLUME_PATH = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/parquet/defaultdb/public/existing-parquet-files"
```

## 🔄 When You Need These Scripts

### Scenario 1: First-Time Hierarchy Migration

You have existing test data in flat structure and want to organize it:

```bash
# Before
/Volumes/.../volume/
  ├── 202512...usertable...parquet
  ├── 202512...usertable...parquet
  └── 202512...usertable...parquet

# After running migration
/Volumes/.../volume/
  └── parquet/
      └── defaultdb/
          └── public/
              └── existing-data/
                  ├── 202512...usertable...parquet
                  ├── 202512...usertable...parquet
                  └── 202512...usertable...parquet
```

**Run:** `migrate_volume_to_hierarchy.py`

### Scenario 2: Multiple Data Sources

You have data from different catalogs/schemas to organize:

```python
# Run migration multiple times with different configs

# First batch - defaultdb.public
FORMAT = "parquet"
CATALOG = "defaultdb"
SCHEMA = "public"
SCENARIO = "production-data"

# Second batch - warehouse.staging
FORMAT = "parquet"
CATALOG = "warehouse"
SCHEMA = "staging"
SCENARIO = "test-data"
```

## 📋 Best Practices

### 1. **Test First**
Run on a small subset to verify behavior:
```python
# Modify script to process only first 5 files
target_files = [f for f in files if f.name.endswith(file_ext)][:5]
```

### 2. **Backup**
Create a backup before migration:
```python
backup_path = f"{VOLUME_BASE}_backup/"
dbutils.fs.cp(VOLUME_BASE, backup_path, recurse=True)
```

### 3. **Verify After**
Check files landed correctly:
```python
files = dbutils.fs.ls(f"{VOLUME_BASE}/{FORMAT}/{CATALOG}/{SCHEMA}/{SCENARIO}/")
print(f"Migrated files: {len(files)}")
```

### 4. **Multiple Scenarios**
If you have different test runs, use descriptive scenario names:
```python
SCENARIO = "test-2024-01-initial"
SCENARIO = "test-2024-01-after-fix"
SCENARIO = "production-snapshot-2024-01"
```

## 🚫 What NOT to Do

❌ **Don't** run migration on production data without testing
❌ **Don't** mix different catalogs/schemas in same scenario
❌ **Don't** reuse scenario names (creates conflicts)
❌ **Don't** run multiple times without cleanup (duplicates)

## ✅ Checklist

Before running migration:
- [ ] Updated `FORMAT`, `CATALOG`, `SCHEMA`, `SCENARIO`
- [ ] Verified `VOLUME_BASE` path is correct
- [ ] Checked source directory has files
- [ ] Have enough space in volume
- [ ] (Optional) Created backup

After running migration:
- [ ] Verified all files moved successfully
- [ ] Updated notebook paths
- [ ] Tested notebooks with new paths
- [ ] Documented the scenario name used
- [ ] (Optional) Cleaned up source directory

## 📚 Related Documentation

- `TEST_HIERARCHY_UPDATE.md` - Explains production hierarchy
- `CDC_FILE_ORGANIZATION.md` - Details on path-based metadata
- `test_cdc_matrix.sh` - How tests create hierarchical paths

---

**Note:** These scripts are for one-time operations. After migration, new changefeeds will automatically create the correct hierarchy.


