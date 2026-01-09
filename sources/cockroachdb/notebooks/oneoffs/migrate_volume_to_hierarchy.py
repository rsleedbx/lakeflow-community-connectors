"""
Migrate Volume Files to Production Hierarchy

Moves existing flat files to production-style hierarchical structure:
  format/catalog/schema/scenario/

Usage in Databricks notebook:
  %run ./migrate_volume_to_hierarchy
"""

# ==============================================================================
# Configuration - Update these for your setup
# ==============================================================================

FORMAT = "parquet"  # or "json"
CATALOG = "defaultdb"
SCHEMA = "public"
SCENARIO = "existing-parquet-files"  # Name for existing data

# Volume base path
VOLUME_BASE = "dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files"

# ==============================================================================
# Migration Script
# ==============================================================================

def migrate_to_hierarchy():
    """Migrate files from flat structure to hierarchical structure."""
    
    # Source: Flat structure (old way)
    source_path = f"{VOLUME_BASE}/"
    
    # Target: Hierarchical structure (production way)
    target_path = f"{VOLUME_BASE}/{FORMAT}/{CATALOG}/{SCHEMA}/{SCENARIO}/"
    
    print("=" * 80)
    print("VOLUME MIGRATION - Production Hierarchy")
    print("=" * 80)
    print(f"Format:   {FORMAT}")
    print(f"Catalog:  {CATALOG}")
    print(f"Schema:   {SCHEMA}")
    print(f"Scenario: {SCENARIO}")
    print()
    print(f"Source (flat):        {source_path}")
    print(f"Target (hierarchical): {target_path}")
    print("=" * 80)
    print()
    
    # Create target directory with full hierarchy
    print("📁 Creating target directory...")
    dbutils.fs.mkdirs(target_path)
    print(f"   ✅ Created: {target_path}")
    print()
    
    # Determine file extension
    file_ext = '.parquet' if FORMAT == 'parquet' else '.ndjson'
    
    # List source files
    print(f"📋 Listing source files ({file_ext})...")
    try:
        files = dbutils.fs.ls(source_path)
    except Exception as e:
        print(f"   ❌ Error listing files: {e}")
        return
    
    # Filter files by extension
    target_files = [f for f in files if f.name.endswith(file_ext) and not f.name.endswith('/')]
    
    if not target_files:
        print(f"   ⚠️  No {file_ext} files found in source directory")
        return
    
    print(f"   Found: {len(target_files)} files")
    print()
    
    # Move files
    print(f"📦 Moving files to hierarchical structure...")
    print()
    
    moved_count = 0
    skipped_count = 0
    
    for file in target_files:
        source = file.path
        target = target_path + file.name
        
        try:
            dbutils.fs.mv(source, target)
            moved_count += 1
            print(f"  ✅ {file.name}")
        except Exception as e:
            skipped_count += 1
            print(f"  ⚠️  {file.name}: {e}")
    
    # Summary
    print()
    print("=" * 80)
    print("MIGRATION COMPLETE")
    print("=" * 80)
    print(f"✅ Moved:   {moved_count} files")
    if skipped_count > 0:
        print(f"⚠️  Skipped: {skipped_count} files")
    print()
    print(f"📂 Files now at:")
    print(f"   {target_path}")
    print()
    print(f"📝 Update your notebooks to use:")
    print(f"   VOLUME_PATH = '{VOLUME_BASE}/{FORMAT}/{CATALOG}/{SCHEMA}/{SCENARIO}'")
    print()
    print("=" * 80)
    print()
    print("🔍 Verify migration:")
    print(f"   dbutils.fs.ls('{target_path}')")
    print()

# ==============================================================================
# Run Migration
# ==============================================================================

if __name__ == "__main__":
    # Check if running in Databricks
    try:
        dbutils
        migrate_to_hierarchy()
    except NameError:
        print("❌ Error: This script must be run in a Databricks notebook")
        print("   Copy this code to a Databricks notebook cell and run it there")


