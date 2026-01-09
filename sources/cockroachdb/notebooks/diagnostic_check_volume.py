# Diagnostic: Check what test scenarios exist in your volume
# Run this cell in your notebook to see available data

import json

# Load pipeline config
git_root = "/Workspace/Repos/robert.lee@databricks.com/lakeflow-community-connectors"  # Adjust if needed
cockroach_dir = f"{git_root}/sources/cockroachdb"
pipeline_config = json.load(open(f"{cockroach_dir}/.env/cockroachdb_pipelines.json"))

CATALOG = pipeline_config["catalog"]
SCHEMA = pipeline_config["schema"]
VOLUME_NAME = pipeline_config["volume_name"]

# Check what exists
volume_base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

print("=" * 80)
print("VOLUME DIAGNOSTIC")
print("=" * 80)
print(f"Volume base: {volume_base}")
print()

# Check formats
print("📁 Checking formats...")
try:
    formats = dbutils.fs.ls(volume_base)
    for fmt in formats:
        if fmt.isDir():
            print(f"   ✅ {fmt.name}")
except Exception as e:
    print(f"   ❌ Error: {e}")
print()

# Check parquet structure
print("📁 Checking parquet structure...")
try:
    parquet_path = f"{volume_base}/parquet"
    catalogs = dbutils.fs.ls(parquet_path)
    for cat in catalogs:
        if cat.isDir():
            print(f"   Catalog: {cat.name}")
            try:
                schemas = dbutils.fs.ls(cat.path)
                for sch in schemas:
                    if sch.isDir():
                        print(f"      Schema: {sch.name}")
                        try:
                            scenarios = dbutils.fs.ls(sch.path)
                            for scen in scenarios:
                                if scen.isDir() and not scen.name.startswith('_'):
                                    # Count files
                                    try:
                                        files = dbutils.fs.ls(scen.path)
                                        parquet_count = sum(1 for f in files if f.name.endswith('.parquet'))
                                        print(f"         ✅ {scen.name} ({parquet_count} files)")
                                    except:
                                        print(f"         ⚠️  {scen.name} (can't count files)")
                        except Exception as e:
                            print(f"         ❌ Error: {e}")
            except Exception as e:
                print(f"      ❌ Error: {e}")
except Exception as e:
    print(f"   ❌ Error: {e}")

print()
print("=" * 80)
print("💡 Update TEST_SCENARIO to match one of the scenarios above")
print("=" * 80)


