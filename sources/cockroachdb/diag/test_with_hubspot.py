"""
Test diagnostic: Use HubSpot connector with CockroachDB connection
to see what parameters Unity Catalog actually passes.
"""
from pyspark.sql import SparkSession

# Temporarily monkey-patch HubSpot connector to print ALL options
import sys
sys.path.insert(0, "/Workspace/Users/robert.lee@databricks.com/cockroachdb")

# Create a diagnostic version of HubSpot connector
class DiagnosticLakeflowConnect:
    def __init__(self, options: dict) -> None:
        print("=" * 80)
        print("🔍 DIAGNOSTIC: HubSpot-style connector __init__ called")
        print("=" * 80)
        print(f"Total options received: {len(options)}")
        print("\nALL OPTIONS (unfiltered):")
        for key in sorted(options.keys()):
            # Show everything except actual passwords
            if "password" in key.lower():
                value = "***REDACTED***"
            else:
                value = options[key]
            print(f"  {key}: {value}")
        print("=" * 80)
        
        # Don't actually initialize anything - just diagnostic
        raise RuntimeError("DIAGNOSTIC COMPLETE - Check output above!")

# Initialize Spark
spark = SparkSession.builder.appName("DiagnosticTest").getOrCreate()

# Try to read using HubSpot-style connector with CockroachDB connection
print("\nAttempting to read with diagnostic connector...")
print("Connection: robert_lee_battle-walrus-11108")
print("")

try:
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", "robert_lee_battle-walrus-11108")
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", "usertable")
        .load()
    )
except Exception as e:
    print("\n✅ Expected error (we just want to see the options):")
    print(f"   {str(e)[:200]}...")

print("\n" + "=" * 80)
print("✅ DIAGNOSTIC COMPLETE")
print("=" * 80)



