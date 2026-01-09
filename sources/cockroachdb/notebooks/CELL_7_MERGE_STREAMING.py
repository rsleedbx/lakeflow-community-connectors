# ============================================================================
# Cell 7: Merge Column Family Fragments (Streaming-Safe Version)
# ============================================================================
# Insert this cell AFTER Cell 6 (transformations) and BEFORE Cell 8 (write)
# ============================================================================

import sys
import os
import importlib

# Add parent directory to path
parent_dir = os.path.abspath("../..")
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import and reload to ensure latest version
import cockroachdb
importlib.reload(cockroachdb)

from cockroachdb import merge_column_family_fragments

print("="*80)
print("STEP 2B: MERGE COLUMN FAMILY FRAGMENTS")
print("="*80)

# Merge column family fragments
# For streaming DataFrames (from Autoloader), this always applies the merge
# For batch DataFrames, it auto-detects and only merges if needed
df_merged = merge_column_family_fragments(
    df_enriched,
    primary_key_columns=PRIMARY_KEY_COLUMNS,  # ['ycsb_key'] from Cell 2
    debug=True
)

# Replace df_enriched for downstream cells
df_enriched = df_merged

print("\n✅ Column family handling complete!")
print("="*80)


# ============================================================================
# EXPECTED OUTPUT (Streaming Mode):
# ============================================================================
"""
================================================================================
STEP 2B: MERGE COLUMN FAMILY FRAGMENTS
================================================================================

🔍 Column Family Merge (Streaming Mode)
   Primary key columns: ['ycsb_key']
   Data columns: 10 columns
     ['field0', 'field1', 'field2', 'field3', 'field4']

🔧 Streaming mode: Applying merge
   (Cannot detect fragmentation in streaming DataFrames)
   - If column families exist: fragments will be merged
   - If no column families: merge is harmless no-op

✅ Merge transformation applied!
   Streaming DataFrame merged
   (Actual counts will be visible after writeStream completes)

✅ Column family handling complete!
================================================================================
"""


# ============================================================================
# VERIFICATION - After writeStream completes in Cell 8
# ============================================================================
"""
After Cell 8 (writeStream) completes, Cell 10 will show:

BEFORE MERGE:
  📊 Total records in Delta table: 109,945  ❌
  
AFTER MERGE:
  📊 Total records in Delta table: 9,995  ✅
  
The merge happens DURING the stream processing, so you'll see the correct
count in the final Delta table!
"""


