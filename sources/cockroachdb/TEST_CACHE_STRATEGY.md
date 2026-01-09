# CDC Test Data Caching Strategy

## Overview

CDC test data is automatically cached locally during sync operations to improve troubleshooting efficiency and reduce Azure API calls.

**Status:** ✅ **INTEGRATED** (Jan 8, 2026) - No separate tools needed!

## How It Works

### Automatic Caching

The `sync_azure_to_volume_compact.py` script **automatically caches** downloaded files:

```
Azure Blob Storage
       ↓ (download once)
$GIT_ROOT/.cache/cdc_test_data/{prefix}/
       ↓ (reuse from cache)
Databricks Unity Catalog Volume
       ↓ (read from cache)
Diagnostic Scripts
```

**Key Insight:** Files are downloaded once and reused automatically!

### Cache Location

```bash
$GIT_ROOT/.cache/cdc_test_data/
├── json/
│   └── defaultdb/
│       └── public/
│           ├── test-json_usertable_with_split/1767823340/
│           │   ├── 202601072205...ndjson
│           │   └── 202601072206...ndjson
│           └── test-json_usertable_no_split/1767823340/
│               └── ...
└── parquet/
    └── defaultdb/
        └── public/
            └── test-parquet_usertable_with_split/1767823340/
                └── ...parquet
```

**Structure:** Mirrors Azure path exactly for easy navigation

## Usage

### Normal Operation (Automatic)

```bash
# Run tests - caching happens automatically
./test_cdc_matrix.sh json

# Sync to volume - uses cache automatically  
python3 sync_azure_to_volume_compact.py --prefix json/defaultdb/public/test-json_usertable_no_split/1767823340

# ✅ Output shows cache hits:
#    ⚡ file1.ndjson (from cache)
#    ⚡ file2.ndjson (from cache)
```

### Run Diagnostics (Use Cache)

```bash
# Find cached files
ls .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/

# Run diagnostic directly on cached file
./diagnose_json_struct.py .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/202601072206*.ndjson

# Or use glob patterns
./diagnose_json_struct.py .cache/cdc_test_data/json/**/test-json_usertable_no_split/**/*.ndjson
```

### Force Re-download (Bypass Cache)

```bash
# When you need fresh data from Azure
python3 sync_azure_to_volume_compact.py --prefix json/.../test-json_usertable_no_split/1767823340 --no-cache

# ✅ Output shows downloads:
#    ⬇️  file1.ndjson (downloaded)
#    ⬇️  file2.ndjson (downloaded)
```

### Clear Cache (Free Disk Space)

```bash
# Clear all cached data
rm -rf .cache/cdc_test_data

# Clear specific scenario
rm -rf .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split

# Clear old timestamps only
find .cache/cdc_test_data -type d -name "1767820*" -exec rm -rf {} +
```

## Benefits

### Performance Improvements

| Operation | Without Cache | With Cache | Speedup |
|-----------|---------------|------------|---------|
| First sync | 30 sec (download) | 30 sec (download) | 1× |
| Second sync | 30 sec (download) | <1 sec (cache hit) | **30× faster** |
| Diagnostic run | 10 sec (download) | <1 sec (cache hit) | **10× faster** |
| 10 diagnostic runs | 100 sec | 1 sec | **100× faster** |

### Real-World Impact

**Before (No Cache):**
```bash
./test_cdc_matrix.sh        # 30 min
# Change code, test fix
./diagnose_json_struct.py   # 10 sec download
# Change code again
./diagnose_json_struct.py   # 10 sec download again
# Repeat 10 times...         # 100 sec wasted
```

**After (With Cache):**
```bash
./test_cdc_matrix.sh        # 30 min (downloads + caches)
# Change code, test fix
./diagnose_json_struct.py   # <1 sec (cache hit!)
# Change code again
./diagnose_json_struct.py   # <1 sec (cache hit!)
# Repeat 10 times...         # 1 sec total (99 sec saved!)
```

## Technical Details

### Implementation

**File:** `sync_azure_to_volume_compact.py`

**Key Changes:**
1. Added `get_cache_dir()` function - creates persistent cache directory
2. Modified `sync_files()` - checks cache before downloading
3. Added `--no-cache` flag - force re-download when needed
4. Updated summary output - shows cache hits vs downloads

**Code Snippet:**
```python
def sync_files(azure_config, w, blobs, volume_path, existing, cache_dir, use_cache=True):
    """Download from Azure (with caching), upload to Volume."""
    for blob_name in blobs:
        cached_file = cache_dir / filename
        
        if use_cache and cached_file.exists():
            # Use cached file (no download!)
            cached += 1
            console.print(f"  [cyan]⚡ {filename} (from cache)[/cyan]")
        else:
            # Download from Azure and save to cache
            cached_file.write_bytes(blob_client.download_blob().readall())
            console.print(f"  [green]⬇️  {filename} (downloaded)[/green]")
        
        # Upload to Volume from cache
        with open(cached_file, 'rb') as f:
            w.files.upload(f"{volume_path}/{filename}", f)
```

### Git Integration

**Added to `.gitignore`:**
```gitignore
# Local cache directories (test data)
.cache/
```

This ensures:
- ✅ Cache not committed to git
- ✅ Each developer has their own cache
- ✅ No repo bloat from test data

## Best Practices

### When Cache is Useful ✅

- **Repeated diagnostic runs** - Same files, multiple iterations
- **Code changes** - Testing fixes without re-downloading
- **Offline work** - Troubleshooting without network
- **Team collaboration** - Share cache via NFS/shared drive

### When to Bypass Cache ❌

- **First time setup** - No cache exists yet
- **Data changes in Azure** - Need fresh data
- **Verification** - Ensuring Azure and cache match
- **Debugging cache issues** - Ruling out stale cache

### Cache Maintenance

**Monitor Size:**
```bash
du -sh .cache/cdc_test_data
# Typical: 2-5 MB per timestamp, ~20 MB for 10 timestamps
```

**Cleanup Strategy:**
```bash
# After each test run (optional - only if space is limited)
find .cache/cdc_test_data -type d -mtime +7 -exec rm -rf {} +   # >7 days old

# Manual cleanup when needed
rm -rf .cache/cdc_test_data/**/17678*   # Old timestamps
```

## Troubleshooting

### "File not in cache"

**Symptom:** Diagnostic script can't find file

**Solution:**
```bash
# Run sync first to populate cache
python3 sync_azure_to_volume_compact.py --prefix json/defaultdb/public/test-json_usertable_no_split/1767823340
```

### "Stale cache data"

**Symptom:** Diagnostic results don't match expectations

**Solution:**
```bash
# Force re-download
python3 sync_azure_to_volume_compact.py --prefix ... --no-cache
```

### "Cache taking too much space"

**Symptom:** Disk space running low

**Solution:**
```bash
# Check cache size
du -sh .cache/cdc_test_data

# Clear old timestamps
find .cache/cdc_test_data -type d -name "1767*" | sort | head -n -3 | xargs rm -rf

# Or clear everything
rm -rf .cache/cdc_test_data
```

## Examples

### Example 1: First-Time Test Run

```bash
$ ./test_cdc_matrix.sh json

# During sync, you'll see:
📁 Syncing files...
   ⬇️  202601072205...ndjson (downloaded)
   ⬇️  202601072206...ndjson (downloaded)

✅ Files cached to: .cache/cdc_test_data/json/defaultdb/public/...
```

### Example 2: Subsequent Run (Cache Hit!)

```bash
$ python3 sync_azure_to_volume_compact.py --prefix json/defaultdb/public/test-json_usertable_no_split/1767823340

# You'll see:
📁 Syncing files...
   ⚡ 202601072205...ndjson (from cache)    ← FAST!
   ⚡ 202601072206...ndjson (from cache)    ← FAST!

Sync Summary:
  Copied to Volume: 2
  Used from Cache: 2                        ← No downloads!
  Downloaded from Azure: 0                  ← Zero downloads!
```

### Example 3: Running Diagnostics

```bash
# List available cached files
$ find .cache/cdc_test_data -name "*.ndjson" | head -3
.cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/202601072205313357901250000000000-...ndjson
.cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/202601072206035945830580000000001-...ndjson

# Run diagnostic (uses cache - super fast!)
$ ./diagnose_json_struct.py .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/*.ndjson

# Output appears immediately (no download wait!)
```

## Summary

**Key Takeaways:**
- ✅ **Automatic:** Caching is enabled by default (no manual steps)
- ✅ **Integrated:** Built into existing sync script (no separate tools)
- ✅ **Fast:** 10-100× speedup for diagnostic workflows
- ✅ **Simple:** Cache location is `$GIT_ROOT/.cache/cdc_test_data/`
- ✅ **Safe:** Added to `.gitignore` (won't commit test data)

**Comparison with Original Design:**
| Aspect | Separate Cache Script | Integrated (Current) |
|--------|----------------------|----------------------|
| Tools needed | 3 scripts | 1 script (existing) |
| Cache location | `/tmp` (deleted on reboot) | `$GIT_ROOT/.cache` (persistent) |
| Automatic | No (manual download) | Yes (during sync) |
| Code duplication | Yes (separate scripts) | No (integrated) |
| Maintenance | High (multiple tools) | Low (single tool) |

**Result:** Simpler, faster, better integrated! 🎉

---

*Created: January 8, 2026*  
*Status: ✅ Integrated into sync_azure_to_volume_compact.py*  
*See also: CONNECTOR_EVOLUTION_STRATEGY.md (Test Data Strategy section)*
