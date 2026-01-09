# PG8000 Auto-Installation in Databricks Serverless

> 🎉 **UPDATE**: With **Databricks Asset Bundles**, all this complexity is NO LONGER NEEDED!
> 
> Simply specify in `databricks.yml`:
> ```yaml
> environment:
>   dependencies:
>     - pg8000>=1.30.0
> ```
> 
> Databricks handles installation automatically. **No vendoring required!**
> 
> See [DATABRICKS_BUNDLE_DEPLOYMENT.md](../DATABRICKS_BUNDLE_DEPLOYMENT.md) for the modern approach.
> 
> ---
> 
> The content below documents the **legacy vendoring approach** (only needed if not using bundles).

---

## Problem Statement (Legacy Approach)

The CockroachDB connector requires `pg8000` (pure Python PostgreSQL driver) to avoid psycopg2/libpq SSL certificate permission issues in Databricks serverless environments.

**Why pg8000?**
- psycopg2 depends on libpq which has hardcoded lookups in `/root/.postgresql/`
- This directory is not accessible in Databricks serverless (permission denied)
- pg8000 is pure Python and uses standard Python `ssl` library

**Installation Challenge:**
- PyPI library configuration via CLI/REST API is not supported for DLT pipelines
- Runtime `pip install` hits permission issues in serverless environments
- Need to auto-install pg8000 when connector is first used

---

## Errors Encountered

### Error 1: Read-only Filesystem (/.local)
```
ERROR: Could not install packages due to an OSError: [Errno 30] Read-only file system: '/.local'
```

**Cause:** pip tries to install to `/.local` by default, which is read-only in serverless

**Solution:** Use `--target` flag to install to a writable temp directory

---

### Error 2: Permission Denied (/databricks/jars)
```
PermissionError: [Errno 13] Permission denied: '/databricks/jars/----ws_4_0--core--core-hive-2.3__hadoop-3.2_2.13_deploy.jar'
```

**Cause:** pip scans all Python paths for existing distributions, including `/databricks/jars` which has restricted permissions

**Ongoing:** Testing solutions with additional pip flags

---

## Current Implementation

```python
def _get_connection(self, table_options: Dict[str, str] = None):
    """Create and return a new connection to CockroachDB."""
    try:
        # LAZY IMPORT: Import pg8000 here (not at module level for Spark serialization)
        try:
            import pg8000
        except ImportError:
            # pg8000 not installed yet - install it now in this worker process
            print("📦 pg8000 not found - installing in worker process...")
            import subprocess
            import sys
            import tempfile
            
            # Create a temporary directory for installation
            # This avoids the read-only /.local directory issue
            temp_dir = tempfile.mkdtemp(prefix="pg8000_")
            print(f"   Installing to temporary directory: {temp_dir}")
            
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", 
                 "--target", temp_dir, 
                 "--quiet", 
                 "pg8000>=1.30.0"],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"❌ pip install STDOUT: {result.stdout}")
                print(f"❌ pip install STDERR: {result.stderr}")
                raise ImportError(f"Failed to install pg8000: {result.stderr}")
            
            # Add the temp directory to Python path
            sys.path.insert(0, temp_dir)
            print(f"✅ pg8000 installed successfully to {temp_dir}")
            import pg8000
        
        # ... rest of connection logic
```

---

## Required Imports

**CRITICAL:** The `ssl` module must be imported at the top of the file:

```python
from typing import Dict, List, Iterator, Any
import json
import ssl  # REQUIRED for pg8000 SSL context
from pyspark.sql.types import (...)
```

**Why:** pg8000 connection requires creating an `ssl.SSLContext` to disable certificate verification

---

## Attempted Solutions

### ❌ Attempt 1: Direct pip install (no --target)
```python
subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "pg8000>=1.30.0"])
```
**Result:** `OSError: [Errno 30] Read-only file system: '/.local'`

---

### ❌ Attempt 2: Install with --target to temp directory
```python
temp_dir = tempfile.mkdtemp(prefix="pg8000_")
subprocess.run([sys.executable, "-m", "pip", "install", "--target", temp_dir, "--quiet", "pg8000>=1.30.0"])
sys.path.insert(0, temp_dir)
```
**Result:** `PermissionError: [Errno 13] Permission denied: '/databricks/jars/...'`

**Issue:** pip scans `/databricks/jars` during package discovery, hits permission error

---

### ❌ Attempt 3: Install with --no-cache-dir and --no-deps
```python
subprocess.run([
    sys.executable, "-m", "pip", "install", 
    "--target", temp_dir,
    "--no-cache-dir",  
    "--no-deps",       
    "pg8000>=1.30.0"
])
```
**Result:** `No module named 'scramp'`

**Issue:** pg8000 requires `scramp` for SCRAM authentication - can't skip dependencies

---

### ❌ Attempt 4: Install with --no-cache-dir only
```python
subprocess.run([
    sys.executable, "-m", "pip", "install", 
    "--target", temp_dir,
    "--no-cache-dir",  
    "pg8000>=1.30.0"
])
```
**Result:** `PermissionError: [Errno 13] Permission denied: '/databricks/jars/...'`

**Issue:** pip still scans `/databricks/jars` for existing packages during dependency resolution

---

### ❌ Attempt 5: Install with --isolated and --disable-pip-version-check
```python
subprocess.run([
    sys.executable, "-m", "pip", "install", 
    "--target", temp_dir,
    "--no-cache-dir",
    "--isolated",
    "--disable-pip-version-check",
    "pg8000>=1.30.0"
])
```
**Result:** `PermissionError: [Errno 13] Permission denied: '/databricks/jars/...'`

**Issue:** Even with --isolated, pip's internal package discovery scans sys.path including restricted directories

---

## Alternative Approaches (if auto-install fails)

### Option A: Manual Library Addition via UI
1. Go to Pipeline Settings in Databricks UI
2. Add PyPI library: `pg8000>=1.30.0`
3. Save and restart pipeline

**Drawback:** Requires manual configuration for each pipeline

---

### Option B: Cluster Init Script
Create an init script to pre-install pg8000 on cluster startup:

```bash
#!/bin/bash
/databricks/python/bin/pip install pg8000>=1.30.0
```

**Drawback:** Not applicable for serverless pipelines (no cluster to configure)

---

### Option C: Use psycopg2 with Alternative SSL Configuration
Try to make psycopg2 work by overriding SSL certificate paths:

```python
os.environ['PGSSLCERT'] = '/tmp/dummy.crt'
os.environ['PGSSLKEY'] = '/tmp/dummy.key'
os.environ['PGSSLROOTCERT'] = '/tmp/dummy_ca.crt'
```

**Drawback:** Already attempted 20+ variations, all failed

---

## Root Cause Analysis

**The Core Problem:**
Databricks serverless environments have **strict filesystem permissions** that prevent pip from:
1. Writing to default locations (`/.local`, `~/.local`)
2. Reading certain system paths during package discovery (`/databricks/jars/*`)

**Why `--target` doesn't help:**
Even when installing to a temp directory, pip's internal package discovery mechanism scans `sys.path` for existing installations, which includes `/databricks/jars/` - a directory with restricted permissions that pip cannot stat.

**The pip error chain:**
```
pip install → get_default_environment() → iter_all_distributions() → 
find_linked() → path.is_dir() → os.stat('/databricks/jars/...') → 
PermissionError [Errno 13]
```

---

## Conclusion

**❌ Runtime Auto-Installation is NOT POSSIBLE in Databricks Serverless**

After extensive testing (5+ attempts with various pip flags), we conclude that:
- pip cannot be run successfully in Databricks serverless due to permission restrictions
- No combination of pip flags (`--no-cache-dir`, `--isolated`, `--disable-pip-version-check`, `--target`) bypasses the `/databricks/jars` scanning issue
- This is a Databricks platform limitation, not a code issue

---

## REQUIRED SOLUTION: Manual Library Addition

**Users MUST add pg8000 library manually via Databricks UI:**

### Steps:
1. Navigate to pipeline in Databricks UI
2. Click "Settings" → "Libraries"
3. Add PyPI library: `pg8000>=1.30.0`
4. Save and restart pipeline

**Alternative (if creating new pipeline):**
Use Databricks Asset Bundles (DAB) YAML format which supports PyPI libraries:
```yaml
resources:
  pipelines:
    cockroachdb_pipeline:
      libraries:
        - file:
            path: /Workspace/.../ingest.py
        - pypi:
            package: pg8000>=1.30.0
```

---

## Status

**Current State:** ❌ Auto-installation blocked by Databricks permissions

**Pipeline ID:** `0a26ae89-f33d-4d34-9925-c6833efad759`

**Latest Test Update ID:** `b1f26c83-ec41-4df8-aa1c-cb6f2eaf7267`

**Test Result:** pg8000 is NOT pre-installed in Databricks serverless runtime

**Current Pipeline Libraries:**
```
- File: /Workspace/Users/robert.lee@databricks.com/cockroachdb/ingest.py
- Empty entry: {} (possibly remnant from failed PyPI addition attempt)
```

**Resolution:** Unknown - User reports manual UI was NOT used, but pg8000 worked previously

---

## ✅ FINAL WORKING SOLUTION: Vendoring (Bundling) pg8000

### Discovery

**Critical Finding:** DLT Serverless pipelines have **NO Libraries UI section**!

This explains why all previous attempts failed:
- ❌ No UI to add libraries manually
- ❌ No PyPI support in CLI/REST API  
- ❌ No runtime pip install capability
- ❌ pg8000 not pre-installed

### The Solution: Vendor Dependencies

Since no other method works, we must **bundle (vendor) pg8000 directly** with the connector code.

### Implementation

#### Step 1: Create vendor directory with dependencies
```bash
cd sources/cockroachdb
mkdir -p vendor
pip3 install --target vendor pg8000
```

This downloads:
- pg8000 (~57KB) - Pure Python PostgreSQL driver
- scramp (~12KB) - SCRAM authentication  
- python-dateutil (~229KB) - Date utilities
- asn1crypto (~105KB) - ASN.1 crypto support
- six (~11KB) - Python 2/3 compatibility

**Total:** ~500KB (acceptable overhead)

#### Step 2: Update cockroachdb.py to use vendor directory
```python
def _get_connection(self, table_options: Dict[str, str] = None):
    try:
        # Add vendor directory to sys.path
        import sys
        import os
        vendor_dir = os.path.join(os.path.dirname(__file__), 'vendor')
        if vendor_dir not in sys.path:
            sys.path.insert(0, vendor_dir)
            print(f"📦 Added vendor directory to path: {vendor_dir}")
        
        # Now import pg8000 from vendor
        import pg8000
        # ... rest of connection code
```

#### Step 3: Update copydir.sh to include vendor directory
```bash
# Copy vendor directory with pg8000 and dependencies
if [ -d "sources/$SOURCE_NAME/vendor" ]; then
  echo "Copying vendor directory (pg8000 and dependencies)..."
  cp -rv sources/$SOURCE_NAME/vendor "$TEMP_DIR/sources/$SOURCE_NAME/"
fi
```

### Test Result

✅ **Pipeline COMPLETED successfully!**
- Update ID: abd76576-383c-4c89-8f21-ed3cc026dfd0
- pg8000 imported successfully from vendor directory
- Connection to CockroachDB established
- Pipeline ran to completion

### Why This is the Only Solution

For DLT Serverless Pipelines specifically:
1. No Libraries UI section exists
2. PyPI in pipeline JSON not supported
3. Runtime pip install blocked by permissions
4. Dependencies not pre-installed

**Vendoring is the ONLY way** to include external dependencies in DLT serverless connectors.

---

## Lessons Learned

1. **Document working solutions immediately** - If auto-install ever worked, it was likely on a different cluster type or Databricks version
2. **Test in target environment early** - Serverless has different restrictions than standard clusters  
3. **Platform limitations exist** - Not all Python packaging approaches work in managed environments
4. **Fallback to manual configuration** - For production connectors, document manual setup steps clearly

---

## Related Documentation

- [SSL_CERTIFICATE_FIX.md](SSL_CERTIFICATE_FIX.md) - Why pg8000 is needed
- [PG8000_PARAMETER_INDEXING.md](PG8000_PARAMETER_INDEXING.md) - pg8000 API differences
- [PG8000_TIMEOUT_EXCEPTION_HANDLING.md](PG8000_TIMEOUT_EXCEPTION_HANDLING.md) - Exception handling

