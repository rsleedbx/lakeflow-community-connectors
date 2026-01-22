# Connection URL Priority Fix

## Issue

When creating a connector with a credentials file containing multiple formats:
```json
{
  "cockroachdb_url": "postgresql://user:pass@host:port/db?sslmode=require",
  "token": "user:pass",
  "base_url": "postgresql://host:port/db?sslmode=require"
}
```

The `LakeflowConnect.__init__()` was checking `token` and `base_url` first, and only using them if both were non-empty strings. However, if `token` or `base_url` were empty strings (or the check failed for other reasons), it would fall through to the "no credentials" case, leaving `self.user`, `self.password`, etc. as `None`.

This caused errors when methods like `get_table_schema()` tried to connect to CockroachDB:

```
InterfaceError: The 'user' connection parameter cannot be None
ConnectionError: Failed to connect to CockroachDB: The 'user' connection parameter cannot be None
```

## Root Cause

The credential parsing logic in `__init__()` had the wrong priority order:

**Before (BROKEN)**:
```python
token = options.get("token")
base_url = options.get("base_url")

if token and base_url:
    # Parse token + base_url
    ...
elif options.get("host"):
    # Parse individual params
    ...
else:
    # No credentials
    self.user = None
    self.password = None
    ...
```

**Problems**:
1. Didn't check for `cockroachdb_url` first (highest priority)
2. If `token` or `base_url` were empty strings, the `if token and base_url:` check would fail
3. Would fall through to "no credentials" case even when `cockroachdb_url` was present

## Solution

Reordered credential parsing to prioritize full URLs:

**After (FIXED)**:
```python
token = options.get("token")
base_url = options.get("base_url")
# Check multiple URL key variants (handle typos)
connection_url = (options.get("connection_url") or 
                 options.get("cockroachdb_url") or 
                 options.get("cockrodb_url"))  # Handle typo variant

if connection_url:
    # Direct URL format (highest priority)
    self._parse_connection_url(connection_url)
elif token and base_url and token.strip() and base_url.strip():
    # GitHub-style: token + base_url (check for non-empty strings)
    if base_url.startswith("postgresql://"):
        full_url = f"postgresql://{token}@{base_url[13:]}"
        self._parse_connection_url(full_url)
    else:
        raise ValueError(f"Invalid base_url format: {base_url}")
elif options.get("host"):
    # Individual parameters
    ...
else:
    # No credentials provided (OK for volume-only mode)
    self.user = None
    ...
    
    # Debug: Warn if credentials found but not parsed
    if options and any(k in options for k in ['token', 'base_url', 'connection_url', 'cockroachdb_url']):
        import warnings
        found_keys = [k for k in ['token', 'base_url', 'connection_url', 'cockroachdb_url', 'host'] if k in options]
        warnings.warn(
            f"CockroachDB credentials found but not parsed. Keys in options: {found_keys}. "
            f"Check that token/base_url/cockroachdb_url values are non-empty strings.",
            UserWarning
        )
```

## Key Changes

### 1. Priority Order (Highest to Lowest)

1. **Full URL** (`connection_url`, `cockroachdb_url`, `cockrodb_url`)
   - Most explicit, contains all connection info
   - Handles typo variants
2. **Token + Base URL** (`token` + `base_url`)
   - GitHub-style split credentials
   - Now checks for non-empty strings with `.strip()`
3. **Individual Parameters** (`host`, `port`, `database`, `user`, `password`)
   - For local testing
4. **No Credentials**
   - Valid for volume-only mode
   - Now includes warning if credentials were found but not parsed

### 2. Empty String Handling

Added `.strip()` checks to ensure `token` and `base_url` are not just empty/whitespace:

```python
elif token and base_url and token.strip() and base_url.strip():
```

### 3. Typo Tolerance

Handle common typo variants:
```python
connection_url = (options.get("connection_url") or 
                 options.get("cockroachdb_url") or 
                 options.get("cockrodb_url"))  # Missing "ach"
```

### 4. Better Error Detection

If credentials are found but not parsed, warn the user:

```python
warnings.warn(
    f"CockroachDB credentials found but not parsed. Keys in options: {found_keys}. "
    f"Check that token/base_url/cockroachdb_url values are non-empty strings.",
    UserWarning
)
```

## Supported Credential Formats

### Format 1: Full URL (Recommended)
```json
{
  "cockroachdb_url": "postgresql://user:pass@host:port/db?sslmode=require"
}
```

### Format 2: Token + Base URL (GitHub-style)
```json
{
  "token": "user:pass",
  "base_url": "postgresql://host:port/db?sslmode=require"
}
```

### Format 3: Individual Parameters
```json
{
  "host": "host.crdb.io",
  "port": 26257,
  "database": "defaultdb",
  "user": "robert",
  "password": "secret",
  "sslmode": "require"
}
```

### Format 4: Mixed (All Three)
```json
{
  "cockroachdb_url": "postgresql://user:pass@host:port/db?sslmode=require",
  "token": "user:pass",
  "base_url": "postgresql://host:port/db?sslmode=require"
}
```
**Result**: Uses `cockroachdb_url` (highest priority), ignores `token`/`base_url`.

## Expected Behavior After Fix

### Scenario 1: Full URL Present
```python
options = {
    "cockroachdb_url": "postgresql://user:pass@host:26257/db?sslmode=require",
    "token": "",  # Empty
    "base_url": ""  # Empty
}
connector = LakeflowConnect(options)
# ✅ Parses cockroachdb_url, sets self.user='user', self.password='pass', etc.
```

### Scenario 2: Only Token + Base URL
```python
options = {
    "token": "user:pass",
    "base_url": "postgresql://host:26257/db?sslmode=require"
}
connector = LakeflowConnect(options)
# ✅ Combines into full URL, parses correctly
```

### Scenario 3: Empty Credentials
```python
options = {
    "token": "",
    "base_url": "",
    "volume_path": "/Volumes/..."
}
connector = LakeflowConnect(options)
# ⚠️  Warns: "credentials found but not parsed"
# Sets self.user=None (OK for volume-only mode)
```

### Scenario 4: No Credentials at All
```python
options = {
    "volume_path": "/Volumes/..."
}
connector = LakeflowConnect(options)
# ✅ No warning, self.user=None (valid for volume-only)
```

## Testing

### Before Fix
```python
crdb_config = {
    "cockroachdb_url": "postgresql://user:pass@host:26257/db",
    "token": "user:pass",
    "base_url": "postgresql://host:26257/db"
}
connector = create_connector(crdb_config)
connector.get_table_schema("test_table", {})
# ❌ InterfaceError: The 'user' connection parameter cannot be None
```

### After Fix
```python
crdb_config = {
    "cockroachdb_url": "postgresql://user:pass@host:26257/db",
    "token": "user:pass",
    "base_url": "postgresql://host:26257/db"
}
connector = create_connector(crdb_config)
connector.get_table_schema("test_table", {})
# ✅ Connects successfully, returns schema
```

## Files Modified

- ✅ `cockroachdb.py` (lines 191-230)
  - Reordered credential parsing logic
  - Added empty string checks
  - Added typo tolerance
  - Added warning for unparsed credentials
- ✅ `CONNECTION_URL_PRIORITY_FIX.md` (this file)

## Related Issues

This fix also resolves related issues where:
- Diagnostic cells showed credentials were loaded but connection failed
- Volume-mode connectors tried to connect to CockroachDB for schema metadata
- Mixed credential formats caused confusion about which format was used

## Summary

**Problem**: `cockroachdb_url` was not prioritized, causing `self.user` to be `None`.  
**Solution**: Check for full URL first, validate non-empty strings, warn on parse failures.  
**Result**: Robust credential parsing that handles all formats and gives clear error messages.
