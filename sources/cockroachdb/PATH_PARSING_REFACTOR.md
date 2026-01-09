# Volume Path Parsing Refactoring

## Overview
Centralized the logic for parsing Unity Catalog volume paths into a single helper function to eliminate code duplication and improve maintainability.

**Date:** January 8, 2026  
**Status:** ✅ Complete

## Problem

The path parsing logic was duplicated in `load_and_merge_cdc_to_delta()`, making the code harder to maintain and error-prone:

```python
# BEFORE: Duplicated logic in load_and_merge_cdc_to_delta
volumes_index = -1
for i, part in enumerate(path_parts):
    if part == 'Volumes' and i > 0:
        volumes_index = i - 1
        break

if volumes_index < 0:
    raise ValueError(...)

volume_base_end = volumes_index + 1 + 4
if len(path_parts) <= volume_base_end:
    raise ValueError(...)

volume_base = '/'.join(path_parts[:volume_base_end])
path_prefix = '/'.join(path_parts[volume_base_end:])
```

This same logic was needed in multiple places, leading to:
- Code duplication
- Inconsistent error messages
- Harder to maintain and test
- Risk of bugs when updating one instance but not others

## Solution

Created a centralized `parse_volume_path()` helper function that returns a **`VolumePathComponents` class** for explicit, type-safe access:

```python
class VolumePathComponents:
    """Container for parsed volume path components."""
    
    def __init__(self, volume_base: str, path_prefix: str, timestamp: str = None):
        self.volume_base = volume_base
        self.path_prefix = path_prefix
        self.timestamp = timestamp
    
    @property
    def full_path(self) -> str:
        """Reconstruct the full path including timestamp if present."""
        if self.timestamp:
            return f"{self.volume_base}/{self.path_prefix}/{self.timestamp}"
        return f"{self.volume_base}/{self.path_prefix}"
    
    @property
    def has_timestamp(self) -> bool:
        """Check if this path includes a timestamp."""
        return self.timestamp is not None


def parse_volume_path(volume_path: str) -> VolumePathComponents:
    """
    Parse a volume path into components.
    
    Args:
        volume_path: Full volume path to parse
                    Examples:
                    - Without timestamp: 'dbfs:/Volumes/catalog/schema/volume/format/.../test-scenario'
                    - With timestamp: 'dbfs:/Volumes/catalog/schema/volume/format/.../test-scenario/1767895046'
    
    Returns:
        VolumePathComponents with explicit attributes (no tuple unpacking needed!)
    """
    # ... centralized parsing logic ...
    # Automatically detects and strips timestamp if present
    return VolumePathComponents(volume_base, path_prefix, timestamp)
```

### Updated Usage

```python
# AFTER: Clean, explicit, type-safe helper
try:
    components = parse_volume_path(volume_path)
    
    # Access via explicit attributes (no tuple unpacking!)
    print(f"Volume base: {components.volume_base}")
    print(f"Path prefix: {components.path_prefix}")
    print(f"Timestamp: {components.timestamp}")
    
    if components.has_timestamp:
        # Path already has timestamp - use full_path property
        full_path = components.full_path
    else:
        # Need to resolve timestamp
        full_path = get_timestamped_path(
            volume_base=components.volume_base,
            path_prefix=components.path_prefix,
            version=0,
            dbutils=dbutils
        )
except ValueError as e:
    raise ValueError(f"Failed to parse volume path:\n{e}")
```

## Benefits

1. **DRY Principle** - Single source of truth for path parsing
2. **Explicit Return Values** - Class attributes instead of tuple unpacking
3. **Type Safety** - Clear, self-documenting return type
4. **Properties** - Computed values like `full_path` and `has_timestamp`
5. **No Tuple Confusion** - No more `volume_base, path_prefix, timestamp = ...`
6. **Consistency** - All functions parse paths the same way
7. **Maintainability** - Changes only needed in one place
8. **Testability** - Can unit test path parsing independently
9. **Better Errors** - Consistent, helpful error messages
10. **Documentation** - Clear docstring explains the format

### Why Class Instead of Dataclass?

We chose a simple class over `@dataclass` to avoid lazy loading issues in Databricks/Spark environments:

```python
# ❌ Dataclass - Can cause lazy loading issues
@dataclass
class VolumePathComponents:
    volume_base: str
    path_prefix: str
    timestamp: Optional[str] = None

# ✅ Simple class - No dependencies, always works
class VolumePathComponents:
    def __init__(self, volume_base: str, path_prefix: str, timestamp: str = None):
        self.volume_base = volume_base
        self.path_prefix = path_prefix
        self.timestamp = timestamp
```

Benefits of simple class:
- **No import dependencies** (dataclasses module)
- **No lazy loading issues** in distributed environments
- **Explicit and simple** - easy to understand
- **Works everywhere** - Python 3.6+

## Path Format

The helper function parses Unity Catalog volume paths following this structure:

```
dbfs:/Volumes/catalog/schema/volume/format/catalog/schema/test-scenario
└─────────┬────────────────────┘ └──────────────┬──────────────────┘
      volume_base                          path_prefix
```

### Components

- **Protocol:** `dbfs:` (or other filesystem protocols)
- **Volumes Path:** `/Volumes/catalog/schema/volume`
  - catalog: Unity Catalog catalog name
  - schema: Unity Catalog schema name
  - volume: Unity Catalog volume name
- **Path Prefix:** `format/catalog/schema/test-scenario`
  - Hierarchical organization within the volume
  - Can include timestamps: `format/.../test-scenario/1767895046`

## Functions Updated

### 1. `parse_volume_path()` (NEW)
- **Location:** `cockroachdb.py:4094`
- **Purpose:** Centralized path parsing logic
- **Returns:** `(volume_base, path_prefix)` tuple

### 2. `load_and_merge_cdc_to_delta()`
- **Before:** 40 lines of duplicated parsing logic
- **After:** 3 lines calling `parse_volume_path()`
- **Reduction:** ~37 lines removed

## Backward Compatibility

The refactoring maintains 100% backward compatibility:

1. **Timestamped paths** (new format):
   ```
   dbfs:/Volumes/main/schema/volume/json/.../test-scenario/1767895046/
   ```

2. **Non-timestamped paths** (legacy format):
   ```
   dbfs:/Volumes/main/schema/volume/json/.../test-scenario/
   ```

Both formats are automatically detected and handled correctly.

## Testing

The refactoring was validated against:
- ✅ All 8 test scenarios from `test_cdc_matrix.sh`
- ✅ Both timestamped and legacy path formats
- ✅ Various error conditions (invalid paths, missing components)
- ✅ Integration with `get_timestamped_path()`

## Error Messages

The helper provides clear, actionable error messages:

### Invalid Format
```
ValueError: Invalid volume_path format: /some/invalid/path
Expected: dbfs:/Volumes/catalog/schema/volume/path/prefix
The path must contain '/Volumes/' segment.
```

### Missing Path After Volume
```
ValueError: Invalid volume_path: missing path after volume name
Path: dbfs:/Volumes/main/schema/volume
Expected: dbfs:/Volumes/catalog/schema/volume/format/catalog/schema/test-scenario
Found only 5 parts, need at least 6
```

## Future Enhancements

Potential improvements:
- [ ] Support for additional filesystem protocols (s3://, abfss://)
- [ ] Path validation (check if path exists before parsing)
- [ ] Extract metadata from path (format, catalog, schema)
- [ ] Path normalization (handle trailing slashes, etc.)
- [ ] Caching parsed paths for performance

## Related Files

- **Main Code:** `sources/cockroachdb/cockroachdb.py`
  - `parse_volume_path()` (line 4094)
  - `get_timestamped_path()` (line 4158)
  - `load_and_merge_cdc_to_delta()` (line 4644)

- **Documentation:**
  - `TIMESTAMPED_PATH_HELPER.md` - Timestamped path usage
  - `VALIDATION_MODE.md` - Test validation mode
  - `VERSION_PARAMETER_INTEGRATION.md` - Version parameter usage

## Summary

This refactoring:
- ✅ Eliminated ~37 lines of duplicated code
- ✅ Improved code maintainability
- ✅ Maintained 100% backward compatibility
- ✅ Added clear documentation
- ✅ Provided better error messages
- ✅ Made the codebase more testable

**Lines Changed:**
- Added: `parse_volume_path()` function (~65 lines)
- Removed: Duplicate parsing logic (~40 lines)
- Net Change: +25 lines (but much cleaner code structure)

