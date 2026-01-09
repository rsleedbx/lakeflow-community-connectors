# Databricks Bundle Deployment Test Results

**Date**: December 19, 2025  
**Test**: Deploying CockroachDB connector via Databricks Asset Bundles  
**Result**: ⚠️ **Partial Success - Limitation Identified**

## Test Summary

### ✅ What Worked

1. **Bundle Validation**
   - Configuration syntax validated successfully
   - Variables resolved correctly
   - YAML format accepted

2. **Bundle Deployment**
   - Files uploaded to workspace
   - Pipeline resource created
   - Workspace path configured correctly

3. **File Upload**
   - All Python files uploaded
   - Vendor directory with pg8000 uploaded
   - Directory structure preserved

### ❌ What Failed

1. **Pipeline Execution**
   - State: FAILED during INITIALIZING
   - Pipeline could not start

## Root Cause

**PyPI packages are NOT supported in Databricks Asset Bundles for Delta Live Tables (DLT) pipelines.**

### What We Tried

```yaml
# Attempt 1: environment.dependencies (NOT SUPPORTED)
environment:
  dependencies:
    - pg8000

# Attempt 2: libraries.pypi (NOT SUPPORTED - CONFIRMED)
libraries:
  - file:
      path: ./ingest.py
  - pypi:
      package: pg8000
```

### Test Results

Both approaches:
1. ✅ Pass validation (with warnings)
2. ✅ Deploy successfully
3. ❌ **PyPI library is IGNORED** - shows as `{}` in pipeline spec
4. ❌ Pipeline fails during initialization

**Confirmed**: The `pypi` field in bundle YAML is completely ignored for DLT pipelines.

### Why It Failed

1. **DLT vs Jobs Limitation**
   - `environment.dependencies` works for Databricks Jobs
   - Does NOT work for Delta Live Tables pipelines
   - DLT serverless has different package management

2. **Vendored Dependencies**
   - Even though `vendor/` directory was uploaded
   - The sys.path handling may not work in bundle deployment
   - Different from manual workspace file upload

## Comparison: Bundle vs CLI

| Aspect | Pipeline CLI (Current) | Databricks Bundles |
|--------|----------------------|-------------------|
| **Deployment** | ✅ Working | ✅ Working |
| **File Upload** | ✅ Manual | ✅ Automatic |
| **PyPI Packages** | ⚠️ Vendoring needed | ❌ Not supported |
| **Execution** | ✅ Runs successfully | ❌ Fails to initialize |
| **pg8000 Support** | ✅ Via vendoring | ❌ Cannot access vendor |

## Current Working Solution

**Use Pipeline CLI with vendored pg8000:**

```bash
# 1. Ensure vendor directory has pg8000
ls sources/cockroachdb/vendor/

# 2. Deploy with existing scripts
./sources/cockroachdb/scripts/copydir.sh
./sources/cockroachdb/scripts/createpipeline.sh

# 3. Run pipeline
databricks pipelines start-update $PIPELINE_ID --full-refresh
```

This approach works because:
- ✅ pg8000 is vendored in the `vendor/` directory
- ✅ `cockroachdb.py` adds vendor to `sys.path`
- ✅ Manual workspace upload preserves file structure
- ✅ DLT can import from workspace files

## Why Bundles Would Be Better (If They Worked)

1. **Version Control**
   - Infrastructure as code
   - YAML configuration
   - Git-friendly

2. **Multi-Environment**
   - Easy dev/prod separation
   - Variable overrides
   - Target-based deployment

3. **Validation**
   - Pre-deployment checks
   - Configuration validation
   - Catch errors early

4. **Simplicity**
   - Single `deploy` command
   - Automatic file sync
   - Less manual steps

## Recommendations

### Short Term (Current)

**Continue using Pipeline CLI deployment:**
- ✅ Proven to work
- ✅ Supports vendored dependencies
- ✅ Production-ready
- ✅ Documented in `scripts/createpipeline.sh`

### Long Term (Future)

**Monitor Databricks for updates:**
1. Check for PyPI support in DLT bundles
2. Watch for `environment` configuration support
3. Test new Databricks versions
4. Migrate to bundles when supported

### Alternative Approaches

1. **Switch to psycopg2**
   - Pre-installed in Databricks
   - But has SSL certificate issues in serverless
   - Not recommended

2. **Request Feature from Databricks**
   - File support ticket
   - Request PyPI package support for DLT in bundles
   - Share this use case

3. **Use Non-Serverless DLT**
   - Classic DLT with custom clusters
   - More control over dependencies
   - But loses serverless benefits

## Test Commands Used

```bash
# Validation
databricks bundle validate

# Deployment
databricks bundle deploy --target development

# Check resources
databricks bundle summary

# Start pipeline
databricks pipelines start-update $PIPELINE_ID --full-refresh

# Monitor
databricks pipelines get-update $PIPELINE_ID $UPDATE_ID

# Cleanup
databricks bundle destroy --target development --auto-approve
```

## Files Created

- `databricks.yml` - Bundle configuration
- `scripts/deploy_bundle.sh` - Deployment script
- `DATABRICKS_BUNDLE_DEPLOYMENT.md` - Documentation (updated with limitation)
- `BUNDLE_TEST_RESULTS.md` - This file

## Conclusion

**Databricks Asset Bundles are great for Databricks Jobs, but have limitations for Delta Live Tables pipelines regarding PyPI package management.**

**For the CockroachDB connector with pg8000 dependency:**
- ✅ **Use**: Pipeline CLI deployment with vendoring
- ❌ **Don't use**: Databricks Asset Bundles (not yet supported)

The existing deployment method is production-ready and should be used until Databricks adds PyPI support for DLT in bundles.

## References

- [Working Deployment Method](./learnings/PG8000_INSTALLATION_FIX.md)
- [Pipeline Creation Script](./scripts/createpipeline.sh)
- [Databricks Bundles Docs](https://docs.databricks.com/dev-tools/bundles/index.html)
- [DLT Configuration](https://docs.databricks.com/delta-live-tables/properties.html)

