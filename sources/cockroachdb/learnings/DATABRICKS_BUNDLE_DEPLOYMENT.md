# Deploying CockroachDB Connector with Databricks Asset Bundles

> ⚠️ **IMPORTANT LIMITATION**: 
> 
> **PyPI packages are NOT supported in Databricks Asset Bundles for DLT pipelines.**
> 
> While bundles work great for Jobs, Delta Live Tables pipelines do not support:
> - `environment.dependencies`
> - `libraries.pypi` specification
> 
> **For now, use the traditional Pipeline CLI deployment method with vendored pg8000.**
> 
> See `scripts/createpipeline.sh` and `learnings/PG8000_INSTALLATION_FIX.md` for the working approach.

---

## 🎯 Why Use Databricks Asset Bundles? (Note: Limited for DLT)

Databricks Asset Bundles (DAB) provide a **better way** to deploy pipelines compared to raw CLI commands:

| Feature | Pipeline CLI | Databricks Bundles (DAB) |
|---------|-------------|---------------------------|
| **Dependency Management** | ❌ Not supported<br>(requires vendoring) | ✅ Native support<br>`environment.dependencies` |
| **Configuration Management** | ❌ Manual JSON files | ✅ Version-controlled YAML |
| **Multi-environment** | ❌ Manual switching | ✅ Built-in targets (dev/prod) |
| **Infrastructure as Code** | ⚠️ Partial | ✅ Complete |
| **Validation** | ❌ No pre-deploy checks | ✅ `bundle validate` |
| **Deployment** | ⚠️ Multiple commands | ✅ Single `bundle deploy` |
| **Updates** | ❌ Complex | ✅ Simple redeploy |

## ✅ Key Advantage: No More Vendoring!

With bundles, you can specify `pg8000` as a dependency:

```yaml
environment:
  dependencies:
    - pg8000>=1.30.0
```

Databricks installs it automatically - **no need to vendor the library**!

## 📁 Bundle Structure

```
sources/cockroachdb/
├── databricks.yml          # Bundle configuration
├── ingest.py              # Pipeline code
├── cockroachdb.py         # Connector code
├── requirements.txt       # Optional: for local dev
└── scripts/
    ├── deploy_bundle.sh   # Deployment script
    └── monitor_pipeline.sh # Monitoring script
```

## 🚀 Quick Start

### 1. Configure Bundle (if needed)

Edit `databricks.yml` to customize:

```yaml
variables:
  connection_name:
    default: your_connection_name
  
  table_list:
    default: table1,table2,table3
```

### 2. Deploy

```bash
cd sources/cockroachdb

# Deploy to development
./scripts/deploy_bundle.sh development

# Or deploy to production
./scripts/deploy_bundle.sh production
```

### 3. Run Pipeline

```bash
# Option 1: Via bundle
databricks bundle run cockroachdb_pipeline

# Option 2: Via pipeline ID
PIPELINE_ID=$(databricks pipelines list-pipelines | grep YOUR_NAME_cockroachdb | awk '{print $2}')
databricks pipelines start-update $PIPELINE_ID --full-refresh
```

## 📋 Complete Deployment Workflow

### First Time Setup

```bash
# 1. Create Unity Catalog connection
databricks connections create \
  --name your_connection_name \
  --connection-type COCKROACHDB \
  --options '{
    "host": "your-host.cockroachlabs.cloud",
    "port": "26257",
    "user": "your_user",
    "password": "your_password",
    "database": "defaultdb"
  }'

# 2. Validate bundle
cd sources/cockroachdb
databricks bundle validate

# 3. Deploy
databricks bundle deploy

# 4. Run pipeline
databricks bundle run cockroachdb_pipeline --full-refresh
```

### Subsequent Updates

```bash
# After code changes
databricks bundle deploy

# The pipeline will use the updated code on next run
databricks bundle run cockroachdb_pipeline --full-refresh
```

## 🎨 Bundle Configuration Explained

### databricks.yml

```yaml
bundle:
  name: cockroachdb_connector  # Bundle identifier

variables:
  # Variables can be overridden per target
  catalog: main
  schema_name: ${workspace.current_user.short_name}_cockroachdb_cdc
  connection_name: ${workspace.current_user.short_name}_connection
  table_list: usertable

targets:
  development:
    mode: development  # Dev mode for easier debugging
    default: true
    variables:
      schema_name: ${workspace.current_user.short_name}_cockroachdb
  
  production:
    mode: production  # Production mode
    variables:
      schema_name: cockroachdb_production

resources:
  pipelines:
    cockroachdb_pipeline:
      name: ${workspace.current_user.short_name}_cockroachdb
      
      # Unity Catalog integration
      catalog: ${var.catalog}
      target: ${var.schema_name}
      
      # Pipeline settings
      serverless: true
      channel: PREVIEW
      development: true
      
      # Connector configuration
      configuration:
        connection_name: ${var.connection_name}
        source_name: cockroachdb
        table_list: ${var.table_list}
      
      # Code to deploy
      libraries:
        - file:
            path: ./ingest.py
      
      # 🎯 KEY: Dependency management
      environment:
        dependencies:
          - pg8000>=1.30.0
```

## 🔧 Common Operations

### Deploy to Specific Target

```bash
# Development (default)
databricks bundle deploy

# Production
databricks bundle deploy --target production
```

### Override Variables

```bash
# Via command line
databricks bundle deploy --var="table_list=table1,table2,table3"

# Via environment variable
export DATABRICKS_BUNDLE_VAR_table_list="table1,table2,table3"
databricks bundle deploy
```

### Validate Before Deploy

```bash
databricks bundle validate --target development
```

### Destroy Resources

```bash
# ⚠️  This will delete the pipeline!
databricks bundle destroy --target development
```

### View Deployed Resources

```bash
databricks bundle summary
```

## 🆚 Comparison: CLI vs Bundle

### Old Way (Pipeline CLI)

```bash
# Create connection manually
databricks connections create ...

# Upload files manually
databricks workspace import ...

# Create pipeline with JSON config
cat > pipeline_config.json << EOF
{
  "name": "robert_lee_cockroachdb",
  "libraries": [{"file": {"path": "/Workspace/..."}}],
  "configuration": {...},
  ...
}
EOF
databricks pipelines create --settings pipeline_config.json

# Update after code changes - complex!
databricks workspace import ...  # Re-upload
databricks pipelines update ...  # Update config
databricks pipelines start-update ... --full-refresh
```

**Problems:**
- ❌ No dependency management (requires vendoring)
- ❌ Manual file uploads
- ❌ JSON configuration is error-prone
- ❌ No validation before deployment
- ❌ Hard to manage multiple environments

### New Way (Databricks Bundles)

```bash
# Everything in code
databricks bundle validate
databricks bundle deploy
databricks bundle run cockroachdb_pipeline --full-refresh
```

**Benefits:**
- ✅ Dependencies managed automatically
- ✅ Files deployed automatically
- ✅ YAML configuration with validation
- ✅ Built-in multi-environment support
- ✅ Infrastructure as code
- ✅ Easier updates

## 📊 Migration from CLI to Bundles

### Step 1: Create databricks.yml

```bash
cd sources/cockroachdb
# (databricks.yml already created)
```

### Step 2: Remove Vendored Dependencies

```bash
# No longer needed!
rm -rf vendor/
```

### Step 3: Update cockroachdb.py

Remove vendoring logic:

```python
# OLD (with vendoring):
import sys, os
vendor_dir = os.path.join(os.path.dirname(__file__), 'vendor')
if vendor_dir not in sys.path:
    sys.path.insert(0, vendor_dir)

# NEW (with bundles):
# Just import directly!
import pg8000
```

### Step 4: Deploy with Bundle

```bash
databricks bundle deploy
```

### Step 5: Verify

```bash
# Check pipeline exists
databricks pipelines list-pipelines | grep YOUR_NAME_cockroachdb

# Run a test
databricks bundle run cockroachdb_pipeline --full-refresh
```

## 🎯 Best Practices

### 1. Use Version Control

```bash
git add sources/cockroachdb/databricks.yml
git add sources/cockroachdb/scripts/deploy_bundle.sh
git commit -m "Add Databricks Bundle configuration"
```

### 2. Separate Dev and Prod

```yaml
targets:
  development:
    variables:
      schema_name: ${workspace.current_user.short_name}_dev
  
  production:
    variables:
      schema_name: production
      # Use production connection
      connection_name: prod_cockroachdb_connection
```

### 3. Use CI/CD

```yaml
# .github/workflows/deploy.yml
- name: Deploy to Development
  run: databricks bundle deploy --target development
  
- name: Run Tests
  run: databricks bundle run cockroachdb_pipeline

- name: Deploy to Production
  if: github.ref == 'refs/heads/main'
  run: databricks bundle deploy --target production
```

### 4. Pin Dependency Versions

```yaml
environment:
  dependencies:
    - pg8000==1.30.5  # Pin exact version for reproducibility
```

## 🐛 Troubleshooting

### Bundle Validation Fails

```bash
# Get detailed error messages
databricks bundle validate --log-level debug
```

### Dependencies Not Installing

Check the pipeline logs:
```bash
PIPELINE_ID=$(databricks pipelines list-pipelines | grep YOUR_NAME | awk '{print $2}')
databricks pipelines get-update $PIPELINE_ID $UPDATE_ID --output json | jq '.update.events[] | select(.message | contains("pg8000"))'
```

### Pipeline Not Found After Deploy

```bash
# List all pipelines
databricks pipelines list-pipelines

# Check bundle deployment status
databricks bundle summary
```

## 📚 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Bundle Reference](https://docs.databricks.com/dev-tools/bundles/reference.html)
- [DLT Pipeline Configuration](https://docs.databricks.com/delta-live-tables/properties.html)

## 🎉 Summary

**Use Databricks Asset Bundles for:**
- ✅ Better dependency management (no vendoring!)
- ✅ Infrastructure as code
- ✅ Easier deployments and updates
- ✅ Multi-environment support
- ✅ Version control friendly
- ✅ CI/CD integration

The old CLI approach with vendoring should only be used if bundles are not available in your environment.

