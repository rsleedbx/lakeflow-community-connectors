# Loading .env Files in VS Code for Jupyter Notebooks

This guide explains how to auto-load `cockroachdb_cdc_azure.env` when running the unit test notebook in VS Code.

## 🎯 Three Ways to Load Credentials

### **Option 1: test_credentials.json** (Recommended) ✅

**Best for:** Local testing, easy to edit

```bash
# Copy template
cp ../test_credentials.json.template ../test_credentials.json

# Edit with your credentials
vi ../test_credentials.json
```

**Pros:**
- ✅ Simple JSON format
- ✅ Easy to edit
- ✅ Works in any environment
- ✅ Already gitignored

**Cons:**
- ⚠️ Need to maintain separate file

---

### **Option 2: .env File with python-dotenv** 🔧

**Best for:** Reusing existing Azure/CockroachDB credentials

**Step 1: Install python-dotenv**
```bash
pip install python-dotenv
```

**Step 2: Place .env file in one of these locations:**
```
/Users/robert.lee/github/lakeflow-community-connectors/
├── cockroachdb_cdc_azure.env  ← Option A: Repo root
└── sources/
    └── cockroachdb_s3/
        └── cockroachdb_cdc_azure.env  ← Option B: Original location
```

**Step 3: Run the notebook**
The notebook will automatically detect and load the `.env` file using `python-dotenv`.

**Pros:**
- ✅ Reuse existing `.env` files
- ✅ Works for bash scripts AND notebooks
- ✅ Standard Python approach

**Cons:**
- ⚠️ Requires `python-dotenv` package
- ⚠️ Need to specify file paths

---

### **Option 3: VS Code Auto-Loading** 🚀

**Best for:** Automatic loading without code changes

VS Code can automatically load `.env` files for all Python execution in the workspace.

**Step 1: Create `.vscode/settings.json`**

```json
{
  "python.envFile": "${workspaceFolder}/cockroachdb_cdc_azure.env"
}
```

**Step 2: Place your `.env` file in repo root:**
```bash
cp sources/cockroachdb_s3/cockroachdb_cdc_azure.env cockroachdb_cdc_azure.env
```

**Step 3: Reload VS Code**
- Press `Cmd+Shift+P` → "Developer: Reload Window"
- Or restart VS Code

**Step 4: Run the notebook**
Environment variables will be automatically available via `os.environ.get()`.

**Pros:**
- ✅ Automatic - no code changes needed
- ✅ Works for all Python files in workspace
- ✅ VS Code native feature

**Cons:**
- ⚠️ VS Code specific (won't work in other editors)
- ⚠️ May not work in Databricks
- ⚠️ Requires workspace configuration

---

## 📋 .env File Format

Your `cockroachdb_cdc_azure.env` should look like:

```bash
# Azure Blob Storage for CockroachDB Changefeed
export AZURE_STORAGE_ACCOUNT=cockroachcdc1766161393
export AZURE_STORAGE_KEY=your_storage_key_here
export AZURE_STORAGE_CONTAINER=changefeed-events

# CockroachDB Connection
export COCKROACHDB_URL="postgresql://user:pass@host:port/db?sslmode=require"

# Optional: Runtime state
export CHANGEFEED_JOB_ID=1134071129406242817
```

**Note:** The `export` keyword is optional for `python-dotenv` but required for bash scripts.

---

## 🧪 Testing Credential Loading

The notebook includes a test cell to verify environment variables:

```python
# Cell 3: Environment Variables Check
import os
from os import environ

azure_account = environ.get("AZURE_STORAGE_ACCOUNT")
cockroach_url = environ.get("COCKROACHDB_URL")

print("Environment Variables Check:")
print(f"  AZURE_STORAGE_ACCOUNT: {azure_account or '❌ Not found'}")
print(f"  COCKROACHDB_URL: {'✅ Found' if cockroach_url else '❌ Not found'}")
```

**Expected Output:**
- ✅ If auto-loaded: Shows "✅ Found"
- ❌ If not loaded: Shows "❌ Not found" → Will fall back to manual loading

---

## 🔄 Credential Loading Priority

The notebook tries multiple methods in this order:

1. **test_credentials.json** (highest priority)
2. **.env file via python-dotenv** (searches multiple paths)
3. **Environment variables** (if auto-loaded by VS Code)
4. **Create template** (if nothing found)

---

## 🔒 Security

All credential files are gitignored:

```gitignore
# From .gitignore
sources/*/*.env
sources/*/credentials.*
sources/*/test_credentials.json
**/cockroachdb_cdc_*.env
*.env
```

✅ **Safe to use any method - credentials won't be committed!**

---

## 💡 Recommendation

**For local development:**
- Use **test_credentials.json** (simplest, most portable)

**For CI/CD or shared environments:**
- Use **environment variables** injected by your CI system

**For bash script compatibility:**
- Use **.env file** with `source` command

---

## 🐛 Troubleshooting

### "python-dotenv not installed"
```bash
pip install python-dotenv
```

### ".env file not found"
Check the paths:
```python
import os
print(os.path.abspath("../../cockroachdb_s3/cockroachdb_cdc_azure.env"))
```

### "Environment variables not auto-loaded"
- Check `.vscode/settings.json` exists
- Reload VS Code window
- Verify `.env` file is in workspace root

### "COCKROACHDB_URL not found in .env"
- Ensure the variable is defined in your `.env` file
- Check for typos in variable names
- Verify the file is actually being loaded (check the print statements)

---

## 📚 Related Files

- `cockroachdb.ipynb` - Main unit test notebook
- `test_credentials.json.template` - Template for JSON credentials
- `../../cockroachdb_s3/cockroachdb_cdc_azure.env` - Azure/CockroachDB credentials
- `.vscode/settings.json` - VS Code workspace settings (create if needed)







