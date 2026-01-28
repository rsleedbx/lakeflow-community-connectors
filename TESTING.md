# Testing Lakeflow Community Connectors

This guide explains how to run tests for community connectors.

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-org/lakeflow-community-connectors.git
cd lakeflow-community-connectors

# 2. Set up Python virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install package with dev dependencies (REQUIRED for tests to work)
pip install -e ".[dev]"

# 4. Run tests for your connector (replace 'github' with your connector)
pytest sources/github/test/ -v

# Or run all tests
pytest -v
```

## Developer Workflow

**Working on a specific connector?** Here's the recommended workflow:

```bash
# 1. Make changes to your connector
vim sources/myconnector/myconnector.py

# 2. Test ONLY your connector (fast feedback)
pytest sources/myconnector/test/ -v

# 3. Fix issues, repeat step 2 until tests pass

# 4. Before committing, run all tests (ensure no regressions)
pytest -v
```

💡 **Tip**: Running connector-specific tests is **much faster** than running all tests, giving you quicker feedback during development.

## Prerequisites

- **Python 3.8+**: Required for running tests
- **Virtual environment**: Recommended to avoid dependency conflicts
- **Editable install**: The `-e` flag is **required** for imports to work

## Test Structure

```
lakeflow-community-connectors/
├── tests/                          # Shared test framework
│   ├── test_suite.py              # Generic LakeflowConnect tests
│   ├── lakeflow_connect_test_utils.py  # Test utilities
│   └── test_utils.py              # Helper functions
│
└── sources/                        # Connector-specific tests
    ├── github/
    │   └── test/
    │       └── test_github_lakeflow_connect.py
    ├── hubspot/
    │   └── test/
    │       └── test_hubspot_lakeflow_connect.py
    └── cockroachdb/
        └── test/
            ├── test_cockroachdb_lakeflow_connect.py  # API tests
            └── test_cockroachdb_cdc_scenarios.py     # CDC integration tests
```

## Running Tests

### Run Tests for Specific Connector (Recommended)

When developing a connector, test your specific connector first:

```bash
# From project root
cd /path/to/lakeflow-community-connectors

# GitHub connector
pytest sources/github/test/ -v

# HubSpot connector
pytest sources/hubspot/test/ -v

# CockroachDB connector (API tests only)
pytest sources/cockroachdb/test/test_cockroachdb_lakeflow_connect.py -v

# Your custom connector (replace 'myconnector' with your connector name)
pytest sources/myconnector/test/ -v
```

### Run All Tests

After your connector tests pass, optionally run all tests to ensure no regressions:

```bash
pytest -v
```

### Run Tests by Pattern

```bash
# Run all tests with "github" in the name
pytest -k github -v

# Run only read_table tests
pytest -k read_table -v

# Run all tests except slow ones
pytest -m "not slow" -v
```

## Standard LakeflowConnect Tests

The `tests/test_suite.py` module provides a standard test framework that validates the LakeflowConnect API contract:

| Test Method | Description |
|-------------|-------------|
| `test_initialization()` | Validates connector initialization |
| `test_list_tables()` | Validates `list_tables()` returns list of strings |
| `test_get_table_schema()` | Validates schema is valid StructType |
| `test_read_table_metadata()` | Validates metadata has required fields |
| `test_read_table()` | Validates iterator returns dict records |

### Example: GitHub Connector Test

```python
# sources/github/test/test_github_lakeflow_connect.py
from tests.test_suite import LakeflowConnectTester
from sources.github.github import LakeflowConnect

def test_github_connector():
    config = load_config("configs/dev_config.json")
    tester = LakeflowConnectTester(config)
    report = tester.run_all_tests()
    tester.print_report(report)
    assert report.passed_tests == report.total_tests
```

## CockroachDB CDC Integration Tests

The CockroachDB connector includes **additional integration tests** that validate end-to-end CDC correctness:

### Prerequisites

1. **Databricks environment** (requires Spark + dbutils)
2. **Test data generated** by `test_cdc_matrix.sh`
3. **Unity Catalog Volume** with CDC files

### Running CDC Tests

```bash
# These tests require Databricks environment
# Run in Databricks workspace or via Databricks Connect
# See "Running Tests with Databricks Connect" section below for setup

pytest sources/cockroachdb/test/test_cockroachdb_cdc_scenarios.py -v
```

**Note**: CDC tests will skip if running locally without Databricks. See `sources/cockroachdb/CDC_TESTS_QUICKSTART.md` for details and the "Running Tests with Databricks Connect" section below for local execution.

## Configuration

### Test Configuration Files

Each connector may require configuration files in `sources/{connector}/configs/`:

- `dev_config.json` - Connection-level config (API tokens, URLs)
- `dev_table_config.json` - Table-specific options

Example:
```json
// sources/github/configs/dev_config.json
{
  "token": "ghp_xxxxx",
  "base_url": "https://api.github.com"
}
```

### Environment Variables

Some tests may require environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_xxxxx"
```

## Common Issues

### ImportError: cannot import name 'test_suite'

**Problem**: Tests fail with import errors.

**Solution**: Install package in editable mode with dev dependencies:
```bash
cd /path/to/lakeflow-community-connectors
pip install -e ".[dev]"
```

### Tests Skip: "No active Spark session"

**Problem**: CDC tests skip because Spark is not available.

**Solution**: Run in Databricks environment or configure Databricks Connect. See the "Running Tests with Databricks Connect" section below for detailed setup instructions.

### Authentication Errors

**Problem**: Tests fail with 401/403 errors.

**Solution**: 
1. Check configuration files have valid credentials
2. Verify API tokens haven't expired
3. Ensure tokens have required permissions

### Module Not Found Errors

**Problem**: Tests can't find connector modules.

**Solution**: Always run pytest from project root, not from test subdirectories.

```bash
# ✅ Correct - run from project root
cd /path/to/lakeflow-community-connectors
pytest sources/github/test/ -v

# ❌ Wrong - running from test directory
cd sources/github/test
pytest test_github_lakeflow_connect.py -v  # Will fail!
```

**Quick fix**: Just `cd` back to project root:
```bash
# If you're in sources/github/test/
cd ../../..
pytest sources/github/test/ -v  # Now it works!
```

### PySpark/Numpy Segfault on macOS

**Problem**: Tests crash with `Fatal Python error: Segmentation fault` in numpy/PySpark initialization.

**Cause**: Known incompatibility between PySpark, numpy, and macOS (especially Apple Silicon).

**Solution**: These tests **cannot run locally on macOS**. Use one of these alternatives:

1. **Databricks Workspace** (recommended) - Run tests in a notebook
2. **Databricks Connect** (see next section) - Execute locally, run remotely
3. **Linux environment** - Docker, VM, or Linux machine
4. **CI/CD** - Let GitHub Actions run tests on Linux runners

**Affected tests**: All tests that use `test_suite.py` (imports PySpark):
- `sources/github/test/`
- `sources/hubspot/test/`
- `sources/stripe/test/`
- `sources/example/test/`
- `sources/cockroachdb/test/`

## Running Tests with Databricks Connect

**Databricks Connect** allows you to run pytest locally on your Mac while executing PySpark code on a remote Databricks cluster, avoiding the macOS segfault issue.

### Prerequisites

1. A Databricks workspace
2. Personal access token for authentication
3. **Either:**
   - **Serverless compute enabled** (recommended - no cluster required)
   - **OR an active cluster** (cluster-based - requires cluster ID)

#### Serverless vs Cluster-based

| Feature | Serverless | Cluster-based |
|---------|------------|---------------|
| **Setup** | Easier - no cluster ID needed | Requires cluster ID |
| **Cost** | Pay only for compute used | Cluster must be running (cost even when idle) |
| **Startup** | Instant (auto-starts) | Must start cluster first |
| **Maintenance** | None | Manage cluster lifecycle |
| **Recommended for** | Development, testing | Production workloads with consistent usage |

**For pytest testing, Serverless is recommended** due to simpler setup and no need to manage cluster lifecycle.

### Quick Setup (Recommended)

#### Option A: Using VS Code Databricks Extension

If you have the **Databricks Extension for VS Code** installed:

1. **Configure Databricks Extension** (one-time setup):
   - Open VS Code Command Palette (`Cmd+Shift+P` or `Ctrl+Shift+P`)
   - Run: `Databricks: Configure Databricks Extension`
   - Follow prompts to authenticate and select your workspace
   - This creates/updates `~/.databrickscfg` automatically

2. **Choose Compute Type**:
   - **Serverless** (recommended): No cluster ID needed, skip to step 3
   - **Cluster-based**: 
     - In VS Code, open Databricks sidebar
     - Find your cluster under "Compute"
     - Copy the cluster ID (e.g., `1234-567890-abc123`)

3. **Run Setup Script**:
   ```bash
   ./setup_databricks_connect.sh
   
   # Script will:
   # - Detect VS Code Databricks Extension config
   # - Ask if using Serverless or Cluster-based compute
   # - Prompt for cluster ID only if using cluster-based
   # - Configure SPARK_REMOTE appropriately
   # - Test connection
   ```

4. **Run Tests in VS Code Terminal**:
   ```bash
   # In VS Code integrated terminal
   pytest sources/github/test/ -v
   ```

#### Option B: Manual CLI Setup

Use the provided setup script without VS Code extension:

```bash
# Run the interactive setup script
./setup_databricks_connect.sh

# Follow the prompts to enter:
# - Workspace URL
# - Personal access token
# - Compute type (Serverless or Cluster-based)
# - Cluster ID (only if cluster-based)

# Script will test connection and set SPARK_REMOTE
```

### Manual Setup (Advanced)

#### Step 1: Install Databricks Connect

```bash
# Activate your virtual environment
source .venv/bin/activate

# Install databricks-connect (matches your Databricks Runtime version)
# For Databricks Runtime 14.x (PySpark 3.5.x)
pip install databricks-connect==14.3.*

# For Databricks Runtime 13.x (PySpark 3.4.x)
pip install databricks-connect==13.3.*
```

#### Step 2: Configure Authentication

Create or edit `~/.databrickscfg`:

**For Serverless (recommended)**:
```ini
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi_your_personal_access_token_here
# No cluster_id needed for serverless
```

**For Cluster-based**:
```ini
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi_your_personal_access_token_here
cluster_id = 1234-567890-abc123
```

**Or** set environment variables:

**For Serverless**:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token_here"
# No DATABRICKS_CLUSTER_ID needed
```

**For Cluster-based**:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token_here"
export DATABRICKS_CLUSTER_ID="1234-567890-abc123"
```

#### Step 3: Verify Connection

```bash
# Test the connection
databricks-connect test

# Or with Python
python -c "from databricks.connect import DatabricksSession; print('✅ Connected!')"
```

#### Step 4: Configure Spark Remote

Set the `SPARK_REMOTE` environment variable to enable Databricks Connect:

**For Serverless** (recommended):
```bash
export SPARK_REMOTE="sc://your-workspace.cloud.databricks.com:443/;token=dapi_your_token"
```

**For Cluster-based**:
```bash
export SPARK_REMOTE="sc://your-workspace.cloud.databricks.com:443/;token=dapi_your_token;x-databricks-cluster-id=1234-567890-abc123"
```

**Or** add to your shell profile (`~/.bashrc`, `~/.zshrc`):

**For Serverless**:
```bash
# Add to ~/.zshrc or ~/.bashrc
export SPARK_REMOTE="sc://$(grep host ~/.databrickscfg | head -1 | cut -d'=' -f2 | tr -d ' ' | sed 's|https://||'):443/;token=$(grep token ~/.databrickscfg | head -1 | cut -d'=' -f2 | tr -d ' ')"
```

**For Cluster-based**:
```bash
# Add to ~/.zshrc or ~/.bashrc
export SPARK_REMOTE="sc://$(grep host ~/.databrickscfg | head -1 | cut -d'=' -f2 | tr -d ' ' | sed 's|https://||'):443/;token=$(grep token ~/.databrickscfg | head -1 | cut -d'=' -f2 | tr -d ' ');x-databricks-cluster-id=$(grep cluster_id ~/.databrickscfg | head -1 | cut -d'=' -f2 | tr -d ' ')"
```

### Running pytest with Databricks Connect

Once configured, run pytest normally:

```bash
# Activate virtual environment
source .venv/bin/activate

# Ensure SPARK_REMOTE is set
echo $SPARK_REMOTE

# Run tests - PySpark will execute remotely!
pytest sources/github/test/ -v
pytest sources/hubspot/test/ -v
pytest sources/cockroachdb/test/ -v

# Run all tests
pytest -v
```

### Troubleshooting Databricks Connect

#### Connection Test Fails

```bash
# Check configuration
cat ~/.databrickscfg

# Verify cluster is running
databricks clusters list

# Test with explicit configuration
python -c "
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.remote(
    host='https://your-workspace.cloud.databricks.com',
    token='dapi_your_token',
    cluster_id='1234-567890-abc123'
).getOrCreate()
print(f'✅ Spark version: {spark.version}')
"
```

#### Version Mismatch Errors

**Problem**: `databricks-connect` version doesn't match Databricks Runtime.

**Solution**: Check your cluster's Databricks Runtime version and install matching `databricks-connect`:

```bash
# For DBR 14.3
pip install databricks-connect==14.3.*

# For DBR 13.3
pip install databricks-connect==13.3.*
```

#### Cluster Not Running (Cluster-based only)

**Problem**: Tests fail with connection timeout when using cluster-based compute.

**Solution**: Ensure cluster is running:

```bash
# Start cluster via CLI
databricks clusters start --cluster-id 1234-567890-abc123

# Or start via web UI
```

**Note**: If using Serverless compute, this doesn't apply - serverless starts automatically.

#### Environment Variable Not Set

**Problem**: Tests still segfault even after installing Databricks Connect.

**Solution**: Ensure `SPARK_REMOTE` is set:

```bash
# Check if set
echo $SPARK_REMOTE

# If empty, set it:
export SPARK_REMOTE="sc://your-workspace:443/;token=dapi_xxx;x-databricks-cluster-id=xxx"

# Verify and run
pytest sources/example/test/ -v
```

### Performance Considerations

When using Databricks Connect:
- **First run is slow** - Initializes remote Spark session
- **Subsequent tests are faster** - Reuses existing session
- **Network latency** - Small overhead for remote execution
- **Cost** - Cluster must be running (consider auto-termination)

### Alternative: Run Tests in Databricks Notebook

If Databricks Connect setup is too complex, run tests directly in Databricks:

```python
# In Databricks notebook
%pip install -e .

# Run specific connector tests
!pytest sources/github/test/ -v

# Run all tests
!pytest -v
```

## Using VS Code to Run pytest

VS Code provides multiple ways to run pytest tests with Databricks Connect.

### Quick Start (VS Code)

```bash
# 1. Copy example VS Code configurations
cp .vscode/settings.example.json .vscode/settings.json
cp .vscode/launch.example.json .vscode/launch.json
cp .vscode/tasks.example.json .vscode/tasks.json

# 2. Edit .vscode/settings.json and replace SPARK_REMOTE with your values
# Or run setup script to get your SPARK_REMOTE string:
./setup_databricks_connect.sh

# 3. In VS Code:
# - Open Test Explorer (flask/beaker icon in sidebar)
# - Click refresh to discover tests
# - Click ▶️ to run any test

# 4. Or use integrated terminal:
pytest sources/github/test/ -v
```

### Prerequisites

1. **Install VS Code Extensions**:
   - [Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
   - [Databricks Extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks)

2. **Configure Databricks Connect** (see previous section)

3. **Configure Python Test Framework**:
   - Open VS Code Command Palette (`Cmd+Shift+P`)
   - Run: `Python: Configure Tests`
   - Select `pytest`
   - Select project root as test directory
   
   Or copy example configuration:
   ```bash
   # Copy example VS Code settings
   cp .vscode/settings.example.json .vscode/settings.json
   # Edit .vscode/settings.json to add your SPARK_REMOTE value
   ```

### Method 1: VS Code Test Explorer (Recommended)

The Test Explorer provides a graphical interface for running tests:

1. **Open Test Explorer**:
   - Click the flask/beaker icon in VS Code sidebar
   - Or: `View` → `Testing`

2. **Discover Tests**:
   - Tests should auto-discover
   - If not, click refresh icon in Test Explorer

3. **Run Tests**:
   - Click ▶️ icon next to any test/file/folder
   - Or right-click → `Run Test`
   - Output appears in Test Results panel

4. **Debug Tests**:
   - Click debug icon (🐛) next to test
   - Set breakpoints in test code
   - Debugger will pause at breakpoints

**Benefits**:
- ✅ Visual test hierarchy
- ✅ Run individual tests with one click
- ✅ See test status at a glance (✅ passed, ❌ failed)
- ✅ Integrated debugging
- ✅ Jump to test definition

### Method 2: VS Code Integrated Terminal

Run pytest commands directly in VS Code's terminal:

1. **Open Terminal**:
   - ``Ctrl+` `` (backtick) or `Terminal` → `New Terminal`

2. **Ensure SPARK_REMOTE is set**:
   ```bash
   # Check if set
   echo $SPARK_REMOTE
   
   # If not set, run setup
   source ./setup_databricks_connect.sh
   ```

3. **Run Tests**:
   ```bash
   # Run specific connector
   pytest sources/github/test/ -v
   
   # Run with output in terminal
   pytest sources/hubspot/test/ -v --tb=short
   
   # Run and stop on first failure
   pytest sources/cockroachdb/test/ -v -x
   ```

**Benefits**:
- ✅ Full pytest command-line options
- ✅ Familiar terminal workflow
- ✅ Easy to copy/paste commands
- ✅ Can chain commands

### Method 3: VS Code Tasks

Create reusable test tasks in `.vscode/tasks.json`.

**Quick Setup**: Copy the example file:
```bash
cp .vscode/tasks.example.json .vscode/tasks.json
```

**Or create manually**:

```json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "Test: GitHub Connector",
      "type": "shell",
      "command": "pytest sources/github/test/ -v",
      "group": "test",
      "problemMatcher": [],
      "presentation": {
        "echo": true,
        "reveal": "always",
        "panel": "new"
      }
    },
    {
      "label": "Test: All Connectors",
      "type": "shell",
      "command": "pytest -v",
      "group": {
        "kind": "test",
        "isDefault": true
      },
      "problemMatcher": []
    },
    {
      "label": "Test: Current File",
      "type": "shell",
      "command": "pytest ${file} -v",
      "group": "test",
      "problemMatcher": []
    }
  ]
}
```

**Run tasks**:
- `Cmd+Shift+P` → `Tasks: Run Task`
- Select task from list
- Or: `Cmd+Shift+B` for default test task

### Method 4: Code Lens (Inline Test Running)

Enable pytest integration to run tests inline:

1. **Add to `.vscode/settings.json`**:
   ```json
   {
     "python.testing.pytestEnabled": true,
     "python.testing.unittestEnabled": false,
     "python.testing.pytestArgs": [
       "sources",
       "-v"
     ]
   }
   ```

2. **Run Tests**:
   - Click `Run Test` / `Debug Test` above each test function
   - Appears as clickable link in editor

### VS Code + Databricks Connect Tips

1. **Set SPARK_REMOTE in VS Code Terminal Profile**:
   
   Add to `~/.zshrc` or `~/.bashrc`:
   ```bash
   # Load Databricks Connect config
   if [ -f ~/.databrickscfg ]; then
     export SPARK_REMOTE="sc://$(grep host ~/.databrickscfg | head -1 | cut -d'=' -f2- | tr -d ' ' | sed 's|https://||'):443/;token=$(grep token ~/.databrickscfg | head -1 | cut -d'=' -f2- | tr -d ' ');x-databricks-cluster-id=$(grep cluster_id ~/.databrickscfg | head -1 | cut -d'=' -f2- | tr -d ' ')"
   fi
   ```
   
   Then restart VS Code terminal.

2. **Use VS Code Workspace Settings**:
   
   Add to `.vscode/settings.json`:
   ```json
   {
     "terminal.integrated.env.osx": {
       "SPARK_REMOTE": "sc://your-workspace:443/;token=xxx;x-databricks-cluster-id=xxx"
     },
     "terminal.integrated.env.linux": {
       "SPARK_REMOTE": "sc://your-workspace:443/;token=xxx;x-databricks-cluster-id=xxx"
     }
   }
   ```

3. **Create Launch Configuration for Debugging**:
   
   **Quick Setup**: Copy the example file:
   ```bash
   cp .vscode/launch.example.json .vscode/launch.json
   # Edit to add your SPARK_REMOTE value
   ```
   
   **Or create manually** - Add to `.vscode/launch.json`:
   ```json
   {
     "version": "0.2.0",
     "configurations": [
       {
         "name": "Debug Current Test File",
         "type": "debugpy",
         "request": "launch",
         "module": "pytest",
         "args": [
           "${file}",
           "-v",
           "-s"
         ],
         "env": {
           "SPARK_REMOTE": "sc://your-workspace:443/;token=xxx;x-databricks-cluster-id=xxx"
         },
         "console": "integratedTerminal",
         "justMyCode": false
       }
     ]
   }
   ```

4. **Keyboard Shortcuts**:
   
   Add to `keybindings.json`:
   ```json
   [
     {
       "key": "cmd+shift+t",
       "command": "python.runTestMethod"
     },
     {
       "key": "cmd+shift+d",
       "command": "python.debugTestMethod"
     }
   ]
   ```

### Troubleshooting VS Code Testing

**Problem**: Test Explorer shows "No tests discovered"

**Solution**:
1. Check Python interpreter is correct: `Cmd+Shift+P` → `Python: Select Interpreter`
2. Ensure package is installed: `pip install -e ".[dev]"`
3. Check pytest configuration in `pyproject.toml`
4. Refresh Test Explorer: Click refresh icon

**Problem**: Tests pass in terminal but fail in Test Explorer

**Solution**: Ensure `SPARK_REMOTE` is set in VS Code terminal environment (see tips above).

**Problem**: Databricks Extension conflicts with testing

**Solution**: Databricks Extension and Python testing extension work independently. No conflicts expected.

## Creating Tests for New Connectors

When creating a new connector, follow this pattern:

1. **Create test file**: `sources/{connector}/test/test_{connector}_lakeflow_connect.py`

2. **Use standard test suite**:
```python
from tests.test_suite import LakeflowConnectTester
from sources.{connector}.{connector} import LakeflowConnect

def test_{connector}_connector():
    # Inject your LakeflowConnect class
    test_suite.LakeflowConnect = LakeflowConnect
    
    # Load config
    config = load_config("configs/dev_config.json")
    
    # Run tests
    tester = LakeflowConnectTester(config)
    report = tester.run_all_tests()
    tester.print_report(report)
    
    # Assert all passed
    assert report.passed_tests == report.total_tests
```

3. **Add configuration**: Create `configs/dev_config.json` with test credentials

4. **Document prerequisites**: Update connector README with test setup instructions

## CI/CD Integration

Tests can be integrated into GitHub Actions:

```yaml
# .github/workflows/test.yml
name: Test Connectors

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install -e ".[dev]"
      
      - name: Run tests
        run: pytest -v
```

## Further Reading

- **Standard API Tests**: `tests/test_suite.py` - Framework documentation
- **CDC Integration Tests**: `sources/cockroachdb/CDC_TEST_FRAMEWORK_INTEGRATION.md`
- **CockroachDB Testing**: `sources/cockroachdb/CDC_TESTS_QUICKSTART.md`
- **Connector Development**: `prompts/vibe_coding_instruction.md`
