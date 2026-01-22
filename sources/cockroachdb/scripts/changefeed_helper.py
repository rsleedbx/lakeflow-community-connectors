#!/usr/bin/env python3
"""
Changefeed Helper Utility

Command-line wrapper for CockroachDB changefeed operations and analysis.
Designed to be called from bash scripts instead of using inline Python.

Usage:
    changefeed_helper.py COMMAND [OPTIONS]

Commands:
    find-changefeeds --table TABLE --json CRDB_JSON
        Find existing changefeeds for a table
        
    check-status --job-id JOB_ID --json CRDB_JSON
        Check status of a changefeed job
        
    create-changefeed --table TABLE --azure-uri URI --format FORMAT --json CRDB_JSON
        Create a new changefeed to Azure
        
    cancel-changefeed --job-id JOB_ID --json CRDB_JSON
        Cancel a changefeed job
        
    get-row-count --table TABLE --json CRDB_JSON
        Get row count for a table
        
    execute-sql --sql SQL --json CRDB_JSON [--commit]
        Execute SQL query
        
    analyze-files --format FORMAT --azure-json AZURE_JSON [--prefix PREFIX]
        Analyze changefeed files in Azure Blob Storage
        
    create-schema-file --table TABLE --json CRDB_JSON --azure-json AZURE_JSON --prefix PREFIX
        Create and upload schema file to Azure
"""

import sys
import os
import argparse
import json

# Add parent directory to path to import cockroachdb module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import utility functions from cockroachdb module
from cockroachdb import (
    load_crdb_config, create_connector, analyze_azure_changefeed_files, 
    get_primary_keys, generate_test_table_sql, generate_test_insert_sql,
    generate_test_update_sql, generate_test_delete_sql, get_timestamped_path
)


def load_azure_config(json_path: str) -> dict:
    """Load Azure credentials from JSON file"""
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Azure config file not found: {json_path}")
    
    with open(json_path, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    required_fields = ['azure_storage_account', 'azure_storage_key']
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required field '{field}' in Azure config: {json_path}")
    
    return config


def cmd_find_changefeeds(args):
    """Find existing changefeeds for a table."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    changefeeds = connector.find_changefeeds_for_table(args.table)
    
    if changefeeds:
        print(f"  Found {len(changefeeds)} existing changefeed(s):")
        healthy_jobs = []
        
        for cf in changefeeds:
            job_id = cf['job_id']
            status = cf['status']
            has_errors = cf['has_errors']
            
            print(f"    Job {job_id}: {status}")
            
            # Cancel if has errors OR if force_new flag is set
            should_cancel = has_errors or args.force_new
            
            if should_cancel:
                if args.force_new:
                    print(f"      🔄 Cancelling (--force-new flag)...")
                else:
                    print(f"      ⚠️  Has errors - canceling...")
                
                result = connector.cancel_changefeed(job_id)
                
                if result['success']:
                    print(f"      ✅ Cancelled (status: {result['final_status']})")
                else:
                    print(f"      ⚠️  {result['message']}")
            else:
                healthy_jobs.append(job_id)
        
        if healthy_jobs and not args.force_new:
            print(f"\n  ✅ {len(healthy_jobs)} healthy changefeed(s) already running")
            print(f"     Job IDs: {', '.join(map(str, healthy_jobs))}")
            print(f"     Using existing changefeed: {healthy_jobs[0]}")
            # Exit with code 42 to signal "skip creation, but continue testing"
            sys.exit(42)
        elif args.force_new:
            print(f"\n  ✅ All changefeeds cancelled (--force-new)")
    else:
        print("  ✅ No existing changefeeds")


def cmd_check_status(args):
    """Check status of a changefeed job with enhanced error diagnostics."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    status_info = connector.check_changefeed_status(int(args.job_id))
    
    # Pretty print for human readability
    print(f"  Status: {status_info['status']}")
    if status_info['running_status']:
        print(f"  Running Status: {str(status_info['running_status'])[:200]}")
    if status_info['error']:
        print(f"  Error: {status_info['error']}")
    
    # Show categorized error info if available
    if status_info.get('error_category'):
        print("")
        print(f"  🔍 Error Analysis:")
        print(f"     Category: {status_info['error_category']}")
        print(f"     Description: {status_info['error_description']}")
        print(f"     Retryable: {'Yes' if status_info.get('is_retryable') else 'No'}")
        print(f"     💡 Suggestion: {status_info['error_suggestion']}")
    
    print("")
    if status_info['is_healthy']:
        print("  ✅ Changefeed is running normally")
    else:
        print("  ❌ Changefeed has errors!")
        sys.exit(1)


def cmd_create_changefeed(args):
    """Create a new changefeed to Azure."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    job_id = connector.create_changefeed_to_azure(
        table_name=args.table,
        azure_uri=args.azure_uri,
        changefeed_format=args.format,
        initial_scan='yes'
    )
    
    # Output just the job ID for easy capture in bash
    print(job_id)


def cmd_cancel_changefeed(args):
    """Cancel a changefeed job."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    result = connector.cancel_changefeed(int(args.job_id))
    
    if result['success']:
        print(f"✅ Cancelled (status: {result['final_status']})")
    else:
        print(f"⚠️  {result['message']}")
        sys.exit(1)


def cmd_get_row_count(args):
    """Get row count for a table."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    count = connector.get_table_row_count(args.table)
    
    # Output just the count for easy capture in bash
    print(count)


def cmd_execute_sql(args):
    """Execute SQL query."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    results = connector.execute_sql(args.sql, commit=args.commit)
    
    if results:
        for row in results:
            print('\t'.join(str(col) for col in row))


def cmd_get_latest_job(args):
    """Get the most recent healthy changefeed job ID for a table."""
    crdb_config = load_crdb_config(args.json)
    connector = create_connector(crdb_config)
    
    changefeeds = connector.find_changefeeds_for_table(args.table)
    
    if changefeeds:
        # Find first healthy changefeed (without errors)
        for cf in changefeeds:
            if not cf['has_errors']:
                print(cf['job_id'])
                return
        
        # If no healthy changefeed found, return the first one anyway
        print(changefeeds[0]['job_id'])
    else:
        sys.exit(1)


def cmd_get_primary_keys(args):
    """Get primary key columns for a table."""
    try:
        crdb_config = load_crdb_config(args.json)
        pk_columns = get_primary_keys(
            crdb_config,
            args.table,
            catalog=args.catalog,
            schema=args.schema
        )
        
        # Output as comma-separated list (easy to parse in bash)
        print(','.join(pk_columns))
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_generate_test_sql(args):
    """Generate CREATE TABLE + INSERT SQL for test data."""
    try:
        sql = generate_test_table_sql(
            args.table,
            schema=args.schema_type,
            row_count=args.rows,
            include_column_families=args.families
        )
        
        # Output SQL (ready to pipe to psql)
        print(sql)
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_generate_insert_sql(args):
    """Generate INSERT SQL for adding test rows."""
    try:
        sql = generate_test_insert_sql(
            args.table,
            schema=args.schema_type,
            row_count=args.rows,
            start_id=args.start_id,
            key_prefix=args.key_prefix
        )
        
        # Output SQL (ready to pipe to psql)
        print(sql)
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_generate_update_sql(args):
    """Generate UPDATE SQL for modifying existing rows."""
    try:
        sql = generate_test_update_sql(
            args.table,
            schema=args.schema_type,
            row_count=args.rows,
            start_id=args.start_id
        )
        
        # Output SQL (ready to pipe to psql)
        print(sql)
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_generate_delete_sql(args):
    """Generate DELETE SQL for removing rows."""
    try:
        sql = generate_test_delete_sql(
            args.table,
            schema=args.schema_type,
            row_count=args.rows,
            from_end=args.from_end
        )
        
        # Output SQL (ready to pipe to psql)
        print(sql)
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_get_timestamped_path(args):
    """Get timestamped path for a test scenario."""
    try:
        # Load dbutils from Databricks environment if available
        dbutils = None
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
        except:
            print("ERROR: dbutils not available. This command requires Databricks environment.", file=sys.stderr)
            sys.exit(1)
        
        path = get_timestamped_path(
            volume_base=args.volume_base,
            path_prefix=args.path_prefix,
            version=args.version,
            dbutils=dbutils
        )
        
        # Output just the path for easy capture in bash/notebooks
        print(path)
        sys.exit(0)
        
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def cmd_analyze_files(args):
    """Analyze changefeed files in Azure Blob Storage."""
    # Parse Azure config from JSON file
    azure_config_raw = load_azure_config(args.azure_json)
    
    # Extract Azure credentials
    account_name = azure_config_raw['azure_storage_account']
    account_key = azure_config_raw['azure_storage_key']
    container = azure_config_raw.get('azure_container', 'changefeed-events')
    
    format_type = args.format.lower()
    
    # Validate format
    if format_type not in ['parquet', 'json']:
        print(f"❌ Invalid format: {format_type}", file=sys.stderr)
        print("   Must be 'parquet' or 'json'", file=sys.stderr)
        sys.exit(1)
    
    # Auto-detect path prefix if not provided
    path_prefix = args.prefix
    if not path_prefix:
        path_prefix = f'{format_type}-cdc'
    
    # Determine primary key columns
    primary_key_columns = None
    if hasattr(args, 'primary_keys') and args.primary_keys:
        # Explicit primary keys provided
        primary_key_columns = [pk.strip() for pk in args.primary_keys.split(',')]
    elif hasattr(args, 'table') and args.table:
        # Infer from table name
        table_name = args.table.lower()
        if 'usertable' in table_name:
            primary_key_columns = ['ycsb_key']
        elif 'simple_test' in table_name:
            primary_key_columns = ['id']
        else:
            # Try to infer from data
            primary_key_columns = None
    
    # Analyze changefeed files
    print(f"📊 Analyzing {format_type.upper()} changefeed files...")
    print(f"   Account: {account_name}")
    print(f"   Container: {container}")
    print(f"   Prefix: {path_prefix or '(root)'}")
    if primary_key_columns:
        print(f"   Primary keys: {primary_key_columns}")
    print("")
    
    debug_mode = args.debug
    
    try:
        total_stats = analyze_azure_changefeed_files(
            account_name=account_name,
            account_key=account_key,
            container_name=container,
            path_prefix=path_prefix,
            format_type=format_type,
            primary_key_columns=primary_key_columns,
            debug=debug_mode
        )
    except Exception as e:
        print(f"❌ Failed to analyze changefeed files: {e}", file=sys.stderr)
        if debug_mode:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    # Check if any files were found
    if total_stats.get('file_count', 0) == 0:
        print(f"⚠️  No {format_type} files found with prefix: {path_prefix}")
        sys.exit(0)
    
    print(f"✅ Found {total_stats.get('file_count', 0)} {format_type} file(s)")
    
    # Show unique keys for Parquet format
    if 'unique_keys' in total_stats:
        print(f"   Deduplicated to {total_stats['unique_keys']:,} unique rows")
    
    print("")
    
    # Print results
    print("")
    print("=" * 80)
    print("📊 CHANGEFEED STATISTICS")
    print("=" * 80)
    print("")
    print(f"  📸 Snapshot Rows:    {total_stats['snapshot']:,}")
    print(f"  ➕ INSERT Operations: {total_stats['insert']:,}")
    print(f"  ✏️  UPDATE Operations: {total_stats['update']:,}")
    print(f"  ➖ DELETE Operations: {total_stats['delete']:,}")
    print("")
    print(f"  📈 Total Events:      {sum(total_stats.values()):,}")
    print("")
    print("=" * 80)
    
    # Helpful hint if only snapshot events found
    if total_stats['snapshot'] > 0 and total_stats['insert'] == 0 and total_stats['update'] == 0 and total_stats['delete'] == 0:
        print("")
        print("💡 Only SNAPSHOT events found - no CDC operations detected")
        print("")
        print("   Possible causes:")
        print("   1. Changefeed created with initial_scan='only' (snapshot-only mode)")
        print("   2. Changefeed stopped after initial scan completed")
        print("   3. No data changes occurred yet (changefeed waiting for changes)")
        print("")
        print("   To capture CDC operations:")
        print("   • Verify changefeed is still running:")
        print("     SHOW JOBS WHERE job_type = 'CHANGEFEED';")
        print("   • Generate test data changes:")
        print("     UPDATE usertable SET field0 = 'test' WHERE ycsb_key LIKE 'user%' LIMIT 10;")
        print("   • Wait 30-60 seconds for files to appear")
        print("")
    
    # Return stats as JSON for parsing by shell scripts
    if not args.no_json:
        print("JSON_STATS=" + json.dumps(total_stats))


def cmd_create_schema_file(args):
    """Create and upload schema file for a table to Azure (uses existing cockroachdb.py code)"""
    from cockroachdb import create_connector
    
    # Parse CockroachDB config
    crdb_config = load_crdb_config(args.json)
    
    # Parse Azure config from JSON file
    azure_config_raw = load_azure_config(args.azure_json)
    
    # Extract Azure credentials
    account_name = azure_config_raw['azure_storage_account']
    account_key = azure_config_raw['azure_storage_key']
    container = azure_config_raw.get('azure_container', 'changefeed-events')
    
    # Create connector (uses pg8000 internally)
    catalog = crdb_config.get('catalog', 'defaultdb')
    schema = crdb_config.get('schema', 'public')
    
    # Build Azure config for connector
    azure_config = {
        'account_name': account_name,
        'account_key': account_key,
        'container': container
    }
    
    try:
        # Create connector with Azure config
        connector = create_connector(crdb_config, catalog, schema, azure_config)
        
        # Dump table schema (uses existing _dump_table_schema method with pg8000)
        schema_info = connector._dump_table_schema(args.table)
        
        # Upload directly to Azure at the specified prefix path
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )
        
        # Schema file path: {prefix}/_metadata/schema.json
        schema_blob_name = f"{args.prefix}/_metadata/schema.json"
        
        blob_client = blob_service_client.get_blob_client(
            container=container,
            blob=schema_blob_name
        )
        
        # Convert to JSON and upload
        schema_json = json.dumps(schema_info, indent=2)
        blob_client.upload_blob(schema_json, overwrite=True)
        
        print(f"✅ Schema file created in Azure: {container}/{schema_blob_name}")
        if args.debug:
            print(f"   Primary keys: {schema_info['primary_keys']}")
            print(f"   Columns: {len(schema_info['columns'])}")
            print(f"   Has column families: {schema_info['has_column_families']}")
        
    except Exception as e:
        print(f"❌ Failed to upload schema to Azure: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='CockroachDB Changefeed Helper Utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # find-changefeeds command
    find_parser = subparsers.add_parser('find-changefeeds', help='Find existing changefeeds')
    find_parser.add_argument('--table', required=True, help='Table name')
    find_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    find_parser.add_argument('--force-new', action='store_true', help='Force new changefeed (exit 0 even if exists)')
    
    # check-status command
    status_parser = subparsers.add_parser('check-status', help='Check changefeed status')
    status_parser.add_argument('--job-id', required=True, help='Changefeed job ID')
    status_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    
    # create-changefeed command
    create_parser = subparsers.add_parser('create-changefeed', help='Create changefeed to Azure')
    create_parser.add_argument('--table', required=True, help='Table name')
    create_parser.add_argument('--azure-uri', required=True, help='Azure storage URI')
    create_parser.add_argument('--format', default='parquet', choices=['parquet', 'json'], help='Changefeed format')
    create_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    
    # cancel-changefeed command
    cancel_parser = subparsers.add_parser('cancel-changefeed', help='Cancel changefeed')
    cancel_parser.add_argument('--job-id', required=True, help='Changefeed job ID')
    cancel_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    
    # get-row-count command
    count_parser = subparsers.add_parser('get-row-count', help='Get table row count')
    count_parser.add_argument('--table', required=True, help='Table name')
    count_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    
    # execute-sql command
    sql_parser = subparsers.add_parser('execute-sql', help='Execute SQL query')
    sql_parser.add_argument('--sql', required=True, help='SQL query')
    sql_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    sql_parser.add_argument('--commit', action='store_true', help='Commit after execution')
    
    # get-latest-job command
    latest_parser = subparsers.add_parser('get-latest-job', help='Get latest changefeed job ID')
    latest_parser.add_argument('--table', required=True, help='Table name')
    latest_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    
    # get-primary-keys command
    pk_parser = subparsers.add_parser('get-primary-keys', help='Get primary key columns for a table')
    pk_parser.add_argument('--table', required=True, help='Table name')
    pk_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    pk_parser.add_argument('--catalog', default='defaultdb', help='Catalog name (default: defaultdb)')
    pk_parser.add_argument('--schema', default='public', help='Schema name (default: public)')
    
    # generate-test-sql command
    gen_sql_parser = subparsers.add_parser('generate-test-sql', help='Generate CREATE TABLE + INSERT SQL for test data')
    gen_sql_parser.add_argument('--table', required=True, help='Table name')
    gen_sql_parser.add_argument('--schema-type', default='simple', choices=['simple', 'ycsb'], help='Schema type (default: simple)')
    gen_sql_parser.add_argument('--rows', type=int, default=1000, help='Number of rows to generate (default: 1000)')
    gen_sql_parser.add_argument('--families', action='store_true', help='Include column families (YCSB schema only)')
    
    # generate-insert-sql command
    gen_insert_parser = subparsers.add_parser('generate-insert-sql', help='Generate INSERT SQL for adding test rows')
    gen_insert_parser.add_argument('--table', required=True, help='Table name')
    gen_insert_parser.add_argument('--schema-type', default='simple', choices=['simple', 'ycsb'], help='Schema type (default: simple)')
    gen_insert_parser.add_argument('--rows', type=int, default=50, help='Number of rows to insert (default: 50)')
    gen_insert_parser.add_argument('--start-id', type=int, default=10001, help='Starting ID for simple schema (default: 10001)')
    gen_insert_parser.add_argument('--key-prefix', default='newuser', help='Key prefix for YCSB schema (default: newuser)')
    
    # generate-update-sql command
    gen_update_parser = subparsers.add_parser('generate-update-sql', help='Generate UPDATE SQL for modifying existing rows')
    gen_update_parser.add_argument('--table', required=True, help='Table name')
    gen_update_parser.add_argument('--schema-type', default='simple', choices=['simple', 'ycsb'], help='Schema type (default: simple)')
    gen_update_parser.add_argument('--rows', type=int, default=400, help='Number of rows to update (default: 400)')
    gen_update_parser.add_argument('--start-id', type=int, default=1, help='Starting ID for simple schema (default: 1)')
    
    # generate-delete-sql command
    gen_delete_parser = subparsers.add_parser('generate-delete-sql', help='Generate DELETE SQL for removing rows')
    gen_delete_parser.add_argument('--table', required=True, help='Table name')
    gen_delete_parser.add_argument('--schema-type', default='simple', choices=['simple', 'ycsb'], help='Schema type (default: simple)')
    gen_delete_parser.add_argument('--rows', type=int, default=100, help='Number of rows to delete (default: 100)')
    gen_delete_parser.add_argument('--from-end', action='store_true', default=True, help='Delete from end of table (default: True)')
    
    # get-timestamped-path command
    timestamped_path_parser = subparsers.add_parser('get-timestamped-path', help='Get timestamped path for a test scenario')
    timestamped_path_parser.add_argument('--volume-base', required=True, help='Base volume path (e.g., dbfs:/Volumes/catalog/schema/volume)')
    timestamped_path_parser.add_argument('--path-prefix', required=True, help='Path prefix without timestamp (e.g., json/defaultdb/public/test-json_usertable_with_split)')
    timestamped_path_parser.add_argument('--version', type=int, default=0, help='Version to retrieve (0=latest, -1=oldest, default: 0)')
    timestamped_path_parser.add_argument('--debug', action='store_true', help='Enable debug output')
    
    # create-schema-file command
    schema_parser = subparsers.add_parser('create-schema-file', help='Create and upload schema file to Azure')
    schema_parser.add_argument('--table', required=True, help='Table name')
    schema_parser.add_argument('--json', required=True, help='Path to CockroachDB credentials JSON')
    schema_parser.add_argument('--azure-json', required=True, help='Path to Azure credentials JSON')
    schema_parser.add_argument('--prefix', required=True, help='Path prefix (e.g., parquet/defaultdb/public/test-parquet_simple_test_no_split/1767823340)')
    schema_parser.add_argument('--debug', action='store_true', help='Enable debug output')
    
    # analyze-files command
    analyze_parser = subparsers.add_parser('analyze-files', help='Analyze changefeed files in Azure')
    analyze_parser.add_argument('--format', required=True, choices=['parquet', 'json'], help='Changefeed format')
    analyze_parser.add_argument('--azure-json', required=True, help='Path to Azure credentials JSON')
    analyze_parser.add_argument('--prefix', help='Path prefix (default: auto-detected as parquet-cdc or json-cdc)')
    analyze_parser.add_argument('--table', help='Table name (used to infer primary key for deduplication)')
    analyze_parser.add_argument('--primary-keys', help='Comma-separated list of primary key columns (e.g., id,name)')
    analyze_parser.add_argument('--debug', action='store_true', help='Enable debug output')
    analyze_parser.add_argument('--no-json', action='store_true', help='Suppress JSON output for parsing')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Dispatch to command handler
    command_handlers = {
        'find-changefeeds': cmd_find_changefeeds,
        'check-status': cmd_check_status,
        'create-changefeed': cmd_create_changefeed,
        'cancel-changefeed': cmd_cancel_changefeed,
        'get-row-count': cmd_get_row_count,
        'execute-sql': cmd_execute_sql,
        'get-latest-job': cmd_get_latest_job,
        'get-primary-keys': cmd_get_primary_keys,
        'generate-test-sql': cmd_generate_test_sql,
        'generate-insert-sql': cmd_generate_insert_sql,
        'generate-update-sql': cmd_generate_update_sql,
        'generate-delete-sql': cmd_generate_delete_sql,
        'get-timestamped-path': cmd_get_timestamped_path,
        'create-schema-file': cmd_create_schema_file,
        'analyze-files': cmd_analyze_files
    }
    
    handler = command_handlers.get(args.command)
    if handler:
        try:
            handler(args)
        except Exception as e:
            print(f"❌ Error: {str(e)}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

