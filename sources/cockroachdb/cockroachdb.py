from typing import Dict, List, Iterator, Any
from enum import Enum
import json
import ssl
import os
import subprocess

# Lazy import PySpark types only when needed
try:
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, IntegerType,
        DoubleType, BooleanType, DateType, TimestampType, BinaryType,
        DecimalType, ArrayType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    # Define stub types for non-Databricks environments
    StructType = type('StructType', (), {})
    StructField = type('StructField', (), {})
    StringType = type('StringType', (), {})
    LongType = type('LongType', (), {})
    IntegerType = type('IntegerType', (), {})
    DoubleType = type('DoubleType', (), {})
    BooleanType = type('BooleanType', (), {})
    DateType = type('DateType', (), {})
    TimestampType = type('TimestampType', (), {})
    BinaryType = type('BinaryType', (), {})
    DecimalType = type('DecimalType', (), {})
    ArrayType = type('ArrayType', (), {})


class ConnectorMode(str, Enum):
    """
    Enumeration of connector operation modes.
    
    Modes:
        VOLUME: Read JSON/Parquet files from Unity Catalog Volumes
        AZURE_PARQUET: Read Parquet files from Azure Blob Storage
        AZURE_JSON: Read JSON files from Azure Blob Storage
        AZURE_DUAL: Read both JSON and Parquet from Azure Blob Storage
        DIRECT: Direct sinkless changefeed connection (instream CDC)
    """
    VOLUME = "volume"
    AZURE_PARQUET = "azure_parquet"
    AZURE_JSON = "azure_json"
    AZURE_DUAL = "azure_dual"
    DIRECT = "direct"
    
    def __str__(self):
        """Return the string value for compatibility with existing code."""
        return self.value
    
    @classmethod
    def from_string(cls, mode_str: str) -> 'ConnectorMode':
        """
        Convert string to ConnectorMode enum.
        
        Args:
            mode_str: String representation of mode
            
        Returns:
            ConnectorMode enum value
            
        Raises:
            ValueError: If mode_str is not a valid mode
        """
        try:
            return cls(mode_str)
        except ValueError:
            valid_modes = [m.value for m in cls]
            raise ValueError(
                f"Invalid mode '{mode_str}'. Valid modes: {valid_modes}"
            )


class LakeflowConnect:
    """
    CockroachDB connector with tri-mode operation:
    
    Mode 1: Direct sinkless changefeed (testing/development)
    Mode 2: Azure Parquet changefeed (production CDC)
    Mode 3: Unity Catalog Volume (local testing with pre-synced files)
    
    CockroachDB CDC Format Support:
    ===============================
    
    Parquet Format (Native CockroachDB):
    - Uses __crdb__event_type column to indicate operation:
      * 'c' = snapshot/change (used for BOTH initial snapshots AND updates - indistinguishable by type)
      * 'i' = insert (new row from CDC)
      * 'd' = delete (removed row from CDC)
      * NOTE: No 'u' event type exists in Parquet - updates also use 'c'
    - Data columns are at the top level (not nested)
    - Includes __crdb__updated timestamp
    - Works for both snapshot AND CDC events
    - UPDATE DETECTION: Uses timestamp-based logic to distinguish snapshots from updates:
      * Events with __crdb__updated <= snapshot_cutoff_timestamp = SNAPSHOT
      * Events with __crdb__updated > snapshot_cutoff_timestamp = UPDATE
      * Snapshot cutoff is captured when changefeed is created/first run
    
    JSON Format (Wrapped Envelope):
    - Uses 'before' and 'after' columns
    - 'after' only = snapshot or insert
    - 'after' + 'before' = update
    - 'before' only = delete
    
    Important: CDC File Flush Timing
    ================================
    - Snapshot files: Appear within 30 seconds
    - CDC files: Appear after 60+ seconds (batched by time and size)
    - Must wait at least 60 seconds after workload for CDC files to flush
    - Both JSON and Parquet follow similar flush patterns
    
    Testing Results (December 22, 2025):
    - All format/table/split_column_families combinations work correctly
    - 100% success rate (6/6 valid tests passed)
    - See CDC_TEST_MATRIX_RESULTS.md for detailed analysis
    """
    
    # Class variable to share snapshot timestamp across tables in multi-table pipelines
    # This ensures all tables use the same timestamp for snapshot consistency
    _shared_snapshot_timestamp = None
    
    # Class variable to cache column family detection results per table
    # Format: {(schema, table_name): has_multiple_families}
    _column_family_cache = {}
    
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the CockroachDB connector.
        
        Supports two connection modes:
        
        1. GitHub-style (recommended - works with Unity Catalog):
            - token: Username and password as "username:password"
            - base_url: Server URL without credentials
              Format: postgresql://host:port/database?sslmode=require
            Connector reconstructs: postgresql://username:password@host:port/database?sslmode=require
        
        2. Individual parameters (for local testing only):
            - host: CockroachDB host
            - port: CockroachDB port (default: 26257)
            - database: Database name
            - user: Username
            - password: Password (can be empty for insecure mode)
            - sslmode: SSL mode (default: 'require')
        
        REQUIRED Parameters (for multi-database deployments):
            - catalog: Database/catalog name (e.g., 'ecommerce', 'warehouse')
            - schema: Schema name (e.g., 'public', 'staging')
        
        Optional Parameters:
            - format: Changefeed format ('parquet', 'json', or 'both', default: 'parquet')
        
        Tri-Mode Operation:
        
        - Direct Mode: Only CockroachDB credentials provided
          Uses sinkless changefeed for immediate data streaming
        
        - Azure Parquet/JSON Mode: Azure credentials also provided
          Creates changefeed to Azure Blob Storage (Parquet and/or JSON format)
          Reads files back from Azure for production performance
        
        - Volume Mode: volume_path provided
          Reads pre-synced files from Unity Catalog Volume
        """
        # Note: We do NOT create a connection in __init__ to avoid Spark serialization issues
        # Connection will be created lazily in methods that need it
        
        # Catalog and Schema for namespace isolation (optional for utility-only usage)
        self.catalog = options.get('catalog', 'defaultdb')
        self.schema = options.get('schema', 'public')
        
        # Warn if missing (but don't fail - allows utility method usage)
        if 'catalog' not in options or 'schema' not in options:
            import warnings
            if options:  # Only warn if options were provided (not empty dict)
                warnings.warn(
                    "catalog/schema not specified - using defaults. "
                    "Specify 'catalog' and 'schema' for production use.",
                    UserWarning
                )
        
        # Format selection: 'parquet' (default), 'json', or 'both'
        self.format = options.get('format', 'parquet').lower()
        if self.format not in ['parquet', 'json', 'both']:
            raise ValueError(f"Invalid format '{self.format}'. Must be 'parquet', 'json', or 'both'.")
        
        # Parse connection credentials
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
            self.host = options.get("host")
            self.port = int(options.get("port", "26257"))
            self.database = options.get("database")
            self.user = options.get("user")
            self.password = options.get("password", "")
            self.sslmode = options.get("sslmode", "require")
        else:
            # No credentials provided (OK for volume-only mode)
            self.host = None
            self.database = None
            self.user = None
            self.password = None
            self.port = None
            self.sslmode = None
            
            # Debug: Log what keys were found in options for troubleshooting
            if options and any(k in options for k in ['token', 'base_url', 'connection_url', 'cockroachdb_url']):
                import warnings
                found_keys = [k for k in ['token', 'base_url', 'connection_url', 'cockroachdb_url', 'host'] if k in options]
                warnings.warn(
                    f"CockroachDB credentials found but not parsed. Keys in options: {found_keys}. "
                    f"Check that token/base_url/cockroachdb_url values are non-empty strings.",
                    UserWarning
                )
        
        # Detect Volume path for volume-based reading
        self.volume_path = options.get("volume_path")
        
        # Detect Azure credentials for dual-mode operation
        self.azure_account_name = options.get("azure_account_name")
        self.azure_account_key = options.get("azure_account_key")
        self.azure_container = options.get("azure_container")
        
        # Store Spark session and dbutils for Spark Connect compatibility
        # These should be passed from caller (e.g., DLT, notebooks) to avoid
        # SparkSession.builder.getOrCreate() which doesn't work in Spark Connect
        self._spark = options.get("spark")
        self._dbutils = options.get("dbutils")
        
        # Auto-construct storage paths using format-first hierarchy
        self._setup_storage_paths(options)
        
        # Determine operation mode (priority: volume > azure > direct)
        if self.volume_path:
            self.mode = ConnectorMode.VOLUME
            # print(f"Mode: Volume | Path: {self.volume_path}")  # Debug only
        elif self.azure_account_name and self.azure_account_key and self.azure_container:
            # Mode name depends on format
            if self.format == 'both':
                self.mode = ConnectorMode.AZURE_DUAL
                # print(f"Mode: Azure Dual (JSON+Parquet) | Container: {self.azure_container}")  # Debug only
            elif self.format == 'json':
                self.mode = ConnectorMode.AZURE_JSON
                # print(f"Mode: Azure JSON | Container: {self.azure_container}")  # Debug only
            else:
                self.mode = ConnectorMode.AZURE_PARQUET
                # print(f"Mode: Azure Parquet | Container: {self.azure_container}")  # Debug only
        else:
            self.mode = ConnectorMode.DIRECT
            # print(f"Mode: Direct Sinkless | Database: {self.database}")  # Debug only
        
        # Instance variable to track snapshot cutoff timestamp for UPDATE detection in Parquet
        # Format: {table_name: cutoff_timestamp}
        self._snapshot_cutoff_timestamps = {}
    
    def _ensure_spark_and_dbutils(self, spark=None, dbutils=None):
        """
        Validate and return spark and dbutils, ensuring they are available.
        
        This is the SINGLE SOURCE OF TRUTH for spark/dbutils validation.
        No fallbacks - spark and dbutils are REQUIRED for Spark Connect compatibility.
        
        Args:
            spark: Optional SparkSession (uses self._spark if not provided)
            dbutils: Optional DBUtils (uses self._dbutils if not provided)
            
        Returns:
            Tuple of (spark, dbutils)
            
        Raises:
            RuntimeError: If spark or dbutils not available
        """
        # Use provided or stored instances
        effective_spark = spark or self._spark
        effective_dbutils = dbutils or self._dbutils
        
        # Validate spark is available
        if effective_spark is None:
            raise RuntimeError(
                "SparkSession is required but not available. "
                "Pass 'spark' parameter or initialize connector with 'spark' in options dict.\n"
                "Example: LakeflowConnect({'volume_path': '...', 'spark': spark, 'dbutils': dbutils})"
            )
        
        # Validate dbutils is available
        if effective_dbutils is None:
            raise RuntimeError(
                "DBUtils is required but not available. "
                "Pass 'dbutils' parameter or initialize connector with 'dbutils' in options dict.\n"
                "Example: LakeflowConnect({'volume_path': '...', 'spark': spark, 'dbutils': dbutils})"
            )
        
        return effective_spark, effective_dbutils
    
    def _parse_connection_url(self, url: str) -> None:
        """Parse PostgreSQL connection URL into individual components."""
        import re
        from urllib.parse import urlparse, parse_qs
        
        parsed = urlparse(url)
        
        self.user = parsed.username
        self.password = parsed.password or ""
        self.host = parsed.hostname
        self.port = parsed.port or 26257
        self.database = parsed.path.lstrip("/").split("?")[0]
        
        query_params = parse_qs(parsed.query)
        self.sslmode = query_params.get("sslmode", ["require"])[0]
    
    def _setup_storage_paths(self, options: Dict[str, str]) -> None:
        """
        Setup format-specific storage paths using format-first hierarchy.
        
        Storage structure:
            {format}/{catalog}/{schema}/{table}/YYYY-MM-DD/files
        
        Examples:
            parquet/ecommerce/public/orders/2025-12-23/...
            json/ecommerce/public/orders/2025-12-23/...
        """
        base_path = f"{self.catalog}/{self.schema}"
        
        if self.format == 'both':
            # Create paths for both formats
            self.json_path_prefix = f"json/{base_path}"
            self.parquet_path_prefix = f"parquet/{base_path}"
            self.azure_path_prefix = None  # Will use format-specific paths
        elif self.format == 'json':
            self.azure_path_prefix = f"json/{base_path}"
            self.json_path_prefix = self.azure_path_prefix
            self.parquet_path_prefix = None
        else:  # parquet
            self.azure_path_prefix = f"parquet/{base_path}"
            self.parquet_path_prefix = self.azure_path_prefix
            self.json_path_prefix = None
    
    def _get_fully_qualified_table(self, table_name: str) -> str:
        """Return fully qualified table name: catalog.schema.table"""
        return f"{self.catalog}.{self.schema}.{table_name}"
    
    def _get_schema_from_files(self, table_name: str) -> StructType:
        """
        Get Spark schema for a table by reading from files (Volume or Azure).
        
        This method infers the schema by reading a sample data file, since the
        _schema.json file only contains primary keys/column families, not full column types.
        
        Works for both:
        - VOLUME mode: Reads from Unity Catalog Volumes
        - AZURE modes: Reads from Azure Blob Storage
        """
        from pyspark.sql.types import StructType
        
        # VOLUME mode: Use volume files
        if self.mode == ConnectorMode.VOLUME:
            spark, dbutils = self._ensure_spark_and_dbutils(self._spark, self._dbutils)
            
            # Get file list from volume
            file_list = self._list_volume_files(self.volume_path, spark=spark, dbutils=dbutils)
            
            if not file_list:
                raise ValueError(f"No files found in volume: {self.volume_path}")
            
            # Filter out metadata files
            data_files = [f for f in file_list if not f['name'].startswith('_')]
            
            if not data_files:
                raise ValueError(f"No data files found in volume: {self.volume_path}")
            
            # Read first file to infer Spark schema
            sample_file = data_files[0]
            is_json = sample_file['name'].endswith(('.ndjson', '.json'))
            file_path = sample_file['path']
            
        # AZURE modes: Use Azure blob files
        else:
            from azure.storage.blob import BlobServiceClient
            from io import BytesIO
            import pandas as pd
            
            spark, _ = self._ensure_spark_and_dbutils(self._spark, None)
            
            # Connect to Azure
            self._ensure_azure_dependencies()
            account_url = f"https://{self.azure_account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.azure_account_key
            )
            container_client = blob_service_client.get_container_client(self.azure_container)
            
            # Determine path prefix and format
            if self.mode == ConnectorMode.AZURE_JSON or self.format == 'json':
                path_prefix = self.json_path_prefix if hasattr(self, 'json_path_prefix') else f"json/{self.catalog}/{self.schema}/{table_name}"
                is_json = True
                file_ext = '.ndjson'
            else:
                path_prefix = self.parquet_path_prefix if hasattr(self, 'parquet_path_prefix') else f"parquet/{self.catalog}/{self.schema}/{table_name}"
                is_json = False
                file_ext = '.parquet'
            
            # List blobs (recursively includes subdirectories like date dirs)
            blob_list = container_client.list_blobs(name_starts_with=path_prefix)
            data_files = [blob for blob in blob_list 
                         if file_ext in blob.name 
                         and '/_metadata/' not in blob.name 
                         and not blob.name.split('/')[-1].startswith('_')]
            
            if not data_files:
                raise ValueError(f"No {file_ext} files found in Azure: {path_prefix}")
            
            # Read first file from Azure
            sample_blob = data_files[0]
            blob_client = blob_service_client.get_blob_client(container=self.azure_container, blob=sample_blob.name)
            download_stream = blob_client.download_blob()
            file_data = BytesIO(download_stream.readall())
            
            # For Azure, we need to read into Spark differently
            # Use pandas as intermediate for small sample file
            if is_json:
                pdf = pd.read_json(file_data, lines=True)
                df = spark.createDataFrame(pdf)
            else:
                pdf = pd.read_parquet(file_data)
                df = spark.createDataFrame(pdf)
            
            return df.schema
        
        # For VOLUME mode, read file and infer schema
        if is_json:
            df = spark.read.json(file_path)
        else:
            df = spark.read.parquet(file_path)
        
        # Return inferred Spark schema (StructType with field names and types)
        return df.schema
    
    def _get_metadata_from_files(self, table_name: str) -> Dict[str, Any]:
        """
        Get table metadata by reading from _schema.json file (Volume or Azure).
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with primary_keys, cursor_field, ingestion_type
        """
        # VOLUME mode: Read from Unity Catalog Volume
        if self.mode == ConnectorMode.VOLUME:
            spark, dbutils = self._ensure_spark_and_dbutils(self._spark, self._dbutils)
            
            # Load schema metadata from _schema.json
            schema_info = self._load_schema_from_volume(self.volume_path, spark=spark, dbutils=dbutils)
            
            if not schema_info:
                raise ValueError(
                    f"Schema file not found in volume: {self.volume_path}/_schema.json\n"
                    f"Run test_cdc_matrix.sh to generate schema files."
                )
        
        # AZURE modes: Read from Azure Blob Storage
        else:
            # Determine format type
            if self.mode == ConnectorMode.AZURE_JSON:
                format_type = 'json'
            else:
                format_type = 'parquet'  # AZURE_PARQUET or AZURE_DUAL default to parquet
            
            # Load schema metadata from Azure
            schema_info = self._load_schema_from_azure(table_name, format_type=format_type)
            
            if not schema_info:
                raise ValueError(
                    f"Schema file not found in Azure for table '{table_name}' ({format_type} format)\n"
                    f"Run test_cdc_matrix.sh to generate schema files."
                )
        
        primary_keys = schema_info.get('primary_keys', [])
        
        if not primary_keys:
            raise ValueError(
                f"No primary keys found in schema file for table '{table_name}'\n"
                f"Ensure the table has a primary key defined."
            )
        
        return {
            "primary_keys": primary_keys,
            "cursor_field": "_cdc_updated",
            "ingestion_type": "cdc"
        }
    
    def _get_connection(self, table_options: Dict[str, str] = None):
        """Create and return a new connection to CockroachDB.
        
        Uses pg8000 (pure Python PostgreSQL driver) to avoid psycopg2/libpq SSL issues.
        pg8000 is installed as a regular Python dependency via requirements.txt.
        """
        try:
            import pg8000
            
            # Create SSL context that doesn't verify certificates
            # This avoids the /root/.postgresql/ permission issues with psycopg2
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            conn = pg8000.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                ssl_context=ssl_context
            )
            return conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect to CockroachDB: {str(e)}")
    
    def _execute_query(self, conn, query: str, params: tuple = None):
        """Execute a query with driver-agnostic API."""
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur.fetchall()
    
    def _create_cursor(self, conn):
        """Create a cursor with driver-agnostic API."""
        return conn.cursor()
    
    def list_tables(self, table_options: Dict[str, str] = None) -> List[str]:
        """List all tables in the specified schema."""
        conn = self._get_connection(table_options)
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            rows = self._execute_query(conn, query, (self.schema,))
            return [row[0] for row in rows]
        finally:
            conn.close()
    
    def get_table_schema(self, table_name: str, table_options: Dict[str, str] = None) -> StructType:
        """Get the Spark schema for a given table."""
        # In file-based modes (VOLUME or AZURE), infer schema from files instead of querying CockroachDB
        if self.mode in [ConnectorMode.VOLUME, ConnectorMode.AZURE_PARQUET, ConnectorMode.AZURE_JSON, ConnectorMode.AZURE_DUAL]:
            return self._get_schema_from_files(table_name)
        
        # For DIRECT mode, query CockroachDB
        if table_name not in self.list_tables(table_options):
            raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
        
        conn = self._get_connection(table_options)
        try:
            query = """
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s 
                  AND table_name = %s
                ORDER BY ordinal_position
            """
            
            columns = self._execute_query(conn, query, (self.schema, table_name))
            
            fields = []
            for col in columns:
                col_name, data_type, char_max_len, num_precision, num_scale, is_nullable = col
                nullable = True  # Always nullable for CDC
                spark_type = self._map_cockroachdb_type_to_spark(
                    data_type, char_max_len, num_precision, num_scale
                )
                fields.append(StructField(col_name, spark_type, nullable))
            
            # Add CDC metadata fields
            fields.append(StructField("_cdc_key", ArrayType(StringType()), True))
            fields.append(StructField("_cdc_updated", StringType(), True))
            fields.append(StructField("_cdc_operation", StringType(), True))
            
            return StructType(fields)
        finally:
            conn.close()
    
    def _map_cockroachdb_type_to_spark(
        self, data_type: str, char_max_len: int, num_precision: int, num_scale: int
    ) -> Any:
        """Map CockroachDB data types to Spark SQL data types."""
        data_type = data_type.lower()
        if data_type in ("uuid", "string", "varchar", "text", "char", "character"):
            return StringType()
        elif data_type in ("int", "integer", "smallint", "bigint"):
            return LongType()
        elif data_type in ("float", "double precision"):
            return DoubleType()
        elif data_type == "boolean":
            return BooleanType()
        elif data_type == "date":
            return DateType()
        elif data_type in ("timestamp", "timestamptz"):
            return TimestampType()
        elif data_type == "bytes":
            return BinaryType()
        elif data_type == "decimal" or data_type == "numeric":
            if num_precision is not None and num_scale is not None:
                return DecimalType(num_precision, num_scale)
            return DecimalType(38, 18)
        elif data_type == "jsonb":
            return StringType()
        else:
            print(f"⚠️  Warning: Unknown CockroachDB type '{data_type}', mapping to StringType.")
            return StringType()
    
    def _has_multiple_column_families(self, table_name: str, table_options: Dict[str, str] = None) -> bool:
        """
        Check if a table has multiple column families by analyzing SHOW CREATE TABLE.
        
        Results are cached in class variable to avoid repeated queries.
        
        Args:
            table_name: Name of the table to check
            table_options: Optional connection parameters
        
        Returns:
            True if table has multiple column families, False otherwise
        """
        # Check cache first
        cache_key = (self.schema, table_name)
        if cache_key in LakeflowConnect._column_family_cache:
            return LakeflowConnect._column_family_cache[cache_key]
        
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            # Get the CREATE TABLE statement
            target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            cursor.execute(f"SHOW CREATE TABLE {target}")
            result = cursor.fetchone()
            
            if not result or len(result) < 2:
                cursor.close()
                conn.close()
                LakeflowConnect._column_family_cache[cache_key] = False
                return False
            
            create_statement = result[1]
            family_count = create_statement.upper().count('FAMILY ')
            has_multiple = family_count > 1
            
            cursor.close()
            conn.close()
            
            LakeflowConnect._column_family_cache[cache_key] = has_multiple
            return has_multiple
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            LakeflowConnect._column_family_cache[cache_key] = False
            return False
    
    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """Read table metadata."""
        # In file-based modes (VOLUME or AZURE), read metadata from _schema.json file
        if self.mode in [ConnectorMode.VOLUME, ConnectorMode.AZURE_PARQUET, ConnectorMode.AZURE_JSON, ConnectorMode.AZURE_DUAL]:
            return self._get_metadata_from_files(table_name)
        
        # For DIRECT mode, query CockroachDB
        if table_name not in self.list_tables(table_options):
            raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
        
        conn = self._get_connection(table_options)
        try:
            pk_query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                  AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """
            
            rows = self._execute_query(conn, pk_query, (self.schema, table_name))
            pk_columns = [row[0] for row in rows]
            
            if not pk_columns:
                raise ValueError(f"Table '{table_name}' has no primary key.")
            
            return {
                "primary_keys": pk_columns,
                "cursor_field": "_cdc_updated",
                "ingestion_type": "cdc"
            }
        finally:
            conn.close()
    
    def read_table(
        self, table_name: str, start_offset: Dict[str, str], table_options: Dict[str, str]
    ) -> Iterator[Dict[str, Any]]:
        """
        Read data from a CockroachDB table.
        
        Routes to one of three modes based on configuration:
        - volume: Read Parquet files from Databricks Volume
        - azure_parquet: Read from Azure Blob Storage via changefeed
        - direct: Direct sinkless changefeed connection
        """
        print(f"Reading table: {table_name} (mode={self.mode}, cursor={start_offset.get('cursor') if start_offset else None})")
        
        if self.mode == ConnectorMode.VOLUME:
            return self._read_table_from_volume(table_name, start_offset, table_options)
        elif self.mode == ConnectorMode.AZURE_PARQUET:
            return self._read_table_from_azure_parquet(table_name, start_offset, table_options)
        else:
            return self._read_table_direct(table_name, start_offset, table_options)
    
    def _read_table_direct(
        self, table_name: str, start_offset: Dict[str, str], table_options: Dict[str, str]
    ) -> Iterator[Dict[str, Any]]:
        """Read data from a CockroachDB table using direct sinkless changefeeds."""
        import time
        
        if table_name not in self.list_tables(table_options):
            raise ValueError(f"Table '{table_name}' not found")
        
        cursor = start_offset.get("cursor") if start_offset else None
        resolved_interval = table_options.get("resolved_interval", "1s")
        
        # Determine changefeed mode
        initial_scan_config = table_options.get("initial_scan", "only")
        
        if initial_scan_config.lower() == "no":
            effective_initial_scan = "no"
        elif cursor:
            effective_initial_scan = "no"
        else:
            effective_initial_scan = initial_scan_config
        
        changefeed_options = []
        
        if effective_initial_scan.lower() == "only":
            changefeed_options.append(f"initial_scan='only'")
        elif effective_initial_scan.lower() == "yes":
            changefeed_options.append(f"initial_scan='yes'")
            changefeed_options.append("updated")
            changefeed_options.append(f"resolved='{resolved_interval}'")
        else:
            changefeed_options.append("initial_scan='no'")
            changefeed_options.append("updated")
            changefeed_options.append(f"resolved='{resolved_interval}'")
        
        # Check if table has multiple column families (before creating connection for changefeed)
        # This must be done before _get_connection for changefeed to avoid connection reuse issues
        temp_conn_for_check = self._get_connection(table_options)
        try:
            has_multiple_families = self._has_multiple_column_families(table_name, table_options)
        finally:
            temp_conn_for_check.close()
        
        if has_multiple_families:
            changefeed_options.append("split_column_families")
            print(f"   ✅ Added split_column_families (table has multiple column families)")
        else:
            print(f"   ℹ️  Skipped split_column_families (table has single column family)")
        
        if cursor and effective_initial_scan.lower() != "only":
            changefeed_options.append(f"cursor='{cursor}'")
        
        options_str = ", ".join(changefeed_options)
        target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
        changefeed_query = f"EXPERIMENTAL CHANGEFEED FOR {target} WITH {options_str}"
        
        conn = self._get_connection(table_options)
        
        # Capture current timestamp for cursor tracking
        debug_cursor = self._create_cursor(conn)
        debug_cursor.execute("SELECT cluster_logical_timestamp()::string")
        current_ts = debug_cursor.fetchone()[0]
        debug_cursor.close()
        
        # For snapshot mode: handle multi-table consistency
        if effective_initial_scan.lower() == "only":
            is_multi_table = table_options.get("multi_table_pipeline", "false").lower() == "true"
            
            if is_multi_table and LakeflowConnect._shared_snapshot_timestamp:
                self._snapshot_start_timestamp = LakeflowConnect._shared_snapshot_timestamp
            else:
                self._snapshot_start_timestamp = current_ts
                if is_multi_table:
                    LakeflowConnect._shared_snapshot_timestamp = current_ts
        else:
            # For incremental mode: save timestamp to move cursor forward even if no changes
            self._incremental_run_timestamp = current_ts
        
        def event_generator():
            """Generator that yields changefeed events."""
            query_start = time.time()
            
            # Set timeout
            if cursor and effective_initial_scan.lower() == "no":
                query_timeout = "5s"
            else:
                query_timeout = "600s"
            
            changefeed_cursor = self._create_cursor(conn)
            
            try:
                changefeed_cursor.execute(f"SET statement_timeout = '{query_timeout}'")
            except Exception:
                pass  # Ignore timeout setting errors
            
            try:
                changefeed_cursor.execute(changefeed_query)
            except Exception as e:
                # Check for timeout
                try:
                    import pg8000.dbapi
                    import pg8000.exceptions
                    is_pg8000_error = isinstance(e, (pg8000.dbapi.ProgrammingError, pg8000.exceptions.DatabaseError))
                except ImportError:
                    is_pg8000_error = False
                
                error_msg = str(e).lower()
                error_dict = str(e)
                is_statement_timeout = (
                    'statement timeout' in error_msg or
                    '57014' in error_dict or
                    isinstance(e, TimeoutError) or
                    (is_pg8000_error and '57014' in error_dict)
                )
                
                if is_statement_timeout:
                    try:
                        changefeed_cursor.close()
                    except:
                        pass
                    return cursor
                else:
                    try:
                        changefeed_cursor.close()
                    except:
                        pass
                    raise
            
            event_count = 0
            last_resolved = None
            highest_updated = None
            has_updated = effective_initial_scan.lower() != "only"
            
            try:
                for row in changefeed_cursor:
                    if has_updated:
                        key_json = row[1]
                        value_json = row[2]
                        updated = row[3]
                        
                        if updated:
                            if highest_updated is None or updated > highest_updated:
                                highest_updated = updated
                        
                        if key_json is None and value_json is None:
                            last_resolved = updated
                            if cursor is not None:
                                break
                            continue
                    else:
                        key_json = row[1]
                        value_json = row[2]
                        updated = None
                    
                    def parse_json_column(json_data):
                        if json_data is None:
                            return None
                        if isinstance(json_data, memoryview):
                            json_data = json_data.tobytes().decode('utf-8')
                        elif isinstance(json_data, bytes):
                            json_data = json_data.decode('utf-8')
                        if not isinstance(json_data, str) or json_data.strip() == '':
                            return None
                        try:
                            return json.loads(json_data)
                        except json.JSONDecodeError:
                            return None
                    
                    key = parse_json_column(key_json) or []
                    value = parse_json_column(value_json)
                    
                    transformed_event = self._transform_changefeed_event_native(key, value, updated)
                    yield transformed_event
                    
                    event_count += 1
            except Exception as e:
                try:
                    import pg8000.dbapi
                    import pg8000.exceptions
                    is_pg8000_error = isinstance(e, (pg8000.dbapi.ProgrammingError, pg8000.exceptions.DatabaseError))
                except ImportError:
                    is_pg8000_error = False
                
                error_msg = str(e).lower()
                error_dict = str(e)
                is_timeout = (
                    isinstance(e, TimeoutError) or
                    'timeout' in error_msg or
                    '57014' in error_dict or
                    (is_pg8000_error and '57014' in error_dict)
                )
                
                if not is_timeout:
                    try:
                        changefeed_cursor.close()
                    except:
                        pass
                    raise
                # If timeout, continue (expected behavior for incremental CDC)
            finally:
                try:
                    changefeed_cursor.close()
                except:
                    pass
            
            cursor_to_return = last_resolved or highest_updated
            return cursor_to_return
        
        events = []
        last_resolved = None
        
        try:
            gen = event_generator()
            for event in gen:
                if event is not None:
                    events.append(event)
        except StopIteration as e:
            last_resolved = e.value if hasattr(e, 'value') else None
        finally:
            conn.close()
        
        # Coalesce fragmented events (from split_column_families) into complete rows
        coalesce_enabled = table_options.get("coalesce_split_families", "false").lower() == "true"
        if coalesce_enabled and events:
            events = self._coalesce_events_by_key(events)
        
        end_offset = start_offset.copy() if start_offset else {}
        
        if last_resolved:
            # CDC returned a resolved timestamp (changes detected and processed)
            end_offset["cursor"] = last_resolved
        elif hasattr(self, '_snapshot_start_timestamp') and self._snapshot_start_timestamp:
            # Snapshot mode: use the snapshot start timestamp
            end_offset["cursor"] = self._snapshot_start_timestamp
        elif hasattr(self, '_incremental_run_timestamp') and self._incremental_run_timestamp:
            # Incremental mode with no changes: move cursor forward to current timestamp
            end_offset["cursor"] = self._incremental_run_timestamp
        
        return iter(events), end_offset
    
    def _transform_changefeed_event_native(
        self, key: list, value: Dict, updated: str
    ) -> Dict:
        """Transform CockroachDB changefeed event."""
        # CockroachDB changefeeds return: {"after": {"col1": "val1", ...}}
        # Extract the "after" object which contains the actual column values
        if value and "after" in value:
            result = value["after"].copy()
        else:
            result = value.copy() if value else {}
        
        result["_cdc_key"] = key
        
        if updated is None:
            import time
            result["_cdc_updated"] = str(int(time.time() * 1000000))
        else:
            result["_cdc_updated"] = updated
        
        if value is None:
            result["_cdc_operation"] = "DELETE"
        else:
            result["_cdc_operation"] = "UPSERT"
        
        return result
    
    def _coalesce_events_by_key(self, events: List[Dict]) -> List[Dict]:
        """
        Coalesce fragmented events (from split_column_families) into complete rows.
        
        With split_column_families=true, CockroachDB emits multiple events per row
        (one per column family). This method merges them by primary key using
        last-non-null semantics for each field.
        
        Operation Priority (for same PK):
        1. DELETE operations take absolute priority (latest DELETE wins)
        2. For non-DELETE: latest timestamp determines operation
        3. SNAPSHOT < INSERT < UPDATE for same timestamp (rare edge case)
        
        Args:
            events: List of fragmented changefeed events
            
        Returns:
            List of complete, merged rows
        """
        from collections import defaultdict
        
        # Handle two types of _cdc_key:
        # 1. Simple list of values (from direct mode): ['pk_value']
        # 2. List of (col, val) tuples (from Parquet split_column_families): [('ycsb_key', 'value'), ('field0', 'x')]
        
        # For Parquet format, we need to extract only PK columns (columns present in all events)
        # by finding the intersection of column names
        sample_key = events[0].get("_cdc_key", []) if events else []
        is_tuple_format = sample_key and isinstance(sample_key[0], tuple)
        
        if is_tuple_format:
            # Find columns that appear in ALL events (these are PK columns)
            all_columns = None
            for event in events:
                cdc_key = event.get("_cdc_key", [])
                event_columns = set(col for col, _ in cdc_key)
                if all_columns is None:
                    all_columns = event_columns
                else:
                    all_columns = all_columns.intersection(event_columns)
            
            pk_columns = sorted(all_columns) if all_columns else []
            
            
            # Group by PK columns only
            key_to_events = defaultdict(list)
            for event in events:
                cdc_key = event.get("_cdc_key", [])
                # Extract only PK column values in consistent sorted order (by column name)
                pk_dict = {col: val for col, val in cdc_key if col in pk_columns}
                pk_values = tuple(pk_dict[col] for col in sorted(pk_dict.keys()))
                key_to_events[pk_values].append(event)
        else:
            # Simple list format (direct mode)
            key_to_events = defaultdict(list)
            for event in events:
                key_tuple = tuple(event.get("_cdc_key", []))
                key_to_events[key_tuple].append(event)
        
        # Merge events for each key
        coalesced = []
        for key_tuple, key_events in key_to_events.items():
            # Sort events by timestamp (chronological order)
            # Note: _cdc_updated might be string or numeric, handle both
            sorted_events = sorted(
                key_events, 
                key=lambda e: (float(e.get("_cdc_updated", 0)) if e.get("_cdc_updated") else 0)
            )
            
            # Determine final operation with proper priority
            final_operation = None
            latest_delete_timestamp = None
            latest_non_delete_timestamp = None
            latest_non_delete_op = None
            
            for event in sorted_events:
                op = event.get("_cdc_operation", "").upper()  # Normalize to UPPERCASE
                timestamp = float(event.get("_cdc_updated", 0)) if event.get("_cdc_updated") else 0
                
                if op == "DELETE":
                    # DELETE always wins if it's the latest DELETE
                    if latest_delete_timestamp is None or timestamp >= latest_delete_timestamp:
                        latest_delete_timestamp = timestamp
                        final_operation = "DELETE"
                else:
                    # Track latest non-DELETE operation with priority: UPDATE/INSERT > SNAPSHOT
                    should_update = False
                    
                    if latest_non_delete_timestamp is None:
                        should_update = True
                    elif timestamp > latest_non_delete_timestamp:
                        should_update = True
                    elif timestamp == latest_non_delete_timestamp:
                        # Same timestamp - prioritize by operation type
                        # UPDATE/INSERT > SNAPSHOT (for column family fragments)
                        if op in ('UPDATE', 'INSERT') and latest_non_delete_op in ('SNAPSHOT', None):
                            should_update = True
                        elif op == 'SNAPSHOT' and latest_non_delete_op in ('UPDATE', 'INSERT'):
                            should_update = False  # Keep UPDATE/INSERT, don't replace with SNAPSHOT
                    
                    if should_update:
                        latest_non_delete_timestamp = timestamp
                        latest_non_delete_op = op
            
            # Final operation priority:
            # 1. If we have a DELETE, use it
            # 2. Otherwise use latest non-DELETE operation
            if final_operation != "DELETE" and latest_non_delete_op:
                final_operation = latest_non_delete_op
            
            # Merge column values using last-non-null semantics (chronological order)
            merged = {}
            for event in sorted_events:
                for field, value in event.items():
                    if value is not None:
                        merged[field] = value
            
            # Override operation with our determined final operation
            if final_operation:
                merged["_cdc_operation"] = final_operation
            
            coalesced.append(merged)
        
        # Second pass: De-duplicate by extracting PK-only keys
        # This handles cases where column families create events with same PK but different _cdc_key structures
        if is_tuple_format and pk_columns:
            # Group again by PK values only
            final_dedup = {}
            for event in coalesced:
                cdc_key = event.get("_cdc_key", [])
                # Extract only PK column values in consistent sorted order (by column name)
                pk_dict = {col: val for col, val in cdc_key if col in pk_columns}
                pk_values = tuple(pk_dict[col] for col in sorted(pk_dict.keys()))
                
                if pk_values in final_dedup:
                    # We have a duplicate - merge with priority to DELETE, then operation type, then latest timestamp
                    existing = final_dedup[pk_values]
                    current_op = event.get("_cdc_operation", "").upper()
                    existing_op = existing.get("_cdc_operation", "").upper()
                    current_ts = float(event.get("_cdc_updated", 0)) if event.get("_cdc_updated") else 0
                    existing_ts = float(existing.get("_cdc_updated", 0)) if existing.get("_cdc_updated") else 0
                    
                    # Priority: DELETE > UPDATE/INSERT > SNAPSHOT, then latest timestamp
                    should_replace = False
                    
                    if current_op == "DELETE" and existing_op != "DELETE":
                        should_replace = True
                    elif existing_op != "DELETE" and current_op == "DELETE":
                        should_replace = True
                    elif existing_op != "DELETE" and current_op != "DELETE":
                        # Both are non-DELETE operations
                        if current_ts > existing_ts:
                            should_replace = True
                        elif current_ts == existing_ts:
                            # Same timestamp - prioritize by operation type
                            if current_op in ('UPDATE', 'INSERT') and existing_op == 'SNAPSHOT':
                                should_replace = True
                    
                    if should_replace:
                        final_dedup[pk_values] = event
                    # else: keep existing
                else:
                    final_dedup[pk_values] = event
            
            return list(final_dedup.values())
        else:
            # For non-tuple format, we're already deduplicated by key_tuple
            return coalesced
    
    # ========================================================================
    # Volume Mode Implementation
    # ========================================================================
    
    def _list_volume_files(self, volume_path: str = None, spark = None, dbutils = None, file_extensions: List[str] = None) -> List[Dict[str, Any]]:
        """
        List changefeed files from Unity Catalog Volume.
        
        This method consolidates file listing logic used by multiple entry points.
        Supports both Parquet and JSON formats.
        
        Args:
            volume_path: Path to Unity Catalog Volume (defaults to self.volume_path)
            spark: Optional SparkSession (uses self._spark if not provided, REQUIRED)
            dbutils: Optional DBUtils instance (uses self._dbutils if not provided, REQUIRED)
            file_extensions: List of file extensions to include (default: ['.parquet', '.ndjson', '.json'])
            
        Returns:
            List of file info dictionaries with keys: 'name', 'path', 'size'
            Returns empty list if no files found or on error
            
        Raises:
            RuntimeError: If spark or dbutils not available
            
        Example:
            ```python
            # List all CDC files (Parquet and JSON)
            files = connector._list_volume_files(spark=spark, dbutils=dbutils)
            
            # List only Parquet files
            files = connector._list_volume_files(file_extensions=['.parquet'])
            
            # List only JSON files
            files = connector._list_volume_files(file_extensions=['.ndjson', '.json'])
            ```
        """
        # Default to both Parquet and JSON files
        if file_extensions is None:
            file_extensions = ['.parquet', '.ndjson', '.json']
        
        # Use provided volume_path or instance variable
        effective_path = volume_path or self.volume_path
        if not effective_path:
            return []
        
        # Validate spark and dbutils are available (single source of truth - NO FALLBACKS)
        spark, dbutils = self._ensure_spark_and_dbutils(spark, dbutils)
        
        file_list = []
        
        def list_recursive(path):
            """Recursively list files in directory and subdirectories."""
            files = []
            try:
                items = dbutils.fs.ls(path)
                for item in items:
                    # Skip metadata directories (check path since item.name can be empty)
                    if '/_metadata/' in item.path:
                        continue
                    
                    # Extract directory/file name from path (handle empty item.name)
                    item_name = item.name if item.name else item.path.rstrip('/').split('/')[-1]
                    
                    # Skip items starting with underscore
                    if item_name.startswith('_'):
                        continue
                    
                    # Check if it's a file by extension
                    is_data_file = any(item_name.endswith(ext) for ext in file_extensions)
                    
                    if is_data_file:
                        # Add data file
                        files.append({
                            'name': item_name,
                            'path': item.path,
                            'size': item.size
                        })
                    else:
                        # Assume it's a directory, recurse into it
                        files.extend(list_recursive(item.path))
            except Exception:
                pass  # Directory might not exist or be accessible
            return files
        
        try:
            file_list = list_recursive(effective_path)
            return file_list
            
        except Exception as e:
            # Failed to list files - return empty list
            return []
    
    def _read_table_from_volume(
        self, table_name: str, start_offset: Dict[str, str], table_options: Dict[str, str],
        spark = None, dbutils = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Read table data from Databricks Unity Catalog Volume.
        
        Supports both JSON and Parquet changefeed formats:
        - JSON (.ndjson, .json): Wrapped envelope format (before/after/key)
        - Parquet (.parquet): Native CockroachDB format (__crdb__event_type)
        
        Flow:
        1. List JSON/Parquet files in Volume (using dbutils)
        2. Filter files by cursor (filename-based, has embedded timestamp)
        3. Detect format from file extension
        4. Read each file using appropriate Spark reader (json or parquet)
        5. Transform CDC format (add metadata, classify operations)
        6. Yield rows to caller
        7. Update cursor to latest file processed
        
        Cursor Strategy:
        - Filenames contain timestamp prefix (e.g., 202512191714242809831900000000000-...)
        - Sort files by name (timestamp order)
        - Track last processed filename as cursor
        - Process only files > cursor
        - Mixed JSON/Parquet files are processed in timestamp order
        
        Snapshot vs CDC:
        - Parquet: Uses timestamp comparison against snapshot_cutoff
        - JSON: Uses before/after presence + optional timestamp comparison
        - Both snapshot and CDC events come through same file stream
        
        Args:
            table_name: Name of the table
            start_offset: Offset tracking (cursor)
            table_options: Additional options
            spark: Optional SparkSession (uses self._spark if not provided, REQUIRED)
            dbutils: Optional DBUtils (uses self._dbutils if not provided, REQUIRED)
            
        Raises:
            RuntimeError: If spark or dbutils not available
        """
        # Validate spark and dbutils are available (single source of truth - NO FALLBACKS)
        spark, dbutils = self._ensure_spark_and_dbutils(spark, dbutils)
        
        last_cursor = start_offset.get("cursor", "") if start_offset else ""
        
        # Load schema to get primary keys
        schema_info = self._load_schema_from_volume(self.volume_path, spark=spark, dbutils=dbutils)
        primary_keys = schema_info.get('primary_keys', []) if schema_info else []
        
        # Use shared file listing method (pass spark and dbutils)
        # This will list both JSON and Parquet files
        file_list = self._list_volume_files(self.volume_path, spark=spark, dbutils=dbutils)
        if not file_list:
            return iter([]), start_offset
        
        # Filter out _metadata/ directory contents and apply cursor filter
        new_files = [f for f in file_list if '/_metadata/' not in f['path'] and f['name'] > last_cursor]
        new_files.sort(key=lambda f: f['name'])
        
        if not new_files:
            return iter([]), start_offset
        
        # Read and process each file (JSON or Parquet)
        all_rows = []
        latest_filename = last_cursor
        
        for file_info in new_files:
            filename = file_info['name']
            file_path = file_info['path']
            
            try:
                # Detect format from file extension
                if filename.endswith('.parquet'):
                    # Read Parquet using Spark
                    df = spark.read.parquet(file_path)
                    
                    # Convert to Pandas for row-by-row processing
                    pdf = df.toPandas()
                    records = pdf.to_dict('records')
                    
                    # Process Parquet records
                    snapshot_cutoff = self._snapshot_cutoff_timestamps.get(table_name)
                    transformed_records = self._process_parquet_records(
                        records,
                        source_file=filename,
                        primary_key_columns=primary_keys,
                        fallback_timestamp=None,
                        snapshot_cutoff=snapshot_cutoff
                    )
                    
                elif filename.endswith(('.ndjson', '.json')):
                    # Read JSON using Spark
                    df = spark.read.json(file_path)
                    
                    # Convert to Pandas for row-by-row processing
                    pdf = df.toPandas()
                    records = pdf.to_dict('records')
                    
                    # Process JSON envelope records
                    snapshot_cutoff = self._snapshot_cutoff_timestamps.get(table_name)
                    transformed_records = self._process_json_records(
                        records,
                        source_file=filename,
                        primary_key_columns=primary_keys,
                        fallback_timestamp=None,
                        snapshot_cutoff=snapshot_cutoff
                    )
                    
                else:
                    # Skip unknown file formats
                    continue
                
                all_rows.extend(transformed_records)
                
            except Exception as e:
                # Log error and continue to next file
                print(f"⚠️  Error processing {filename}: {e}")
                continue
            
            # Update cursor to this file
            latest_filename = filename
        
        # ====================================================================
        # Apply CDC deduplication (keep latest state per key, like Autoloader)
        # ====================================================================
        if all_rows and primary_keys:
            try:
                # Convert to pandas DataFrame for merge/dedup operations
                import pandas as pd
                pdf = pd.DataFrame(all_rows)
                
                # Convert to Spark for proper merge handling
                df_raw = spark.createDataFrame(pdf)
                
                # Merge column family fragments (if table uses split column families)
                df_merged = merge_column_family_fragments(
                    df_raw,
                    primary_key_columns=primary_keys,
                    debug=False  # Set to True for debugging
                )
                
                # Deduplicate: Keep only LATEST state per primary key
                # This matches Autoloader's behavior in load_and_merge_cdc_to_delta
                from pyspark.sql import Window
                from pyspark.sql.functions import row_number, col
                
                # Determine which timestamp column to use
                timestamp_col = None
                if '_cdc_timestamp' in df_merged.columns:
                    timestamp_col = '_cdc_timestamp'
                elif '_cdc_updated' in df_merged.columns:
                    timestamp_col = '_cdc_updated'
                elif '__crdb__updated' in df_merged.columns:
                    timestamp_col = '__crdb__updated'
                elif 'updated' in df_merged.columns:
                    timestamp_col = 'updated'
                
                if timestamp_col:
                    # Deduplicate by PK only, keeping latest by timestamp
                    window_spec = Window.partitionBy(*primary_keys).orderBy(col(timestamp_col).desc())
                    df_deduped = df_merged.withColumn("_row_num", row_number().over(window_spec)) \
                                          .filter("_row_num == 1") \
                                          .drop("_row_num")
                    
                    # Filter out DELETE operations (Autoloader does this via left_anti join)
                    # DELETEs are applied by removing those keys, not by keeping DELETE rows
                    df_final = df_deduped.filter("_cdc_operation != 'DELETE'")
                    
                    # Convert back to list of dicts
                    pdf_final = df_final.toPandas()
                    all_rows = pdf_final.to_dict('records')
            except Exception as e:
                # If deduplication fails, return raw data (better than failing completely)
                print(f"⚠️  Deduplication failed: {e}")
                print(f"   Returning raw data without deduplication")
        
        # Return data + updated offset
        end_offset = {"cursor": latest_filename}
        return iter(all_rows), end_offset
    
    # ========================================================================
    # Common Parquet Processing Methods
    # ========================================================================
    
    def _determine_cdc_operation(self, event_type: str, event_timestamp: str = None, snapshot_cutoff: str = None) -> str:
        """
        Map CockroachDB event type to CDC operation.
        
        NOTE: For Parquet format, __crdb__event_type is 'c' for both snapshots and 
        updates (indistinguishable by type alone). Uses timestamp-based logic to distinguish:
        - Events with timestamp <= snapshot_cutoff = SNAPSHOT
        - Events with timestamp > snapshot_cutoff = UPDATE
        
        Args:
            event_type: CockroachDB event type ('c', 'i', 'd')
                       Note: 'u' is included for compatibility but does not exist in Parquet format
            event_timestamp: Optional timestamp from __crdb__updated field
            snapshot_cutoff: Optional cutoff timestamp (initial scan completion time)
        
        Returns:
            CDC operation ('SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE', 'UNKNOWN')
        """
        if event_type == 'c':
            # For 'c' events, use timestamp to distinguish snapshot from update
            if event_timestamp and snapshot_cutoff:
                try:
                    # Compare timestamps as strings (they're in sortable format)
                    if event_timestamp > snapshot_cutoff:
                        return 'UPDATE'
                except:
                    pass
            # Default to SNAPSHOT if no timestamp logic available
            return 'SNAPSHOT'
        elif event_type == 'i':
            return 'INSERT'
        elif event_type == 'u':
            return 'UPDATE'
        elif event_type == 'd':
            return 'DELETE'
        else:
            return 'UNKNOWN'
    
    def _add_cdc_metadata_to_dataframe(self, df, table_name: str = None, source_file: str = None, primary_key_columns: List[str] = None):
        """
        Add CDC metadata columns to DataFrame.
        
        Works for BOTH batch and streaming DataFrames!
        Consolidates CDC transformation logic used by multiple entry points.
        
        Args:
            df: PySpark DataFrame (batch or streaming)
            table_name: Optional table name for snapshot cutoff lookup
            source_file: Optional source file path (for batch mode with literal file name)
            primary_key_columns: Optional list of primary key column names (required for JSON format to extract keys from 'key' array)
            
        Returns:
            DataFrame with CDC metadata columns added:
            - _cdc_operation: CDC operation type (SNAPSHOT, UPDATE, UPSERT, DELETE)
            - _cdc_timestamp: CDC timestamp (from __crdb__updated)
            - _source_file: Source file path
            - _processing_time: Timestamp when processed
            
        Example (Batch):
            ```python
            df_raw = spark.read.parquet("dbfs:/Volumes/...")
            df_enriched = connector._add_cdc_metadata_to_dataframe(
                df_raw,
                table_name="usertable",
                source_file="file.parquet"
            )
            ```
            
        Example (Streaming):
            ```python
            df_raw = spark.readStream.format("cloudFiles").load(...)
            df_enriched = connector._add_cdc_metadata_to_dataframe(
                df_raw,
                table_name="usertable"
            )
            ```
        """
        from pyspark.sql import functions as F
        
        # Get snapshot cutoff for this table (if available)
        snapshot_cutoff = self._snapshot_cutoff_timestamps.get(table_name) if table_name else None
        
        # ============================================================================
        # ⚠️  CDC OPERATION DETECTION LOGIC - PRODUCTION PATH (Spark Streaming)
        # ============================================================================
        # This logic is DUPLICATED in analyze_azure_changefeed_files() for testing.
        # If you change this logic, you MUST update the corresponding code at:
        #   - Line ~3268: JSON format detection in analyze_azure_changefeed_files()
        #   - Line ~3094: Parquet format detection in analyze_azure_changefeed_files()
        #
        # Core Rules (must be consistent across both paths):
        #   - Parquet 'c' + timestamp <= cutoff → SNAPSHOT
        #   - Parquet 'c' + timestamp > cutoff  → UPDATE
        #   - JSON (after && !before) + timestamp <= cutoff → SNAPSHOT
        #   - JSON (after && !before) + timestamp > cutoff  → INSERT
        #   - 'i' event type → INSERT
        #   - 'd' event type → DELETE
        # ============================================================================
        
        # Detect format: JSON has 'before'/'after' columns, Parquet has '__crdb__event_type'
        schema_columns = df.columns
        is_json_format = 'before' in schema_columns and 'after' in schema_columns
        is_parquet_format = '__crdb__event_type' in schema_columns
        
        if is_json_format:
            # JSON FORMAT: Use before/after envelope
            # Flatten the 'after' struct to top level for easier access
            if 'after' in schema_columns:
                # Get all fields from 'after' struct
                after_fields = df.schema['after'].dataType.fieldNames() if hasattr(df.schema['after'].dataType, 'fieldNames') else []
                for field in after_fields:
                    df = df.withColumn(field, F.col(f"after.{field}"))
            
            # CRITICAL: Extract primary key from 'key' array for JSON format
            # The 'key' column is an array like [1234] or [1234, 5678] for composite keys
            # We need to extract these values into the actual PK column names
            if primary_key_columns and 'key' in schema_columns:
                for i, pk_col in enumerate(primary_key_columns):
                    df = df.withColumn(pk_col, F.col("key").getItem(i))
            
            # Add CDC operation based on before/after + timestamp
            if snapshot_cutoff:
                # With snapshot cutoff: distinguish SNAPSHOT from INSERT
                # CRITICAL: Compare as numbers (double), not strings
                # String comparison fails when formats differ (scientific notation, precision)
                # Cast both sides to double for numeric comparison
                try:
                    snapshot_cutoff_double = float(snapshot_cutoff)
                except (ValueError, TypeError):
                    snapshot_cutoff_double = None
                
                if snapshot_cutoff_double is not None:
                    # CRITICAL: In JSON, DELETE events have before=null but after could be null OR {}
                    # Need to check both isNull() AND if the struct has any non-null fields
                    # 
                    # CockroachDB JSON format:
                    # - SNAPSHOT/INSERT: after={...}, before=null
                    # - UPDATE: after={...}, before={...}
                    # - DELETE: after=null (or {}), before={...}
                    #
                    # Problem: Spark's isNull() returns FALSE for empty struct {}
                    # Solution: Convert struct to JSON string and check if it's "null" or "{}"
                    
                    # Add helper columns to detect empty/null structs reliably
                    df = df.withColumn("_after_json", F.to_json(F.col("after")))
                    df = df.withColumn("_before_json", F.to_json(F.col("before")))
                    
                    # DEBUG: Add columns that will persist to Delta for investigation
                    # These will help us see what's actually being compared
                    df = df.withColumn("_debug_after_first_10", F.substring(F.col("_after_json"), 1, 10))
                    df = df.withColumn("_debug_before_first_10", F.substring(F.col("_before_json"), 1, 10))
                    
                    # Check if after/before are "empty" (null or {})
                    # CRITICAL: F.to_json() can return EITHER:
                    #   1. SQL NULL (when input column is NULL)
                    #   2. String "null" (when input is null struct but not NULL column)
                    #   3. String "{}" (when input is empty struct)
                    # We need to check for ALL three cases!
                    after_empty = F.col("_after_json").isNull() | (F.col("_after_json") == F.lit("null")) | (F.col("_after_json") == F.lit("{}"))
                    after_not_empty = ~after_empty
                    before_empty = F.col("_before_json").isNull() | (F.col("_before_json") == F.lit("null")) | (F.col("_before_json") == F.lit("{}"))
                    before_not_empty = ~before_empty
                    
                    df = df.withColumn("_cdc_operation",
                        F.when(
                            after_not_empty & before_empty & 
                            (F.col("updated").cast("double") <= F.lit(snapshot_cutoff_double)),
                            F.lit("SNAPSHOT")
                        )
                        .when(
                            after_not_empty & before_empty & 
                            (F.col("updated").cast("double") > F.lit(snapshot_cutoff_double)),
                            F.lit("INSERT")
                        )
                        .when(after_not_empty & before_not_empty, F.lit("UPDATE"))
                        .when(after_empty & before_not_empty, F.lit("DELETE"))
                        .otherwise(F.lit("UNKNOWN"))
                    )
                    
                    # Keep helper columns for debugging (will be dropped later if needed)
                    # df = df.drop("_after_json", "_before_json")  # Keep for now to debug
                else:
                    # Fallback: treat all 'after only' as SNAPSHOT if cutoff parsing failed
                    # Still need to check for empty structs
                    df = df.withColumn("_after_json", F.to_json(F.col("after")))
                    df = df.withColumn("_before_json", F.to_json(F.col("before")))
                    
                    after_empty = F.col("_after_json").isNull() | (F.col("_after_json") == F.lit("null")) | (F.col("_after_json") == F.lit("{}"))
                    after_not_empty = ~after_empty
                    before_empty = F.col("_before_json").isNull() | (F.col("_before_json") == F.lit("null")) | (F.col("_before_json") == F.lit("{}"))
                    before_not_empty = ~before_empty
                    
                    df = df.withColumn("_cdc_operation",
                        F.when(after_not_empty & before_empty, F.lit("SNAPSHOT"))
                        .when(after_not_empty & before_not_empty, F.lit("UPDATE"))
                        .when(after_empty & before_not_empty, F.lit("DELETE"))
                        .otherwise(F.lit("UNKNOWN"))
                    )
                    
                    df = df.drop("_after_json", "_before_json")
            else:
                # Without cutoff: treat all 'after only' as SNAPSHOT
                # Still need to check for empty structs
                df = df.withColumn("_after_json", F.to_json(F.col("after")))
                df = df.withColumn("_before_json", F.to_json(F.col("before")))
                
                after_empty = F.col("_after_json").isNull() | (F.col("_after_json") == F.lit("null")) | (F.col("_after_json") == F.lit("{}"))
                after_not_empty = ~after_empty
                before_empty = F.col("_before_json").isNull() | (F.col("_before_json") == F.lit("null")) | (F.col("_before_json") == F.lit("{}"))
                before_not_empty = ~before_empty
                
                df = df.withColumn("_cdc_operation",
                    F.when(after_not_empty & before_empty, F.lit("SNAPSHOT"))
                    .when(after_not_empty & before_not_empty, F.lit("UPDATE"))
                    .when(after_empty & before_not_empty, F.lit("DELETE"))
                    .otherwise(F.lit("UNKNOWN"))
                )
                
                df = df.drop("_after_json", "_before_json")
            
            # Add CDC timestamp (JSON uses 'updated' not '__crdb__updated')
            df = df.withColumn("_cdc_timestamp", F.col("updated").cast("string"))
            
        elif is_parquet_format:
            # PARQUET FORMAT: Use __crdb__event_type
            # Add CDC operation based on event type + timestamp
            if snapshot_cutoff:
                # With snapshot cutoff: distinguish SNAPSHOT from UPDATE
                df = df.withColumn("_cdc_operation",
                    F.when(
                        (F.col("__crdb__event_type") == "c") & 
                        (F.col("__crdb__updated") <= F.lit(snapshot_cutoff)),
                        F.lit("SNAPSHOT")
                    )
                    .when(
                        (F.col("__crdb__event_type") == "c") & 
                        (F.col("__crdb__updated") > F.lit(snapshot_cutoff)),
                        F.lit("UPDATE")
                    )
                    .when(F.col("__crdb__event_type") == "i", F.lit("INSERT"))
                    .when(F.col("__crdb__event_type") == "d", F.lit("DELETE"))
                    .otherwise(F.lit("UNKNOWN"))
                )
            else:
                # Without cutoff: treat all 'c' as UPSERT
                df = df.withColumn("_cdc_operation",
                    F.when(F.col("__crdb__event_type") == "c", F.lit("UPSERT"))
                    .when(F.col("__crdb__event_type") == "i", F.lit("INSERT"))
                    .when(F.col("__crdb__event_type") == "d", F.lit("DELETE"))
                    .otherwise(F.lit("UNKNOWN"))
                )
            
            # Add CDC timestamp
            df = df.withColumn("_cdc_timestamp", F.col("__crdb__updated").cast("string"))
        else:
            raise ValueError(
                f"Unknown changefeed format. Schema has columns: {schema_columns}\n"
                f"Expected either:\n"
                f"  - JSON format: 'before', 'after', 'updated' columns\n"
                f"  - Parquet format: '__crdb__event_type', '__crdb__updated' columns"
            )
        
        # Add source file (different for batch vs streaming)
        if source_file:
            # Batch mode: use literal source file name
            df = df.withColumn("_source_file", F.lit(source_file))
        else:
            # Streaming mode: use _metadata.file_path if available
            # Note: _metadata is available in cloudFiles/Autoloader
            try:
                df = df.withColumn("_source_file", F.col("_metadata.file_path"))
            except:
                # Fallback: no source file column
                df = df.withColumn("_source_file", F.lit(None))
        
        # Add processing timestamp
        df = df.withColumn("_processing_time", F.current_timestamp())
        
        return df
    
    def _process_parquet_records(
        self, 
        records: List[Dict[str, Any]], 
        source_file: str,
        primary_key_columns: List[str] = None,
        fallback_timestamp: str = None,
        snapshot_cutoff: str = None
    ) -> List[Dict[str, Any]]:
        """
        Process Parquet records and transform to CDC format.
        
        Handles two CockroachDB Parquet formats:
        1. Native format: __crdb__event_type column with data at top level
        2. Wrapped format: 'before'/'after' columns (less common for Parquet)
        
        Args:
            records: List of records from Parquet file
            source_file: Source filename for tracking
            primary_key_columns: Optional list of primary key column names
                               If provided, only these columns are used for CDC key extraction
                               If not provided, all data columns are used (backward compatible)
            fallback_timestamp: Fallback timestamp if not in record
            snapshot_cutoff: Optional cutoff timestamp to distinguish snapshots from updates
                           Events with timestamp > cutoff are considered UPDATEs
        
        Returns:
            List of transformed records with CDC metadata
        """
        transformed_records = []
        
        for record in records:
            # Check for native CockroachDB Parquet format (with __crdb__event_type)
            if '__crdb__event_type' in record:
                # Native format: data columns at top level, event type indicator
                event_type = record.get('__crdb__event_type', '')
                event_timestamp = record.get('__crdb__updated', record.get('updated'))
                cdc_operation = self._determine_cdc_operation(event_type, event_timestamp, snapshot_cutoff)
                
                # Extract CDC key using primary key columns
                if not primary_key_columns:
                    raise ValueError(
                        f"Primary key columns required for CDC processing. "
                        f"Ensure schema file exists or provide primary_key_columns explicitly."
                    )
                
                cdc_key_pairs = []
                for pk_col in sorted(primary_key_columns):
                    if pk_col in record:
                        cdc_key_pairs.append((pk_col, record[pk_col]))
                
                # Build transformed record
                transformed = {
                    **record,  # All columns from Parquet (including data)
                    '_cdc_key': cdc_key_pairs,  # List of (col, val) tuples
                    '_cdc_updated': record.get('__crdb__updated', record.get('updated', fallback_timestamp)),
                    '_cdc_operation': cdc_operation,
                    '_source_file': source_file
                }
                
            else:
                # Wrapped format (before/after columns) - less common for Parquet
                after_data = record.get('after', {})
                before_data = record.get('before', {})
                
                # Determine operation
                has_after = after_data is not None and (not isinstance(after_data, dict) or len(after_data) > 0)
                has_before = before_data is not None and (not isinstance(before_data, dict) or len(before_data) > 0)
                
                if has_after and not has_before:
                    cdc_operation = 'SNAPSHOT'
                    row_data = after_data if isinstance(after_data, dict) else {}
                elif has_after and has_before:
                    cdc_operation = 'UPDATE'
                    row_data = after_data if isinstance(after_data, dict) else {}
                elif has_before and not has_after:
                    cdc_operation = 'DELETE'
                    row_data = before_data if isinstance(before_data, dict) else {}
                else:
                    cdc_operation = 'UNKNOWN'
                    row_data = {}
                
                # Build transformed record
                transformed = {
                    **row_data,
                    '_cdc_key': record.get('key', []),
                    '_cdc_updated': record.get('updated', fallback_timestamp),
                    '_cdc_operation': cdc_operation,
                    '_source_file': source_file
                }
            
            transformed_records.append(transformed)
        
        return transformed_records
    
    def _process_json_records(
        self,
        records: List[Dict[str, Any]],
        source_file: str,
        primary_key_columns: List[str] = None,
        fallback_timestamp: str = None,
        snapshot_cutoff: str = None
    ) -> List[Dict[str, Any]]:
        """
        Process JSON envelope records and transform to CDC format.
        
        Handles CockroachDB JSON changefeed format:
        - 'after' only = snapshot or insert
        - 'after' + 'before' = update
        - 'before' only = delete
        - 'key' array contains primary key values
        
        Args:
            records: List of records from JSON file
            source_file: Source filename for tracking
            primary_key_columns: Optional list of primary key column names
            fallback_timestamp: Fallback timestamp if not in record
            snapshot_cutoff: Optional cutoff timestamp to distinguish snapshots from updates
                           Events with timestamp > cutoff are considered UPDATEs/INSERTs
        
        Returns:
            List of transformed records with CDC metadata
        """
        transformed_records = []
        
        for record in records:
            after_data = record.get('after', {})
            before_data = record.get('before', {})
            key_array = record.get('key', [])
            updated_timestamp = record.get('updated', fallback_timestamp)
            
            # Determine operation based on before/after presence
            has_after = after_data is not None and (not isinstance(after_data, dict) or len(after_data) > 0)
            has_before = before_data is not None and (not isinstance(before_data, dict) or len(before_data) > 0)
            
            # Classify operation
            if has_after and not has_before:
                # Only 'after' present - could be SNAPSHOT or INSERT
                if snapshot_cutoff and updated_timestamp:
                    # Use timestamp to distinguish
                    if updated_timestamp <= snapshot_cutoff:
                        cdc_operation = 'SNAPSHOT'
                    else:
                        cdc_operation = 'INSERT'
                else:
                    # No cutoff - treat as SNAPSHOT (conservative)
                    cdc_operation = 'SNAPSHOT'
                row_data = after_data if isinstance(after_data, dict) else {}
                
            elif has_after and has_before:
                cdc_operation = 'UPDATE'
                row_data = after_data if isinstance(after_data, dict) else {}
                
            elif has_before and not has_after:
                cdc_operation = 'DELETE'
                row_data = before_data if isinstance(before_data, dict) else {}
                
            else:
                cdc_operation = 'UNKNOWN'
                row_data = {}
            
            # Extract primary key values from 'key' array
            cdc_key_pairs = []
            if primary_key_columns and key_array:
                for i, pk_col in enumerate(primary_key_columns):
                    if i < len(key_array):
                        cdc_key_pairs.append((pk_col, key_array[i]))
            
            # Build transformed record with data columns + metadata
            transformed = {
                **row_data,  # Data columns from 'after' or 'before'
                '_cdc_key': cdc_key_pairs,  # List of (col, val) tuples
                '_cdc_updated': updated_timestamp,
                '_cdc_operation': cdc_operation,
                '_source_file': source_file
            }
            
            # Also add primary key values as top-level columns for easier access
            if primary_key_columns and key_array:
                for i, pk_col in enumerate(primary_key_columns):
                    if i < len(key_array) and pk_col not in transformed:
                        transformed[pk_col] = key_array[i]
            
            transformed_records.append(transformed)
        
        return transformed_records
    
    # ========================================================================
    # Azure Parquet Mode Implementation
    # ========================================================================
    
    def _read_table_from_azure_parquet(
        self, table_name: str, start_offset: Dict[str, str], table_options: Dict[str, str]
    ) -> Iterator[Dict[str, Any]]:
        """
        Read table data from Azure Parquet files.
        
        Flow:
        1. Ensure changefeed exists to Azure (create if needed)
        2. List Parquet files in Azure container
        3. Filter files by cursor (only new files)
        4. Download and read each Parquet file
        5. Yield rows to Spark
        6. Update cursor to latest file timestamp
        """
        self._ensure_azure_dependencies()
        changefeed_job_id, is_newly_created = self._ensure_azure_changefeed(table_name, table_options)
        
        # Load schema to get primary keys
        schema_info = self._load_schema_from_azure(table_name, 'parquet')
        primary_keys = schema_info.get('primary_keys', []) if schema_info else []
        
        # If changefeed was newly created, capture snapshot cutoff timestamp
        # This allows us to distinguish snapshot events from later CDC updates in Parquet format
        if is_newly_created and table_name not in self._snapshot_cutoff_timestamps:
            conn = self._get_connection(table_options)
            try:
                cursor = self._create_cursor(conn)
                cursor.execute("SELECT cluster_logical_timestamp()::string")
                current_ts = cursor.fetchone()[0]
                self._snapshot_cutoff_timestamps[table_name] = current_ts
                cursor.close()
                conn.close()
            except:
                try:
                    conn.close()
                except:
                    pass
        
        from azure.storage.blob import BlobServiceClient
        import time
        
        blob_service_client = BlobServiceClient(
            account_url=f"https://{self.azure_account_name}.blob.core.windows.net",
            credential=self.azure_account_key
        )
        container_client = blob_service_client.get_container_client(self.azure_container)
        
        prefix = f"{self.azure_path_prefix}/"
        last_cursor = start_offset.get("cursor") if start_offset else None
        
        # If changefeed was just created, wait for snapshot files to appear
        if is_newly_created:
            max_wait_time = 90
            check_interval = 10
            elapsed = 0
            
            while elapsed < max_wait_time:
                parquet_files = self._list_azure_parquet_files(
                    container_client, prefix, last_cursor, table_name
                )
                
                if parquet_files:
                    break
                
                time.sleep(check_interval)
                elapsed += check_interval
            
            if not parquet_files:
                return iter([]), start_offset
        else:
            parquet_files = self._list_azure_parquet_files(
                container_client, prefix, last_cursor, table_name
            )
        
        if not parquet_files:
            return iter([]), start_offset
        
        import pyarrow.parquet as pq
        import io
        
        all_rows = []
        latest_timestamp = last_cursor
        
        for file_info in parquet_files:
            blob_name = file_info['name']
            timestamp = file_info['timestamp']
            
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            
            try:
                parquet_table = pq.read_table(io.BytesIO(blob_data))
                records = parquet_table.to_pandas().to_dict('records')
                
                snapshot_cutoff = self._snapshot_cutoff_timestamps.get(table_name)
                transformed_records = self._process_parquet_records(
                    records, 
                    source_file=blob_name,
                    primary_key_columns=primary_keys,
                    fallback_timestamp=timestamp,
                    snapshot_cutoff=snapshot_cutoff
                )
                
                all_rows.extend(transformed_records)
                
            except Exception:
                continue
            
            # Update cursor to highest timestamp
            if not latest_timestamp or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        
        # Return data + updated offset
        end_offset = {"cursor": latest_timestamp} if latest_timestamp else start_offset
        return iter(all_rows), end_offset
    
    def _ensure_azure_changefeed(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, bool]:
        """
        Ensure changefeed(s) exist that write to Azure.
        
        Based on self.format, creates:
        - 'parquet': Single Parquet changefeed
        - 'json': Single JSON changefeed
        - 'both': Two changefeeds (JSON + Parquet)
        
        Returns: (changefeed_job_id(s), is_newly_created)
        """
        if self.format == 'both':
            # Create both JSON and Parquet changefeeds
            json_result = self._create_json_changefeed(table_name, table_options)
            parquet_result = self._create_parquet_changefeed(table_name, table_options)
            return (f"{json_result[0]},{parquet_result[0]}", json_result[1] or parquet_result[1])
        elif self.format == 'json':
            return self._create_json_changefeed(table_name, table_options)
        else:  # parquet
            return self._create_parquet_changefeed(table_name, table_options)
    
    def _create_parquet_changefeed(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, bool]:
        """Create a Parquet-format changefeed."""
        conn = self._get_connection(table_options)
        cursor = self._create_cursor(conn)
        
        try:
            # Check if Parquet changefeed already exists
            fq_table = self._get_fully_qualified_table(table_name)
            existing_job = self._find_existing_changefeed(cursor, table_name, 'parquet')
            if existing_job:
                cursor.close()
                conn.close()
                return (str(existing_job), False)
            
            from urllib.parse import quote
            encoded_key = quote(self.azure_account_key, safe='')
            
            azure_uri = (
                f"azure://{self.azure_container}/{self.parquet_path_prefix}/"
                f"?AZURE_ACCOUNT_NAME={self.azure_account_name}"
                f"&AZURE_ACCOUNT_KEY={encoded_key}"
            )
            
            # Get options
            initial_scan = table_options.get("initial_scan", "yes")
            
            # Check if table has multiple column families
            has_multiple_families = self._has_multiple_column_families(table_name, table_options)
            
            # Build changefeed options
            cf_options = []
            cf_options.append("format = 'parquet'")
            cf_options.append("compression = 'gzip'")
            
            # Add CDC options only if not snapshot-only
            if initial_scan != 'only':
                cf_options.append("updated")
                cf_options.append("resolved = '10s'")
            
            if has_multiple_families:
                cf_options.append("split_column_families")
            
            cf_options.append(f"initial_scan = '{initial_scan}'")
            
            options_str = ",\n                  ".join(cf_options)
            
            changefeed_sql = f"""
                CREATE CHANGEFEED FOR TABLE {fq_table}
                INTO '{azure_uri}'
                WITH 
                  {options_str}
            """
            
            cursor.execute(changefeed_sql)
            result = cursor.fetchone()
            job_id = str(result[0]) if result else None
            
            cursor.close()
            conn.close()
            
            # Dump and store schema alongside data
            try:
                schema_info = self._dump_table_schema(table_name, table_options)
                self._store_schema_to_azure(table_name, schema_info, 'parquet')
            except Exception as e:
                # Schema dump is best-effort - don't fail changefeed creation
                import warnings
                warnings.warn(f"Failed to dump schema for {table_name}: {e}")
            
            return (job_id, True)
            
        except Exception as e:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
            raise
    
    def _create_json_changefeed(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, bool]:
        """Create a JSON-format changefeed."""
        conn = self._get_connection(table_options)
        cursor = self._create_cursor(conn)
        
        try:
            # Check if JSON changefeed already exists
            fq_table = self._get_fully_qualified_table(table_name)
            existing_job = self._find_existing_changefeed(cursor, table_name, 'json')
            if existing_job:
                cursor.close()
                conn.close()
                return (str(existing_job), False)
            
            from urllib.parse import quote
            encoded_key = quote(self.azure_account_key, safe='')
            
            azure_uri = (
                f"azure://{self.azure_container}/{self.json_path_prefix}/"
                f"?AZURE_ACCOUNT_NAME={self.azure_account_name}"
                f"&AZURE_ACCOUNT_KEY={encoded_key}"
            )
            
            # Get options
            initial_scan = table_options.get("initial_scan", "yes")
            
            # Build changefeed options
            cf_options = []
            cf_options.append("format = 'json'")
            cf_options.append("envelope = 'wrapped'")
            
            # Add CDC options only if not snapshot-only
            if initial_scan != 'only':
                cf_options.append("diff")
                cf_options.append("updated")
                cf_options.append("resolved = '10s'")
            
            cf_options.append(f"initial_scan = '{initial_scan}'")
            
            options_str = ",\n                  ".join(cf_options)
            
            changefeed_sql = f"""
                CREATE CHANGEFEED FOR TABLE {fq_table}
                INTO '{azure_uri}'
                WITH 
                  {options_str}
            """
            
            cursor.execute(changefeed_sql)
            result = cursor.fetchone()
            job_id = str(result[0]) if result else None
            
            cursor.close()
            conn.close()
            
            # Dump and store schema alongside data
            try:
                schema_info = self._dump_table_schema(table_name, table_options)
                self._store_schema_to_azure(table_name, schema_info, 'json')
            except Exception as e:
                # Schema dump is best-effort - don't fail changefeed creation
                import warnings
                warnings.warn(f"Failed to dump schema for {table_name}: {e}")
            
            return (job_id, True)
            
        except Exception as e:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
            raise
    
    def _find_existing_changefeed(self, cursor, table_name: str, format_type: str) -> str:
        """Find existing changefeed for table and format."""
        cursor.execute("""
            SELECT job_id, status, description
            FROM [SHOW JOBS] 
            WHERE job_type = 'CHANGEFEED' 
              AND status IN ('running', 'pending')
            ORDER BY created DESC
            LIMIT 50
        """)
        
        jobs = cursor.fetchall()
        format_prefix = f"{format_type}/{self.catalog}/{self.schema}"
        
        for job in jobs:
            job_id, status, description = job
            desc_str = str(description)
            if (str(table_name) in desc_str and 
                'azure://' in desc_str.lower() and
                format_prefix in desc_str):
                return str(job_id)
        
        return None
    
    def _categorize_changefeed_error(self, error_msg: str, running_status: str = None) -> Dict[str, Any]:
        """
        Categorize changefeed errors into common types for better diagnostics.
        
        Args:
            error_msg: Error message from changefeed job
            running_status: Running status message from changefeed job
            
        Returns:
            Dictionary with categorized error information:
            {
                'category': str,        # 'auth', 'storage', 'network', 'config', 'unknown'
                'is_retryable': bool,  # Whether error is likely transient
                'description': str,     # Human-readable description
                'suggestion': str       # Suggested fix
            }
        """
        combined_msg = f"{error_msg or ''} {running_status or ''}".lower()
        
        # Azure/Storage authentication errors
        if any(x in combined_msg for x in ['accountnotfound', 'account not found', 'authorizationpermissionmismatch', 
                                             'invalidauthenticationinfo', 'forbidden', 'authorization failed']):
            return {
                'category': 'auth',
                'is_retryable': False,
                'description': 'Azure storage authentication failed',
                'suggestion': 'Check Azure storage account name and access key. Verify the storage account exists.'
            }
        
        # Storage container/resource not found
        if any(x in combined_msg for x in ['containernotfound', 'container not found', 'blobnotfound', 
                                             'resourcenotfound', 'resource not found', 'the specified container does not exist']):
            return {
                'category': 'storage',
                'is_retryable': False,
                'description': 'Azure storage container or resource not found',
                'suggestion': 'Verify the Azure container exists. It may have been deleted.'
            }
        
        # Network/connectivity errors
        if any(x in combined_msg for x in ['connection refused', 'connection reset', 'timeout', 'unreachable',
                                             'dial tcp', 'connection timed out', 'no such host']):
            return {
                'category': 'network',
                'is_retryable': True,
                'description': 'Network connectivity issue',
                'suggestion': 'Check network connectivity to Azure. May be transient - retry may succeed.'
            }
        
        # Configuration errors
        if any(x in combined_msg for x in ['invalid uri', 'malformed', 'invalid option', 'unknown option',
                                             'invalid format', 'invalid parameter']):
            return {
                'category': 'config',
                'is_retryable': False,
                'description': 'Changefeed configuration error',
                'suggestion': 'Check changefeed URI and options. Verify format, envelope, and other settings.'
            }
        
        # Permission errors
        if any(x in combined_msg for x in ['permission denied', 'access denied', 'insufficient privileges',
                                             'permissiondenied']):
            return {
                'category': 'permissions',
                'is_retryable': False,
                'description': 'Insufficient permissions',
                'suggestion': 'Verify Azure storage account has correct permissions for changefeed operations.'
            }
        
        # Rate limiting / throttling
        if any(x in combined_msg for x in ['rate limit', 'throttl', 'too many requests', '429']):
            return {
                'category': 'rate_limit',
                'is_retryable': True,
                'description': 'Azure storage rate limit exceeded',
                'suggestion': 'Reduce changefeed write rate or wait for rate limit to reset.'
            }
        
        # Unknown error
        return {
            'category': 'unknown',
            'is_retryable': False,
            'description': 'Unknown error',
            'suggestion': 'Review full error message for details.'
        }
    
    def check_changefeed_status(self, job_id: int, table_options: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Check the status of a changefeed job with enhanced error diagnostics.
        
        Args:
            job_id: The changefeed job ID to check
            table_options: Optional connection parameters
        
        Returns:
            Dictionary with status information:
            {
                'job_id': int,
                'status': str,              # 'running', 'paused', 'failed', etc.
                'running_status': str,      # Detailed running status
                'error': str,               # Error message if any
                'is_healthy': bool,         # True if running without errors
                'has_errors': bool,         # True if errors detected
                'error_category': str,      # 'auth', 'storage', 'network', 'config', 'unknown'
                'error_suggestion': str,    # Suggested fix for the error
                'is_retryable': bool        # Whether error is likely transient
            }
        
        Raises:
            ValueError: If job_id not found
        """
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            cursor.execute(f"""
                SELECT status, running_status, error
                FROM [SHOW JOBS]
                WHERE job_id = {job_id}
            """)
            
            result = cursor.fetchone()
            
            if not result:
                cursor.close()
                conn.close()
                raise ValueError(f"Changefeed job {job_id} not found")
            
            status, running_status, error = result
            
            # Check for errors in running_status
            has_errors = False
            if running_status:
                has_errors = 'error' in str(running_status).lower()
            
            # Check for errors in error field
            if error:
                has_errors = True
            
            is_healthy = (status in ('running', 'pending')) and not has_errors
            
            # Categorize error if present
            error_info = None
            if has_errors:
                error_info = self._categorize_changefeed_error(error, running_status)
            
            cursor.close()
            conn.close()
            
            result_dict = {
                'job_id': job_id,
                'status': status,
                'running_status': running_status,
                'error': error,
                'is_healthy': is_healthy,
                'has_errors': has_errors
            }
            
            # Add error categorization if available
            if error_info:
                result_dict['error_category'] = error_info['category']
                result_dict['error_suggestion'] = error_info['suggestion']
                result_dict['is_retryable'] = error_info['is_retryable']
                result_dict['error_description'] = error_info['description']
            
            return result_dict
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise
    
    def get_table_row_count(self, table_name: str, table_options: Dict[str, str] = None) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table to count
            table_options: Optional connection parameters
        
        Returns:
            Number of rows in the table
        
        Raises:
            ValueError: If table doesn't exist
        """
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            # Use fully qualified table name
            target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            
            cursor.execute(f"SELECT COUNT(*) FROM {target}")
            result = cursor.fetchone()
            
            count = result[0] if result else 0
            
            cursor.close()
            conn.close()
            
            return count
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise ValueError(f"Failed to get row count for table '{table_name}': {str(e)}")
    
    def find_changefeeds_for_table(
        self, 
        table_name: str, 
        table_options: Dict[str, str] = None,
        statuses: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find existing changefeeds for a specific table.
        
        Args:
            table_name: Name of the table to search for
            table_options: Optional connection parameters
            statuses: List of job statuses to filter by (default: ['running', 'pending', 'paused'])
        
        Returns:
            List of dictionaries with changefeed information:
            [{
                'job_id': int,
                'status': str,
                'running_status': str,
                'description': str,
                'has_errors': bool
            }, ...]
        """
        if statuses is None:
            statuses = ['running', 'pending', 'paused']
        
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            # Build status filter
            status_list = "', '".join(statuses)
            
            cursor.execute(f"""
                SELECT job_id, status, running_status, description
                FROM [SHOW JOBS] 
                WHERE job_type = 'CHANGEFEED' 
                AND description LIKE '%{table_name}%'
                AND status IN ('{status_list}')
                LIMIT 100
            """)
            
            jobs = cursor.fetchall()
            
            result = []
            for job_id, status, running_status, description in jobs:
                has_errors = False
                if running_status:
                    has_errors = 'error' in str(running_status).lower()
                
                result.append({
                    'job_id': int(job_id),
                    'status': status,
                    'running_status': running_status,
                    'description': description,
                    'has_errors': has_errors
                })
            
            cursor.close()
            conn.close()
            
            return result
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise
    
    def create_changefeed_to_azure(
        self,
        table_name: str,
        azure_uri: str,
        changefeed_format: str = 'parquet',
        initial_scan: str = 'yes',
        table_options: Dict[str, str] = None
    ) -> int:
        """
        Create a changefeed that writes to Azure Blob Storage.
        
        Args:
            table_name: Name of the table to create changefeed for
            azure_uri: Azure storage URI (with credentials)
            changefeed_format: 'parquet' or 'json' (default: 'parquet')
            initial_scan: 'yes', 'no', or 'only' (default: 'yes')
            table_options: Optional connection parameters
        
        Returns:
            Job ID of the created changefeed
        
        Raises:
            Exception: If changefeed creation fails
        """
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            # Use fully qualified table name
            target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            
            # Check if table has multiple column families
            has_multiple_families = self._has_multiple_column_families(table_name, table_options)
            
            # Build changefeed SQL based on format
            if changefeed_format == 'parquet':
                options = [
                    "format = 'parquet'",
                    "compression = 'gzip'",
                    f"initial_scan = '{initial_scan}'"
                ]
                
                # Add CDC options only if not snapshot-only
                if initial_scan != 'only':
                    options.insert(0, "updated")
                    options.insert(1, "resolved = '1s'")
                
                if has_multiple_families:
                    options.append("split_column_families")
                
                options_str = ",\n  ".join(options)
                changefeed_sql = f"""
CREATE CHANGEFEED FOR TABLE {target}
INTO '{azure_uri}'
WITH 
  {options_str}
"""
            else:  # json
                options = [
                    "format = 'json'",
                    "envelope = 'wrapped'",
                    f"initial_scan = '{initial_scan}'"
                ]
                
                # Add CDC options only if not snapshot-only
                if initial_scan != 'only':
                    options.insert(0, "updated")
                    options.insert(1, "diff")  # Include 'before' field for updates
                    options.insert(2, "resolved = '1s'")
                
                if has_multiple_families:
                    options.append("split_column_families")
                
                options_str = ",\n  ".join(options)
                changefeed_sql = f"""
CREATE CHANGEFEED FOR TABLE {target}
INTO '{azure_uri}'
WITH 
  {options_str}
"""
            
            cursor.execute(changefeed_sql)
            result = cursor.fetchone()
            
            job_id = int(result[0]) if result else None
            
            cursor.close()
            conn.close()
            
            if not job_id:
                raise Exception("Failed to create changefeed: no job ID returned")
            
            return job_id
            
        except Exception as e:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
            raise Exception(f"Failed to create changefeed: {str(e)}")
    
    def execute_sql(
        self,
        sql: str,
        commit: bool = False,
        table_options: Dict[str, str] = None
    ) -> List[tuple]:
        """
        Execute arbitrary SQL query.
        
        Args:
            sql: SQL query to execute
            commit: Whether to commit after execution (for DML)
            table_options: Optional connection parameters
        
        Returns:
            List of result rows (empty for DML statements)
        """
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            cursor.execute(sql)
            
            # Try to fetch results (will be empty for DML)
            try:
                results = cursor.fetchall()
            except:
                results = []
            
            if commit:
                conn.commit()
            
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise
    
    def batch_update_table(
        self,
        table_name: str,
        updates: List[Dict[str, Any]],
        batch_size: int = 500,
        table_options: Dict[str, str] = None
    ) -> int:
        """
        Execute batch updates on a table.
        
        Args:
            table_name: Name of the table to update
            updates: List of update specifications, each containing:
                     {'set_clause': 'column = value', 'where_clause': 'condition'}
            batch_size: Number of rows to update per batch
            table_options: Optional connection parameters
        
        Returns:
            Total number of rows updated
        
        Example:
            updates = [
                {
                    'set_clause': "field0 = field0 || '_batch1'",
                    'where_clause': "ycsb_key IN (SELECT ycsb_key FROM usertable ORDER BY ycsb_key LIMIT 500)"
                }
            ]
            count = connector.batch_update_table('usertable', updates)
        """
        conn = self._get_connection(table_options)
        total_updated = 0
        
        try:
            cursor = self._create_cursor(conn)
            target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            
            for update_spec in updates:
                set_clause = update_spec.get('set_clause', '')
                where_clause = update_spec.get('where_clause', '')
                
                if not set_clause:
                    continue
                
                sql = f"UPDATE {target} SET {set_clause}"
                if where_clause:
                    sql += f" WHERE {where_clause}"
                
                cursor.execute(sql)
                conn.commit()
                total_updated += 1
            
            cursor.close()
            conn.close()
            
            return total_updated
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise
    
    def cancel_changefeed(
        self,
        job_id: int,
        max_attempts: int = 3,
        poll_interval: int = 2,
        table_options: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Cancel a changefeed job and wait for it to complete.
        
        Args:
            job_id: The changefeed job ID to cancel
            max_attempts: Maximum number of status check attempts
            poll_interval: Seconds to wait between status checks
            table_options: Optional connection parameters
        
        Returns:
            Dictionary with cancellation result:
            {
                'success': bool,
                'final_status': str,
                'message': str
            }
        """
        import time
        
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            # Issue cancel command
            cursor.execute(f"CANCEL JOB {job_id}")
            cursor.close()
            
            # Poll for completion
            for attempt in range(max_attempts):
                # Check status first, then sleep (except on last attempt)
                cursor = self._create_cursor(conn)
                cursor.execute(f"""
                    SELECT status
                    FROM [SHOW JOBS]
                    WHERE job_id = {job_id}
                """)
                
                result = cursor.fetchone()
                cursor.close()
                
                if not result:
                    conn.close()
                    return {
                        'success': False,
                        'final_status': 'unknown',
                        'message': 'Job not found after cancel'
                    }
                
                status = result[0]
                
                if status in ('canceled', 'cancelled', 'failed'):
                    conn.close()
                    return {
                        'success': True,
                        'final_status': status,
                        'message': f'Successfully cancelled (status: {status})'
                    }
                
                # Sleep before next attempt (but not after last attempt)
                if attempt < max_attempts - 1:
                    time.sleep(poll_interval)
            
            # Timeout
            conn.close()
            return {
                'success': False,
                'final_status': 'timeout',
                'message': f'Cancel timeout after {max_attempts * poll_interval}s'
            }
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            return {
                'success': False,
                'final_status': 'error',
                'message': f'Cancel failed: {str(e)}'
            }
    
    def _list_azure_parquet_files(
        self, container_client, prefix: str, last_cursor: str, table_name: str = None
    ) -> List[Dict[str, Any]]:
        """
        List Parquet files in Azure, filtered by cursor and optionally by table name.
        
        Args:
            container_client: Azure container client
            prefix: Path prefix to search
            last_cursor: Last processed timestamp (cursor)
            table_name: Optional table name to filter by (embedded in filename)
        
        Returns: List of file metadata dictionaries with 'name', 'timestamp', 'size'
        """
        blobs = container_client.list_blobs(name_starts_with=prefix)
        
        parquet_files = []
        for blob in blobs:
            # Skip non-Parquet files
            if not blob.name.endswith('.parquet'):
                continue
            
            # Skip .RESOLVED marker files
            if '.RESOLVED' in blob.name:
                continue
            
            # Skip metadata directories and files (e.g., _metadata/schema.json, _schema.json, _SUCCESS, etc.)
            if '/_metadata/' in blob.name:
                continue
            
            blob_name_only = blob.name.split('/')[-1]
            if blob_name_only.startswith('_'):
                continue
            
            # Filter by table name if provided (table name is embedded in filename)
            # Example filename: 202512222012135254607470000000000-...-usertable-...
            if table_name and table_name not in blob.name:
                continue
            
            # Extract timestamp from filename
            timestamp = self._extract_timestamp_from_blob_name(blob.name)
            if not timestamp:
                continue
            
            # Filter by cursor
            if last_cursor and timestamp <= last_cursor:
                continue
            
            parquet_files.append({
                'name': blob.name,
                'timestamp': timestamp,
                'size': blob.size
            })
        
        # Sort by timestamp
        parquet_files.sort(key=lambda f: f['timestamp'])
        
        return parquet_files
    
    def _extract_timestamp_from_blob_name(self, blob_name: str) -> str:
        """
        Extract CockroachDB timestamp from Parquet file name.
        
        Example: 202512191714242809831900000000000-...-usertable+fam_0_ycsb_key-4.parquet
        Returns: "202512191714242809831900000000000"
        """
        import re
        match = re.search(r'(\d{35})', blob_name)
        return match.group(1) if match else None
    
    def _dump_table_schema(self, table_name: str, table_options: Dict[str, str] = None) -> dict:
        """
        Extract complete table schema including primary keys, columns, and metadata.
        
        Args:
            table_name: Name of the table
            table_options: Optional connection parameters
            
        Returns:
            Dictionary with complete schema information:
            {
                'table_name': str,
                'catalog': str,
                'schema': str,
                'primary_keys': List[str],
                'columns': List[dict],
                'create_statement': str,
                'has_column_families': bool,
                'schema_version': int,
                'created_at': str (ISO format)
            }
        """
        from datetime import datetime
        
        conn = self._get_connection(table_options)
        try:
            cursor = self._create_cursor(conn)
            
            # Get CREATE TABLE statement
            target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            cursor.execute(f"SHOW CREATE TABLE {target}")
            result = cursor.fetchone()
            create_statement = result[1] if result else ""
            
            # Get primary keys
            pk_query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                  AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """
            cursor.execute(pk_query, (self.schema, table_name))
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get column information
            col_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s 
                  AND table_name = %s
                ORDER BY ordinal_position
            """
            cursor.execute(col_query, (self.schema, table_name))
            columns = [
                {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES'
                }
                for row in cursor.fetchall()
            ]
            
            # Check for column families
            has_column_families = create_statement.upper().count('FAMILY ') > 1
            
            cursor.close()
            conn.close()
            
            return {
                'table_name': table_name,
                'catalog': self.catalog,
                'schema': self.schema,
                'primary_keys': primary_keys,
                'columns': columns,
                'create_statement': create_statement,
                'has_column_families': has_column_families,
                'schema_version': 1,
                'created_at': datetime.utcnow().isoformat() + 'Z'
            }
            
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            raise Exception(f"Failed to dump schema for table '{table_name}': {str(e)}")
    
    def _store_schema_to_azure(self, table_name: str, schema_info: dict, format_type: str = 'parquet'):
        """
        Store schema file in Azure Blob Storage alongside data files.
        
        Schema is stored as: {format}/{catalog}/{schema}/{table}/_metadata/schema.json
        
        Args:
            table_name: Name of the table
            schema_info: Schema dictionary from _dump_table_schema()
            format_type: 'parquet' or 'json' (determines storage path)
        """
        from azure.storage.blob import BlobServiceClient
        import json
        
        blob_service_client = BlobServiceClient(
            account_url=f"https://{self.azure_account_name}.blob.core.windows.net",
            credential=self.azure_account_key
        )
        
        # Determine path based on format
        if format_type == 'parquet':
            path_prefix = self.parquet_path_prefix
        else:
            path_prefix = self.json_path_prefix
        
        # Store schema at: format/catalog/schema/table/_metadata/schema.json
        schema_blob_name = f"{path_prefix}/{table_name}/_metadata/schema.json"
        
        blob_client = blob_service_client.get_blob_client(
            container=self.azure_container,
            blob=schema_blob_name
        )
        
        # Convert to pretty JSON
        schema_json = json.dumps(schema_info, indent=2)
        
        # Upload (overwrite if exists)
        blob_client.upload_blob(schema_json, overwrite=True)
    
    def _load_schema_from_azure(self, table_name: str, format_type: str = 'parquet') -> dict:
        """
        Load schema file from Azure Blob Storage.
        
        Args:
            table_name: Name of the table
            format_type: 'parquet' or 'json' (determines storage path)
            
        Returns:
            Schema dictionary, or None if not found
        """
        from azure.storage.blob import BlobServiceClient
        import json
        
        try:
            blob_service_client = BlobServiceClient(
                account_url=f"https://{self.azure_account_name}.blob.core.windows.net",
                credential=self.azure_account_key
            )
            
            # Determine path based on format
            if format_type == 'parquet':
                path_prefix = self.parquet_path_prefix
            else:
                path_prefix = self.json_path_prefix
            
            # Load schema from: format/catalog/schema/table/_metadata/schema.json
            schema_blob_name = f"{path_prefix}/{table_name}/_metadata/schema.json"
            
            blob_client = blob_service_client.get_blob_client(
                container=self.azure_container,
                blob=schema_blob_name
            )
            
            # Download and parse JSON
            download_stream = blob_client.download_blob()
            schema_json = download_stream.readall().decode('utf-8')
            schema_info = json.loads(schema_json)
            
            return schema_info
            
        except Exception as e:
            # Schema file not found or error reading - return None
            return None
    
    def _load_schema_from_volume(self, volume_path: str, spark=None, dbutils=None) -> dict:
        """
        Load schema file from Unity Catalog Volume.
        
        Args:
            volume_path: Base volume path (e.g., 'dbfs:/Volumes/main/schema/volume')
            spark: Optional SparkSession (uses self._spark if not provided, REQUIRED)
            dbutils: Optional DBUtils instance (uses self._dbutils if not provided, REQUIRED)
            
        Returns:
            Schema dictionary, or None if not found
            
        Raises:
            RuntimeError: If spark or dbutils not available
        """
        import json
        
        try:
            # Validate spark and dbutils are available (single source of truth - NO FALLBACKS)
            spark, dbutils = self._ensure_spark_and_dbutils(spark, dbutils)
            
            # Load schema from: volume_path/_metadata/schema.json
            schema_file_path = f"{volume_path}/_metadata/schema.json"
            
            try:
                # Read file (max 1MB)
                file_content = dbutils.fs.head(schema_file_path, 1048576)
                
                # Check if file was truncated (indicates file > 1MB)
                if len(file_content) >= 1048576:
                    raise ValueError(
                        f"Schema file too large (>1MB): {schema_file_path}\n"
                        f"Schema files should be small JSON metadata files."
                    )
                
                # Parse JSON
                schema_info = json.loads(file_content)
                return schema_info
                
            except json.JSONDecodeError as e:
                # Invalid JSON in schema file
                raise ValueError(
                    f"Invalid JSON in schema file: {schema_file_path}\n"
                    f"Error: {e}"
                )
            except ValueError:
                # Re-raise ValueError (file too large or JSON error)
                raise
            except Exception as e:
                # File not found or other dbutils error
                # Return None for "file not found" (expected case)
                if "FileNotFoundException" in str(type(e).__name__) or "does not exist" in str(e).lower():
                    return None
                # For other errors, raise with context
                raise RuntimeError(
                    f"Error reading schema file: {schema_file_path}\n"
                    f"Error: {e}"
                )
                
        except (ValueError, RuntimeError):
            # Re-raise validation/parsing errors
            raise
        except Exception as e:
            # Unexpected error in validation
            raise RuntimeError(f"Unexpected error loading schema: {e}")
    
    def _ensure_azure_dependencies(self):
        """
        Verify Azure dependencies are available.
        
        Note: Dependencies should be installed via pipeline libraries configuration.
        This method just imports them - will fail naturally if missing.
        """
        if self.mode != "azure_parquet":
            return
        
        # Import dependencies - will raise ImportError if not installed
        import azure.storage.blob
        import pyarrow.parquet
        import pandas


# ============================================================================
# Utility Functions
# ============================================================================

def load_crdb_config(json_path: str) -> dict:
    """
    Load CockroachDB credentials from JSON file.
    
    Args:
        json_path: Path to JSON file containing CockroachDB credentials
        
    Returns:
        Dictionary with credentials
        
    Example:
        config = load_crdb_config('.env/cockroachdb_credentials.json')
        connector = create_connector(config)
    """
    import json
    with open(json_path) as f:
        return json.load(f)


def get_primary_keys(
    crdb_config: dict,
    table_name: str,
    catalog: str = 'defaultdb',
    schema: str = 'public'
) -> List[str]:
    """
    Get primary key columns for a table.
    
    Utility function for scripts that need primary keys without creating a full connector.
    
    Args:
        crdb_config: Dictionary with CockroachDB credentials
        table_name: Name of the table
        catalog: Database/catalog name (default: 'defaultdb')
        schema: Schema name (default: 'public')
        
    Returns:
        List of primary key column names (e.g., ['id'] or ['ycsb_key'])
        
    Raises:
        ValueError: If table has no primary key or doesn't exist
        
    Example:
        ```python
        from cockroachdb import load_crdb_config, get_primary_keys
        
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        pk_columns = get_primary_keys(crdb_config, 'usertable')
        print(f"Primary keys: {pk_columns}")
        # Output: Primary keys: ['ycsb_key']
        ```
    """
    connector = create_connector(crdb_config, catalog, schema)
    try:
        metadata = connector.read_table_metadata(table_name, {})
        return metadata['primary_keys']
    except Exception as e:
        raise ValueError(f"Failed to get primary keys for table '{table_name}': {str(e)}")


def generate_test_table_sql(
    table_name: str,
    schema: str = 'simple',
    row_count: int = 1000,
    include_column_families: bool = False
) -> str:
    """
    Generate CREATE TABLE + INSERT SQL for deterministic test data.
    
    Uses generate_series() for fast, deterministic data generation without dependencies.
    
    Args:
        table_name: Name of table to create
        schema: 'simple' (id, name, value) or 'ycsb' (ycsb_key + 10 fields)
        row_count: Number of rows to generate (default: 1000)
        include_column_families: Whether to create column families (default: False)
        
    Returns:
        SQL string for CREATE TABLE + INSERT with DROP TABLE IF EXISTS
        
    Schema Types:
        - 'simple': Basic test table with INT PRIMARY KEY (id)
                   Columns: id, name, value, updated_at
                   Good for: Simple CDC testing
                   
        - 'ycsb': YCSB benchmark-style table with VARCHAR PRIMARY KEY (ycsb_key)
                 Columns: ycsb_key, field0-field9 (all TEXT)
                 Good for: Realistic workload testing with column families
        
    Example:
        ```python
        from cockroachdb import generate_test_table_sql, create_connector, load_crdb_config
        
        # Generate SQL for simple test table
        sql = generate_test_table_sql('test_simple', 'simple', 1000)
        
        # Execute it
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        connector = create_connector(crdb_config)
        connector.execute_sql(sql, commit=True)
        
        # Generate YCSB table with column families
        sql = generate_test_table_sql('test_ycsb', 'ycsb', 10000, include_column_families=True)
        connector.execute_sql(sql, commit=True)
        ```
    """
    if schema == 'simple':
        return f"""DROP TABLE IF EXISTS {table_name} CASCADE;
CREATE TABLE {table_name} (
    id INT PRIMARY KEY,
    name STRING,
    value INT,
    updated_at TIMESTAMP DEFAULT now()
);
INSERT INTO {table_name} (id, name, value)
SELECT i, 'test_' || i, i * 100
FROM generate_series(1, {row_count}) AS i;"""
    
    elif schema == 'ycsb':
        # Column families: pk (ycsb_key) + data (field0-field9)
        families_sql = ""
        if include_column_families:
            families_sql = ",\n    FAMILY pk (ycsb_key),\n    FAMILY data (field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)"
        
        return f"""DROP TABLE IF EXISTS {table_name} CASCADE;
CREATE TABLE {table_name} (
    ycsb_key VARCHAR(255) PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT,
    field3 TEXT,
    field4 TEXT,
    field5 TEXT,
    field6 TEXT,
    field7 TEXT,
    field8 TEXT,
    field9 TEXT{families_sql}
);
INSERT INTO {table_name} (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
SELECT 
    'user' || LPAD(i::TEXT, 10, '0'),
    'field0_' || i || '_data',
    'field1_' || (i % 100) || '_value',
    'field2_' || MD5(i::TEXT),
    'field3_' || REPEAT('x', (i % 50) + 1),
    'field4_' || i,
    'field5_' || i,
    'field6_' || i,
    'field7_' || i,
    'field8_' || i,
    'field9_' || i
FROM generate_series(1, {row_count}) AS i;"""
    
    else:
        raise ValueError(f"Unknown schema type: '{schema}'. Use 'simple' or 'ycsb'.")


def generate_test_insert_sql(
    table_name: str,
    schema: str = 'simple',
    row_count: int = 50,
    start_id: int = 10001,
    key_prefix: str = 'newuser'
) -> str:
    """
    Generate INSERT SQL for adding test rows to existing tables.
    
    Used for CDC testing to insert new rows after initial snapshot.
    Uses generate_series() for deterministic data with non-conflicting keys.
    
    Args:
        table_name: Name of table to insert into
        schema: 'simple' (id, name, value) or 'ycsb' (ycsb_key + 10 fields)
        row_count: Number of rows to insert (default: 50)
        start_id: Starting ID for simple schema (default: 10001)
        key_prefix: Key prefix for ycsb schema (default: 'newuser')
        
    Returns:
        SQL string for INSERT with generate_series()
        
    Example:
        ```python
        from cockroachdb import generate_test_insert_sql, create_connector, load_crdb_config
        
        # Simple table: Insert 50 rows starting at ID 10001
        sql = generate_test_insert_sql('test_simple', 'simple', 50, start_id=10001)
        
        # YCSB table: Insert 50 rows with 'newuser' prefix
        sql = generate_test_insert_sql('test_ycsb', 'ycsb', 50, key_prefix='newuser')
        
        # Execute it
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        connector = create_connector(crdb_config)
        connector.execute_sql(sql, commit=True)
        ```
    """
    if schema == 'simple':
        return f"""INSERT INTO {table_name} (id, name, value)
SELECT {start_id} + i - 1, 'inserted_' || i, i * 200
FROM generate_series(1, {row_count}) AS i;"""
    
    elif schema == 'ycsb':
        return f"""INSERT INTO {table_name} (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
SELECT 
    '{key_prefix}' || LPAD(i::TEXT, 10, '0'),
    'field0_new_' || i,
    'field1_new_' || i,
    'field2_new_' || MD5(i::TEXT),
    'field3_new_' || REPEAT('y', (i % 30) + 1),
    'field4_new_' || i,
    'field5_new_' || i,
    'field6_new_' || i,
    'field7_new_' || i,
    'field8_new_' || i,
    'field9_new_' || i
FROM generate_series(1, {row_count}) AS i;"""
    
    else:
        raise ValueError(f"Unknown schema type: '{schema}'. Use 'simple' or 'ycsb'.")


def generate_test_update_sql(
    table_name: str,
    schema: str = 'simple',
    row_count: int = 400,
    start_id: int = 1
) -> str:
    """
    Generate UPDATE SQL for modifying existing rows deterministically.
    
    Used for CDC testing to update rows and generate CDC events.
    Uses ORDER BY + LIMIT for deterministic row selection.
    
    Args:
        table_name: Name of table to update
        schema: 'simple' (id, name, value) or 'ycsb' (ycsb_key + 10 fields)
        row_count: Number of rows to update (default: 400)
        start_id: Starting ID for simple schema (default: 1, i.e., update first 400 rows)
        
    Returns:
        SQL string for UPDATE with deterministic WHERE clause
        
    Example:
        ```python
        from cockroachdb import generate_test_update_sql, create_connector, load_crdb_config
        
        # Simple table: Update first 400 rows
        sql = generate_test_update_sql('test_simple', 'simple', 400)
        
        # YCSB table: Update first 400 rows by ycsb_key order
        sql = generate_test_update_sql('test_ycsb', 'ycsb', 400)
        
        # Execute it
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        connector = create_connector(crdb_config)
        connector.execute_sql(sql, commit=True)
        ```
    """
    if schema == 'simple':
        return f"""UPDATE {table_name}
SET name = name || '_updated',
    value = value + 1000,
    updated_at = now()
WHERE id IN (
    SELECT id FROM {table_name}
    ORDER BY id
    LIMIT {row_count}
);"""
    
    elif schema == 'ycsb':
        return f"""UPDATE {table_name}
SET field0 = field0 || '_updated',
    field1 = field1 || '_modified'
WHERE ycsb_key IN (
    SELECT ycsb_key FROM {table_name}
    ORDER BY ycsb_key
    LIMIT {row_count}
);"""
    
    else:
        raise ValueError(f"Unknown schema type: '{schema}'. Use 'simple' or 'ycsb'.")


def generate_test_delete_sql(
    table_name: str,
    schema: str = 'simple',
    row_count: int = 100,
    from_end: bool = True
) -> str:
    """
    Generate DELETE SQL for removing rows deterministically.
    
    Used for CDC testing to delete rows and generate CDC delete events.
    Uses ORDER BY + LIMIT for deterministic row selection.
    
    Args:
        table_name: Name of table to delete from
        schema: 'simple' (id, name, value) or 'ycsb' (ycsb_key + 10 fields)
        row_count: Number of rows to delete (default: 100)
        from_end: Delete from end of table (DESC order) vs beginning (ASC order)
        
    Returns:
        SQL string for DELETE with deterministic WHERE clause
        
    Example:
        ```python
        from cockroachdb import generate_test_delete_sql, create_connector, load_crdb_config
        
        # Simple table: Delete last 100 rows
        sql = generate_test_delete_sql('test_simple', 'simple', 100)
        
        # YCSB table: Delete last 100 rows by ycsb_key order
        sql = generate_test_delete_sql('test_ycsb', 'ycsb', 100)
        
        # Execute it
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        connector = create_connector(crdb_config)
        connector.execute_sql(sql, commit=True)
        ```
    """
    order = "DESC" if from_end else "ASC"
    
    if schema == 'simple':
        return f"""DELETE FROM {table_name}
WHERE id IN (
    SELECT id FROM {table_name}
    ORDER BY id {order}
    LIMIT {row_count}
);"""
    
    elif schema == 'ycsb':
        return f"""DELETE FROM {table_name}
WHERE ycsb_key IN (
    SELECT ycsb_key FROM {table_name}
    ORDER BY ycsb_key {order}
    LIMIT {row_count}
);"""
    
    else:
        raise ValueError(f"Unknown schema type: '{schema}'. Use 'simple' or 'ycsb'.")


def create_connector(
    crdb_config: dict, 
    catalog: str = 'defaultdb', 
    schema: str = 'public',
    azure_config: dict = None
) -> LakeflowConnect:
    """
    Create LakeflowConnect instance from configuration dictionary.
    
    Supports multiple formats:
    - token + base_url (GitHub-style)
    - cockroachdb_url (full PostgreSQL URL)
    - host + port + database + user + password (individual params)
    
    Optionally adds Azure credentials for changefeed mode.
    
    Args:
        crdb_config: Dictionary with CockroachDB credentials
        catalog: Database/catalog name (default: 'defaultdb')
        schema: Schema name (default: 'public')
        azure_config: Optional dictionary with Azure credentials:
                     {'azure_account_name', 'azure_account_key', 'azure_container'}
        
    Returns:
        Configured LakeflowConnect instance
        
    Example:
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        azure_config = load_crdb_config('.env/cockroachdb_cdc_azure.json')
        connector = create_connector(crdb_config, azure_config=azure_config)
    """
    import io
    import contextlib
    
    options = {
        'catalog': catalog,
        'schema': schema
    }
    
    # Support multiple credential formats
    # Check for non-empty values, not just presence of keys
    if crdb_config.get('token') and crdb_config.get('base_url'):
        # GitHub-style format
        options['token'] = crdb_config['token']
        options['base_url'] = crdb_config['base_url']
    elif 'cockroachdb_url' in crdb_config:
        # Full URL format - convert to token+base_url
        url = crdb_config['cockroachdb_url']
        
        if not url.startswith('postgresql://'):
            raise ValueError("cockroachdb_url must start with 'postgresql://'")
        
        if '@' not in url:
            raise ValueError("cockroachdb_url must contain credentials (user:pass@host)")
        
        # Extract token (user:pass) and base_url (host:port/db?params)
        url_without_scheme = url.replace('postgresql://', '', 1)
        token_part, host_part = url_without_scheme.split('@', 1)
        
        options['token'] = token_part
        options['base_url'] = f"postgresql://{host_part}"
    elif 'host' in crdb_config:
        # Individual parameters format
        options['host'] = crdb_config['host']
        options['port'] = crdb_config.get('port', 26257)
        options['database'] = crdb_config['database']
        options['user'] = crdb_config['user']
        options['password'] = crdb_config.get('password', '')
        options['sslmode'] = crdb_config.get('sslmode', 'require')
    else:
        raise ValueError(
            "Config must contain either 'token'+'base_url', 'cockroachdb_url', "
            "or 'host'+'database'+'user'"
        )
    
    # Add Azure credentials if provided
    if azure_config:
        options['azure_account_name'] = azure_config.get('azure_storage_account')
        options['azure_account_key'] = azure_config.get('azure_storage_key')
        options['azure_container'] = azure_config.get('azure_storage_container')
    
    # Suppress print statement from __init__
    with contextlib.redirect_stdout(io.StringIO()):
        connector = LakeflowConnect(options)
    
    return connector


def analyze_azure_changefeed_files(
    account_name: str,
    account_key: str,
    container_name: str,
    path_prefix: str,
    format_type: str = 'parquet',
    primary_key_columns: List[str] = None,
    debug: bool = False
) -> dict:
    """
    Analyze all changefeed files in Azure Blob Storage and return statistics.
    
    For Parquet format with split_column_families, automatically deduplicates
    events by primary key using the same coalescing logic as the connector.
    
    Args:
        account_name: Azure storage account name
        account_key: Azure storage account key
        container_name: Azure container name
        path_prefix: Path prefix to search (e.g., 'parquet-cdc' or 'json-cdc')
        format_type: 'parquet' or 'json'
        primary_key_columns: List of primary key column names (e.g., ['id', 'ycsb_key'])
                            Required for Parquet to deduplicate correctly. If not provided, will try to infer.
        debug: Enable debug output
        
    Returns:
        Dictionary with aggregated statistics:
        {
            'snapshot': int,
            'insert': int,
            'update': int,
            'delete': int,
            'file_count': int,
            'unique_keys': int  # Count of unique primary keys (after deduplication)
        }
        
    Example:
        azure_config = load_crdb_config('.env/cockroachdb_cdc_azure.json')
        stats = analyze_azure_changefeed_files(
            azure_config['azure_storage_account'],
            azure_config['azure_storage_key'],
            azure_config['azure_storage_container'],
            'parquet-cdc',
            format_type='parquet',
            primary_key_columns=['ycsb_key']
        )
        print(f"Snapshot: {stats['snapshot']}, Updates: {stats['update']}")
    """
    from azure.storage.blob import BlobServiceClient
    
    # Connect to Azure
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Determine file extension and analysis function
    if format_type == 'parquet':
        file_extension = '.parquet'
    else:
        file_extension = '.ndjson'
    
    # Collect all data files (exclude .RESOLVED files, metadata files, and _metadata/ directory)
    data_blobs = []
    blob_list = container_client.list_blobs(name_starts_with=path_prefix)
    for blob in blob_list:
        # Skip metadata directory contents
        if '/_metadata/' in blob.name:
            continue
            
        blob_name = blob.name.split('/')[-1]  # Get just the filename
        if (file_extension in blob.name and 
            not blob.name.endswith('.RESOLVED') and 
            not blob_name.startswith('_')):
            data_blobs.append(blob.name)
    
    if not data_blobs:
        return {
            'snapshot': 0,
            'insert': 0,
            'update': 0,
            'delete': 0,
            'file_count': 0
        }
    
    # For Parquet format with split_column_families, use coalescing logic
    if format_type == 'parquet':
        import pandas as pd
        from io import BytesIO
        
        # Load primary keys from schema file (REQUIRED)
        if not primary_key_columns:
            # Schema file is required - load it
            schema_blob_name = f"{path_prefix}/_schema.json"
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=schema_blob_name)
                download_stream = blob_client.download_blob()
                schema_json = download_stream.readall().decode('utf-8')
                import json
                schema_info = json.loads(schema_json)
                primary_key_columns = schema_info.get('primary_keys', [])
                
                if not primary_key_columns:
                    raise ValueError(
                        f"Schema file found but contains no primary keys: {schema_blob_name}\n"
                        f"Ensure the source table has a primary key defined."
                    )
                
                if debug:
                    print(f"   Loaded primary keys from schema: {primary_key_columns}")
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Schema file not found: {schema_blob_name}\n"
                    f"Schema file is required for CDC analysis. "
                    f"Create changefeeds using cockroachdb.py to automatically generate schema files."
                )
            except Exception as e:
                raise ValueError(f"Failed to load schema file {schema_blob_name}: {e}")
        
        # Step 1: Determine snapshot cutoff timestamp
        # Find files with sequence 00000000 (snapshot files) and get max timestamp
        snapshot_cutoff = None
        snapshot_files = []
        cdc_files = []
        
        for blob_name in data_blobs:
            # Parse filename: TIMESTAMP-JOBID-SHARD-NODE-SEQUENCE-TABLE+FAMILY-VERSION.parquet
            # Example: 202601070255040508466230000000000-9bf8f51fc303997f-1-31556-00000000-test_parquet_usertable_with_split-1.parquet
            filename = blob_name.split('/')[-1]  # Get just the filename
            parts = filename.split('-')
            
            if len(parts) >= 5:
                sequence = parts[4]  # e.g., "00000000" or "00000001"
                if sequence == "00000000":
                    snapshot_files.append(blob_name)
                else:
                    cdc_files.append(blob_name)
            else:
                # Can't determine from filename, treat as CDC
                cdc_files.append(blob_name)
        
        if debug:
            print(f"   Found {len(snapshot_files)} snapshot files (sequence 00000000)")
            print(f"   Found {len(cdc_files)} CDC files (sequence 00000001+)")
            if snapshot_files:
                print(f"   Sample snapshot file: {snapshot_files[0].split('/')[-1]}")
            if cdc_files:
                print(f"   Sample CDC file: {cdc_files[0].split('/')[-1]}")
        
        # Read snapshot files to find the maximum timestamp (snapshot cutoff)
        if snapshot_files:
            max_timestamp = None
            for blob_name in snapshot_files:
                try:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                    download_stream = blob_client.download_blob()
                    parquet_data = BytesIO(download_stream.readall())
                    df = pd.read_parquet(parquet_data)
                    
                    # Find max timestamp in this file
                    if '__crdb__updated' in df.columns:
                        file_max = df['__crdb__updated'].max()
                        if max_timestamp is None or (file_max and file_max > max_timestamp):
                            max_timestamp = file_max
                        if debug:
                            print(f"   File {blob_name.split('/')[-1][:50]}... has max timestamp: {file_max}")
                except Exception as e:
                    if debug:
                        print(f"   Warning: Could not read timestamp from {blob_name}: {e}")
                    continue
            
            snapshot_cutoff = max_timestamp
            if debug:
                if snapshot_cutoff:
                    print(f"   ✅ Snapshot cutoff timestamp: {snapshot_cutoff}")
                else:
                    print(f"   ⚠️  No snapshot cutoff found (will treat all 'c' events as SNAPSHOT)")
        
        # Step 2: Process all files with timestamp-based classification
        all_events = []
        
        for blob_name in data_blobs:
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                download_stream = blob_client.download_blob()
                parquet_data = BytesIO(download_stream.readall())
                df = pd.read_parquet(parquet_data)
                records = df.to_dict('records')
                
                # Process rows and extract primary keys correctly
                for record in records:
                    event_type = record.get('__crdb__event_type', '')
                    event_timestamp = record.get('__crdb__updated', '')
                    
                    # ============================================================================
                    # ⚠️  CDC OPERATION DETECTION LOGIC - TEST PATH (Parquet Analysis)
                    # ============================================================================
                    # This logic is DUPLICATED from _add_cdc_metadata_to_dataframe() (production).
                    # If you change this logic, you MUST update the corresponding code at:
                    #   - Line ~1098: Production Spark streaming CDC detection
                    #
                    # Core Rules (must match production exactly):
                    #   - Parquet 'c' + timestamp <= cutoff → SNAPSHOT
                    #   - Parquet 'c' + timestamp > cutoff  → UPDATE
                    #   - Parquet 'd' → DELETE
                    # ============================================================================
                    
                    # Determine CDC operation using timestamp-based logic
                    if event_type == 'c':
                        # 'c' = create/change (ambiguous!)
                        # Use timestamp to distinguish snapshot from update
                        if snapshot_cutoff and event_timestamp:
                            try:
                                # Compare timestamps as strings (they're in sortable format)
                                if event_timestamp <= snapshot_cutoff:
                                    cdc_operation = 'SNAPSHOT'
                                else:
                                    cdc_operation = 'UPDATE'
                            except:
                                cdc_operation = 'SNAPSHOT'  # Default to snapshot if comparison fails
                        else:
                            cdc_operation = 'SNAPSHOT'  # No cutoff = treat as snapshot
                    elif event_type == 'd':
                        cdc_operation = 'DELETE'
                    else:
                        cdc_operation = 'UNKNOWN'
                    
                    # Extract ONLY primary key columns for deduplication
                    cdc_key_pairs = []
                    for pk_col in sorted(primary_key_columns):
                        if pk_col in record:
                            cdc_key_pairs.append((pk_col, record[pk_col]))
                    
                    # Build event with correct CDC key (PK only!)
                    event = {
                        **record,
                        '_cdc_key': cdc_key_pairs,  # ONLY primary keys
                        '_cdc_updated': event_timestamp,
                        '_cdc_operation': cdc_operation,
                        '_source_file': blob_name
                    }
                    all_events.append(event)
                
            except Exception as e:
                if debug:
                    print(f"Error processing {blob_name}: {e}")
                continue
        
        # Deduplicate using coalescing logic (groups by _cdc_key which is now PK-only)
        connector = LakeflowConnect({})
        if debug:
            print(f"   📍 Before coalescing: {len(all_events):,} events")
            # Show operation breakdown BEFORE coalescing
            ops_before = {'SNAPSHOT': 0, 'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
            delete_sample_keys = []
            for e in all_events:
                op = e.get('_cdc_operation', 'UNKNOWN')
                if op in ops_before:
                    ops_before[op] += 1
                if op == 'DELETE' and len(delete_sample_keys) < 3:
                    delete_sample_keys.append(e.get('_cdc_key', []))
            print(f"      Operations before: snap={ops_before['SNAPSHOT']}, ins={ops_before['INSERT']}, upd={ops_before['UPDATE']}, del={ops_before['DELETE']}")
            if delete_sample_keys:
                print(f"      Sample DELETE _cdc_keys: {delete_sample_keys}")
        
        coalesced_events = connector._coalesce_events_by_key(all_events)
        
        if debug:
            print(f"   📍 After coalescing: {len(coalesced_events):,} events")
            # Show operation breakdown AFTER coalescing
            ops_after = {'SNAPSHOT': 0, 'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
            for e in coalesced_events:
                op = e.get('_cdc_operation', 'UNKNOWN')
                if op in ops_after:
                    ops_after[op] += 1
            print(f"      Operations after: snap={ops_after['SNAPSHOT']}, ins={ops_after['INSERT']}, upd={ops_after['UPDATE']}, del={ops_after['DELETE']}")
        
        # Count operations from coalesced events
        # Now with timestamp-based analysis, we can distinguish snapshots from updates!
        total_stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
        active_keys = 0  # Count of non-deleted keys
        for event in coalesced_events:
            operation = event.get('_cdc_operation', 'UNKNOWN')
            if operation == 'SNAPSHOT':
                total_stats['snapshot'] += 1
                active_keys += 1
            elif operation == 'INSERT':
                total_stats['insert'] += 1
                active_keys += 1
            elif operation == 'UPDATE':
                total_stats['update'] += 1
                active_keys += 1
            elif operation == 'DELETE':
                total_stats['delete'] += 1
                # Don't count deleted keys in active_keys
            # Note: 'UPSERT' no longer used for Parquet with timestamp analysis
        
        total_stats['file_count'] = len(data_blobs)
        total_stats['unique_keys'] = active_keys  # Only non-deleted keys
        
        if debug:
            print(f"   After deduplication:")
            print(f"     Snapshot: {total_stats['snapshot']}")
            print(f"     Insert: {total_stats['insert']}")
            print(f"     Update: {total_stats['update']}")
            print(f"     Delete: {total_stats['delete']}")
            print(f"     Unique keys: {total_stats['unique_keys']}")
        
        return total_stats
    
    else:
        # For JSON, also use coalescing logic (deduplication by primary key)
        import json as json_lib
        from io import BytesIO
        
        # Load primary keys from schema file (REQUIRED)
        if not primary_key_columns:
            schema_blob_name = f"{path_prefix}/_schema.json"
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=schema_blob_name)
                download_stream = blob_client.download_blob()
                schema_json = download_stream.readall().decode('utf-8')
                schema_info = json_lib.loads(schema_json)
                primary_key_columns = schema_info.get('primary_keys', [])
                
                if not primary_key_columns:
                    raise ValueError(
                        f"Schema file found but contains no primary keys: {schema_blob_name}\n"
                        f"Ensure the source table has a primary key defined."
                    )
                
                if debug:
                    print(f"   Loaded primary keys from schema: {primary_key_columns}")
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Schema file not found: {schema_blob_name}\n"
                    f"Schema file is required for CDC analysis. "
                    f"Create changefeeds using cockroachdb.py to automatically generate schema files."
                )
            except Exception as e:
                raise ValueError(f"Failed to load schema file {schema_blob_name}: {e}")
        
        # Step 1: Determine snapshot cutoff timestamp for JSON files
        # Find files with sequence 00000000 (snapshot files) and get max timestamp
        snapshot_cutoff = None
        if debug:
            print(f"   📍 Determining snapshot cutoff from JSON files...")
            print(f"   📍 Total files to scan: {len(data_blobs)}")
        
        max_timestamp = None
        snapshot_file_count = 0
        for blob_name in data_blobs:
            # Check if it's a snapshot file (sequence 00000000)
            if '-00000000-' in blob_name or blob_name.endswith('-00000000.ndjson') or blob_name.endswith('-00000000.json'):
                snapshot_file_count += 1
                if debug and snapshot_file_count == 1:
                    print(f"   📍 First snapshot file: {blob_name}")
                try:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                    download_stream = blob_client.download_blob()
                    content = download_stream.readall().decode('utf-8')
                    
                    # Find max timestamp in this file
                    for line in content.strip().split('\n'):
                        if not line:
                            continue
                        try:
                            event_data = json_lib.loads(line)
                            timestamp_str = event_data.get('updated', '')
                            if timestamp_str:
                                if max_timestamp is None or timestamp_str > max_timestamp:
                                    max_timestamp = timestamp_str
                        except:
                            continue
                except Exception as e:
                    if debug:
                        print(f"   Warning: Could not read timestamp from {blob_name}: {e}")
                    continue
        
        snapshot_cutoff = max_timestamp
        if debug:
            print(f"   📍 Scanned {snapshot_file_count} snapshot files")
            if snapshot_cutoff:
                print(f"   ✅ JSON snapshot cutoff timestamp: {snapshot_cutoff}")
            else:
                print(f"   ⚠️  No snapshot cutoff found (will treat all 'after and not before' as SNAPSHOT)")
        
        # Step 2: Process all JSON files with snapshot cutoff
        all_events = []
        skipped_fragments = {}  # Track skipped fragments per file
        
        for blob_name in data_blobs:
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                download_stream = blob_client.download_blob()
                content = download_stream.readall().decode('utf-8')
                
                file_skipped = 0
                # Process each line in NDJSON file
                for line in content.strip().split('\n'):
                    if not line:
                        continue
                    try:
                        event_data = json_lib.loads(line)
                        
                        # ============================================================================
                        # ⚠️  CDC OPERATION DETECTION LOGIC - TEST PATH (JSON Analysis)
                        # ============================================================================
                        # This logic is DUPLICATED from _add_cdc_metadata_to_dataframe() (production).
                        # If you change this logic, you MUST update the corresponding code at:
                        #   - Line ~1098: Production Spark streaming CDC detection
                        #
                        # Core Rules (must match production exactly):
                        #   - JSON (after && !before) + timestamp <= cutoff → SNAPSHOT
                        #   - JSON (after && !before) + timestamp > cutoff  → INSERT
                        #   - JSON (after && before) → UPDATE
                        #   - JSON (!after && before) → DELETE
                        # ============================================================================
                        
                        # Determine CDC operation from before/after fields
                        after = event_data.get('after')
                        before = event_data.get('before')
                        event_timestamp = event_data.get('updated', '')
                        
                        if after and not before:
                            # Use timestamp to distinguish SNAPSHOT from INSERT
                            if snapshot_cutoff and event_timestamp:
                                if event_timestamp <= snapshot_cutoff:
                                    cdc_operation = 'SNAPSHOT'
                                else:
                                    cdc_operation = 'INSERT'
                                    # Debug: Log first INSERT detection
                                    if debug and 'insert_count' not in locals():
                                        insert_count = 1
                                        print(f"   📍 First INSERT detected: timestamp={event_timestamp} > cutoff={snapshot_cutoff}")
                            else:
                                # No timestamp info - assume SNAPSHOT (safe default)
                                cdc_operation = 'SNAPSHOT'
                            row_data = after
                        elif after and before:
                            cdc_operation = 'UPDATE'
                            row_data = after
                        elif before and not after:
                            cdc_operation = 'DELETE'
                            row_data = before
                        else:
                            continue  # Skip invalid events
                        
                        # Extract primary key from the top-level 'key' field (always present in CockroachDB changefeeds)
                        # This works correctly for split_column_families where PK might not be in after/before
                        cdc_key_pairs = []
                        key_values = event_data.get('key', [])
                        
                        # If 'key' field is missing or incomplete, fallback to extracting from row_data
                        if key_values and len(key_values) == len(primary_key_columns):
                            # Use top-level 'key' field (preferred - always present)
                            # IMPORTANT: Don't sort! 'key' array is in database order, matching primary_key_columns
                            for pk_col, pk_val in zip(primary_key_columns, key_values):
                                cdc_key_pairs.append((pk_col, pk_val))
                        else:
                            # Fallback: Extract from row_data
                            for pk_col in sorted(primary_key_columns):
                                if pk_col in row_data:
                                    cdc_key_pairs.append((pk_col, row_data[pk_col]))
                        
                        # Skip events that don't have complete primary key
                        if len(cdc_key_pairs) < len(primary_key_columns):
                            file_skipped += 1
                            continue
                        
                        # Build event with correct CDC key (PK only!)
                        event = {
                            **row_data,
                            '_cdc_key': cdc_key_pairs,  # ONLY primary keys
                            '_cdc_updated': event_data.get('updated', ''),
                            '_cdc_operation': cdc_operation,
                            '_source_file': blob_name
                        }
                        all_events.append(event)
                        
                    except json_lib.JSONDecodeError:
                        continue
                
                # Track files with skipped fragments
                if file_skipped > 0:
                    skipped_fragments[blob_name] = file_skipped
                        
            except Exception as e:
                if debug:
                    print(f"Error processing {blob_name}: {e}")
                continue
        
        # Print summary of skipped fragments (if any)
        if skipped_fragments and debug:
            total_skipped = sum(skipped_fragments.values())
            print(f"   ℹ️  Skipped {total_skipped:,} column family fragments without PK from {len(skipped_fragments)} files (expected for split_column_families=true)")
        
        # Deduplicate using coalescing logic (groups by _cdc_key which is now PK-only)
        connector = LakeflowConnect({})
        if debug:
            print(f"   📍 Before coalescing: {len(all_events):,} events")
            # Show operation breakdown BEFORE coalescing
            ops_before = {'SNAPSHOT': 0, 'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
            delete_sample_keys = []
            for e in all_events:
                op = e.get('_cdc_operation', 'UNKNOWN')
                if op in ops_before:
                    ops_before[op] += 1
                if op == 'DELETE' and len(delete_sample_keys) < 3:
                    delete_sample_keys.append(e.get('_cdc_key', []))
            print(f"      Operations before: snap={ops_before['SNAPSHOT']}, ins={ops_before['INSERT']}, upd={ops_before['UPDATE']}, del={ops_before['DELETE']}")
            if delete_sample_keys:
                print(f"      Sample DELETE _cdc_keys: {delete_sample_keys}")
        
        coalesced_events = connector._coalesce_events_by_key(all_events)
        
        if debug:
            print(f"   📍 After coalescing: {len(coalesced_events):,} events")
            # Show operation breakdown AFTER coalescing
            ops_after = {'SNAPSHOT': 0, 'INSERT': 0, 'UPDATE': 0, 'DELETE': 0}
            for e in coalesced_events:
                op = e.get('_cdc_operation', 'UNKNOWN')
                if op in ops_after:
                    ops_after[op] += 1
            print(f"      Operations after: snap={ops_after['SNAPSHOT']}, ins={ops_after['INSERT']}, upd={ops_after['UPDATE']}, del={ops_after['DELETE']}")
        
        # Count operations from coalesced events
        total_stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
        active_keys = 0  # Count of non-deleted keys
        operation_samples = {}  # Track first sample of each operation for debugging
        for event in coalesced_events:
            operation = event.get('_cdc_operation', 'UNKNOWN')
            
            # Track first sample of each operation type for debugging
            if debug and operation not in operation_samples:
                pk_val = event.get('_cdc_key', [])
                operation_samples[operation] = pk_val
            
            if operation == 'SNAPSHOT':
                total_stats['snapshot'] += 1
                active_keys += 1
            elif operation == 'INSERT':
                total_stats['insert'] += 1
                active_keys += 1
            elif operation == 'UPDATE':
                total_stats['update'] += 1
                active_keys += 1
            elif operation == 'DELETE':
                total_stats['delete'] += 1
                # Don't count deleted keys in active_keys
        
        total_stats['file_count'] = len(data_blobs)
        total_stats['unique_keys'] = active_keys  # Only non-deleted keys
        return total_stats


def analyze_volume_changefeed_files(
    volume_path: str,
    primary_key_columns: List[str] = None,
    debug: bool = False,
    spark = None,
    dbutils = None
) -> dict:
    """
    Analyze all changefeed files in Unity Catalog Volume and return statistics.
    
    For Parquet format with split_column_families, automatically deduplicates
    events by primary key using the same coalescing logic as the connector.
    
    Args:
        volume_path: Unity Catalog Volume path (e.g., 'dbfs:/Volumes/main/schema/volume_name')
        primary_key_columns: List of primary key column names (e.g., ['id', 'ycsb_key'])
                            If not provided, will try to load from schema file
        debug: Enable debug output
        spark: Optional SparkSession (REQUIRED for Spark Connect)
        dbutils: Optional DBUtils instance (REQUIRED for Spark Connect)
        
    Returns:
        Dictionary with aggregated statistics:
        {
            'snapshot': int,
            'insert': int,
            'update': int,
            'delete': int,
            'file_count': int,
            'unique_keys': int  # Count of unique primary keys (after deduplication)
        }
        
    Example:
        # With dbutils (Spark Connect compatible)
        stats = analyze_volume_changefeed_files(
            'dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files',
            primary_key_columns=['ycsb_key'],
            dbutils=dbutils,
            debug=False
        )
        
        # Without dbutils (classic Spark only)
        stats = analyze_volume_changefeed_files(
            'dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files',
            primary_key_columns=['ycsb_key'],
            debug=False
        )
        print(f"Snapshot: {stats['snapshot']}, Updates: {stats['update']}")
    """
    import pandas as pd
    
    # Validate spark and dbutils using temporary connector (NO FALLBACKS)
    temp_connector = LakeflowConnect({'volume_path': volume_path, 'spark': spark, 'dbutils': dbutils})
    spark, dbutils = temp_connector._ensure_spark_and_dbutils(spark, dbutils)
    
    # Use shared file listing method
    # Note: We list ALL files (including _metadata/) for accurate reporting
    # Filtering happens during processing (skip non-data files)
    file_list = temp_connector._list_volume_files(volume_path, spark=spark, dbutils=dbutils)
    
    if debug:
        print(f"   📂 Found {len(file_list)} files in {volume_path}")
    
    if not file_list:
        if debug:
            print(f"   ⚠️  No changefeed files found in {volume_path}")
        return {
            'snapshot': 0,
            'insert': 0,
            'update': 0,
            'delete': 0,
            'file_count': 0,
            'unique_keys': 0
        }
    
    # Load primary keys from schema file (REQUIRED)
    if not primary_key_columns:
        # Schema file is required - load it from volume using existing method
        try:
            schema_info = temp_connector._load_schema_from_volume(volume_path, spark=spark, dbutils=dbutils)
            
            if not schema_info:
                raise FileNotFoundError(
                    f"Schema file not found: {volume_path}/_schema.json\n"
                    f"Schema file is required for CDC analysis. "
                )
            
            primary_key_columns = schema_info.get('primary_keys', [])
            
            if not primary_key_columns:
                raise ValueError(
                    f"Schema file found but contains no primary keys: {volume_path}/_schema.json\n"
                    f"Ensure the source table has a primary key defined."
                )
            
            if debug:
                print(f"   Loaded primary keys from schema: {primary_key_columns}")
        except (FileNotFoundError, ValueError):
            raise
        except Exception as e:
            raise FileNotFoundError(
                f"Schema file not found or invalid: {volume_path}/_schema.json\n"
                f"Schema file is required for CDC analysis. "
                f"Create changefeeds using cockroachdb.py to automatically generate schema files.\n"
                f"Error: {e}"
            )
    
    # Step 1: For JSON, detect snapshot cutoff timestamp
    # CRITICAL: Only look at SNAPSHOT files (sequence 00000000), NOT CDC files
    snapshot_cutoff = None
    if any(f['name'].endswith(('.ndjson', '.json')) for f in file_list):
        # JSON format detected - need to find snapshot cutoff
        # Filter for snapshot files only (those with -00000000- in filename)
        snapshot_files = [f for f in file_list 
                         if f['name'].endswith(('.ndjson', '.json'))
                         and '-00000000-' in f['name']]
        
        if not snapshot_files:
            # Fallback: if no explicit snapshot files, use first file only
            json_files = sorted([f for f in file_list if f['name'].endswith(('.ndjson', '.json'))], 
                               key=lambda x: x['name'])
            snapshot_files = json_files[:1] if json_files else []
        
        max_timestamp = None
        
        for file_info in snapshot_files:
            try:
                file_path = file_info['path']
                df = spark.read.json(file_path)
                pdf = df.toPandas()
                
                # Find max timestamp in this file
                if 'updated' in pdf.columns:
                    file_timestamps = pdf['updated'].dropna()
                    if len(file_timestamps) > 0:
                        file_max = str(file_timestamps.max())
                        if max_timestamp is None or file_max > max_timestamp:
                            max_timestamp = file_max
            except:
                continue
        
        snapshot_cutoff = max_timestamp
        if debug:
            if snapshot_cutoff:
                print(f"   📍 JSON snapshot cutoff timestamp: {snapshot_cutoff}")
                print(f"   📁 Detected from {len(snapshot_files)} snapshot file(s)")
            else:
                print(f"   ⚠️  No snapshot cutoff detected (no snapshot files found)")
    
    # Step 2: Read and process changefeed files (Parquet or JSON)
    all_events = []
    json_files = 0
    parquet_files = 0
    
    for file_info in file_list:
        try:
            file_path = file_info['path']
            file_name = file_info['name']
            
            # Skip metadata files and _metadata/ directory contents
            if file_name.startswith('_') or '/_metadata/' in file_path:
                if debug:
                    print(f"   ⏭️  Skipping metadata file: {file_name}")
                continue
            
            # Detect file format
            is_json = file_name.endswith(('.ndjson', '.json'))
            if is_json:
                json_files += 1
            else:
                parquet_files += 1
            
            # Read file based on format
            if is_json:
                df = spark.read.json(file_path)
            else:
                df = spark.read.parquet(file_path)
            
            pdf = df.toPandas()
            records = pdf.to_dict('records')
            
            if debug:
                print(f"   📄 {file_name}: {len(records)} events ({'JSON' if is_json else 'Parquet'})")
            
            # Process rows and extract primary keys correctly
            for record in records:
                # Determine CDC operation and extract data based on format
                if is_json:
                    # JSON FORMAT: Use before/after envelope
                    before = record.get('before')
                    after = record.get('after')
                    updated = record.get('updated', '')
                    
                    # Determine operation from before/after + timestamp
                    if after and not before:
                        # Use timestamp to distinguish SNAPSHOT from INSERT
                        if snapshot_cutoff and updated:
                            # CRITICAL: Compare as floats, not strings
                            # String comparison fails when formats differ:
                            #   - snapshot_cutoff: "1767823574768222906.0000000000" (full precision)
                            #   - str(updated): "1.76782e+18" (scientific) or "1767823574768222906.0" (truncated)
                            # Float comparison works regardless of string format
                            try:
                                updated_float = float(updated)
                                cutoff_float = float(snapshot_cutoff)
                                if updated_float <= cutoff_float:
                                    cdc_operation = 'SNAPSHOT'
                                else:
                                    cdc_operation = 'INSERT'
                            except (ValueError, TypeError):
                                # Fallback: assume SNAPSHOT if conversion fails
                                cdc_operation = 'SNAPSHOT'
                        else:
                            # No cutoff - assume SNAPSHOT
                            cdc_operation = 'SNAPSHOT'
                    elif after and before:
                        cdc_operation = 'UPDATE'
                    elif before and not after:
                        cdc_operation = 'DELETE'
                    else:
                        continue  # Skip invalid records
                    
                    # Extract data from appropriate field
                    if after:
                        row_data = after if isinstance(after, dict) else {}
                    elif before:
                        row_data = before if isinstance(before, dict) else {}
                    else:
                        continue
                    
                    # Extract primary key from top-level 'key' field (always present)
                    key_values = record.get('key', [])
                    cdc_key_pairs = []
                    
                    if key_values and len(key_values) == len(primary_key_columns):
                        # Use top-level 'key' field
                        for pk_col, pk_val in zip(primary_key_columns, key_values):
                            cdc_key_pairs.append((pk_col, pk_val))
                    else:
                        # Fallback: extract from row_data
                        for pk_col in sorted(primary_key_columns):
                            if pk_col in row_data:
                                cdc_key_pairs.append((pk_col, row_data[pk_col]))
                    
                    # Build event
                    event = {
                        **row_data,
                        '_cdc_key': cdc_key_pairs,
                        '_cdc_updated': updated,
                        '_cdc_operation': cdc_operation,
                        '_source_file': file_info['name']
                    }
                    
                else:
                    # PARQUET FORMAT: Use __crdb__event_type
                    event_type = record.get('__crdb__event_type', '')
                    
                    # Parquet: 'c'=upsert (snapshot or CDC), 'd'=delete
                    if event_type == 'c':
                        cdc_operation = 'UPSERT'  # Parquet 'c' = create/update
                    elif event_type == 'd':
                        cdc_operation = 'DELETE'
                    else:
                        cdc_operation = 'UNKNOWN'
                    
                    # Extract ONLY primary key columns for deduplication
                    cdc_key_pairs = []
                    for pk_col in sorted(primary_key_columns):
                        if pk_col in record:
                            cdc_key_pairs.append((pk_col, record[pk_col]))
                    
                    # Build event
                    event = {
                        **record,
                        '_cdc_key': cdc_key_pairs,  # ONLY primary keys
                        '_cdc_updated': record.get('__crdb__updated', ''),
                        '_cdc_operation': cdc_operation,
                        '_source_file': file_info['name']
                    }
                
                all_events.append(event)
            
        except Exception as e:
            # Only print errors for actual data files (not metadata files starting with _)
            if debug and not file_info['name'].startswith('_'):
                print(f"   ⚠️  Error processing {file_info['name']}: {e}")
            continue
    
    if debug:
        print(f"   📊 Total events read: {len(all_events):,}")
        print(f"   📊 Format breakdown: {parquet_files} Parquet, {json_files} JSON")
    
    # Deduplicate using coalescing logic (groups by _cdc_key which is now PK-only)
    # For Parquet: merges column family fragments
    # For JSON: no-op (JSON already has distinct events)
    connector = LakeflowConnect({})
    coalesced_events = connector._coalesce_events_by_key(all_events)
    
    if debug:
        print(f"   📊 After deduplication: {len(coalesced_events):,} events")
    
    # Count operations from coalesced events
    total_stats = {'snapshot': 0, 'insert': 0, 'update': 0, 'delete': 0}
    active_keys = 0  # Count of non-deleted keys
    for event in coalesced_events:
        operation = event.get('_cdc_operation', 'UNKNOWN')
        if operation == 'SNAPSHOT':
            total_stats['snapshot'] += 1
            active_keys += 1
        elif operation == 'INSERT':
            total_stats['insert'] += 1
            active_keys += 1
        elif operation == 'UPDATE' or operation == 'UPSERT':
            # UPSERT from Parquet 'c' (could be snapshot or CDC)
            # UPDATE from JSON 'u' (definitely CDC)
            total_stats['update'] += 1
            active_keys += 1
        elif operation == 'DELETE':
            total_stats['delete'] += 1
            # Don't count deleted keys in active_keys
    
    if debug:
        print(f"   📊 Operation counts: SNAPSHOT={total_stats['snapshot']}, INSERT={total_stats['insert']}, UPDATE={total_stats['update']}, DELETE={total_stats['delete']}")
        print(f"   📊 Active keys (unique): {active_keys:,}")
    
    total_stats['file_count'] = len(file_list)
    total_stats['unique_keys'] = active_keys  # Only non-deleted keys
    return total_stats


def parallel_delete_checkpoint(
    checkpoint_path: str,
    dbutils,
    max_workers: int = 20,
    debug: bool = True
) -> dict:
    """
    Delete checkpoint directory in parallel for faster cleanup.
    
    This function significantly speeds up checkpoint deletion when there are
    hundreds or thousands of subdirectories (common with many test runs).
    
    **Performance:**
    - Sequential deletion: 5-10 minutes for 1000+ subdirs
    - Parallel deletion: 10-60 seconds for 1000+ subdirs
    
    Args:
        checkpoint_path: Path to checkpoint directory (e.g., 'dbfs:/Volumes/.../checkpoints/usertable')
        dbutils: Databricks dbutils instance from parent context
        max_workers: Number of parallel threads (default: 20)
        debug: Enable debug output
        
    Returns:
        Dictionary with deletion statistics:
        {
            'success': bool,
            'deleted_count': int,
            'failed_count': int,
            'elapsed_seconds': float,
            'subdirs': List[str]  # List of deleted subdirectories
        }
        
    Example:
        ```python
        # In a Databricks notebook:
        from cockroachdb import parallel_delete_checkpoint
        
        # Delete checkpoint with parallel processing
        result = parallel_delete_checkpoint(
            checkpoint_path='dbfs:/Volumes/main/schema/volume/_checkpoints/usertable',
            dbutils=dbutils,  # Pass notebook's dbutils
            max_workers=20,
            debug=True
        )
        
        print(f"Deleted {result['deleted_count']} subdirectories in {result['elapsed_seconds']:.1f}s")
        ```
        
    Example (Integrated with load_and_merge_cdc_to_delta):
        ```python
        # Fast checkpoint clearing before test
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        
        parallel_delete_checkpoint(
            checkpoint_path=f"{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}",
            dbutils=dbutils
        )
        ```
    """
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    if debug:
        print("=" * 80)
        print("PARALLEL CHECKPOINT DELETION")
        print("=" * 80)
        print(f"Path: {checkpoint_path}")
        print(f"Workers: {max_workers}")
        print()
    
    start_time = time.time()
    deleted_count = 0
    failed_count = 0
    subdirs = []
    
    try:
        # Check if path exists
        try:
            items = dbutils.fs.ls(checkpoint_path)
        except Exception as e:
            if debug:
                print(f"ℹ️  Checkpoint doesn't exist: {checkpoint_path}")
                print(f"   (This is OK if it's the first run)")
            return {
                'success': True,
                'deleted_count': 0,
                'failed_count': 0,
                'elapsed_seconds': time.time() - start_time,
                'subdirs': []
            }
        
        # Separate directories and files
        # Handle different FileInfo APIs across Databricks runtime versions
        dirs = []
        files = []
        for item in items:
            # Try different ways to check if directory
            is_dir = False
            try:
                is_dir = item.isDir()  # Method call (some versions)
            except (AttributeError, TypeError):
                try:
                    is_dir = item.isDir  # Property (other versions)
                except AttributeError:
                    # Fallback: check if path looks like a directory
                    is_dir = item.path.endswith('/') or '.' not in item.name
            
            if is_dir:
                dirs.append(item.path)
            else:
                files.append(item.path)
        
        if debug:
            print(f"📊 Found:")
            print(f"   Subdirectories: {len(dirs)}")
            print(f"   Files: {len(files)}")
            print()
        
        # Strategy: Recursively collect all leaf directories for parallel deletion
        # Checkpoint structure: checkpoint/delta/state/0/ (level 3 - hundreds of small files)
        #                       checkpoint/schema/...
        # We recursively scan to find ALL leaf directories (up to 3-4 levels deep)
        
        if dirs:
            if debug:
                print(f"🔍 Recursively scanning directories (max 5 levels deep for hierarchical structures)...")
            
            def collect_all_subdirs(dir_path, current_level=1, max_level=5):
                """Recursively collect all leaf subdirectories (directories with no subdirs or at max depth)."""
                subdirs = []
                try:
                    items = dbutils.fs.ls(dir_path)
                    dir_items = []
                    
                    for item in items:
                        is_dir = False
                        try:
                            is_dir = item.isDir()
                        except (AttributeError, TypeError):
                            try:
                                is_dir = item.isDir
                            except AttributeError:
                                is_dir = item.path.endswith('/') or '.' not in item.name
                        
                        if is_dir:
                            dir_items.append(item.path)
                    
                    # If no subdirectories found, this is a leaf - return it
                    if not dir_items:
                        return [dir_path]
                    
                    # If at max level, add all subdirs as leaves (force parallelization)
                    if current_level >= max_level:
                        return dir_items
                    
                    # Otherwise, recurse into each subdirectory
                    for subdir in dir_items:
                        nested = collect_all_subdirs(subdir, current_level + 1, max_level)
                        subdirs.extend(nested)
                        
                except Exception:
                    # If can't list, treat this dir as a leaf
                    subdirs.append(dir_path)
                
                return subdirs
            
            # Collect all leaf directories from all top-level dirs
            all_nested_dirs = []
            for top_dir in dirs:
                top_name = top_dir.rstrip('/').split('/')[-1]
                nested = collect_all_subdirs(top_dir, current_level=1, max_level=5)
                if debug:
                    print(f"   {top_name}/: {len(nested)} leaf directories")
                all_nested_dirs.extend(nested)
            
            if debug:
                print(f"\n   Total: {len(all_nested_dirs)} directories to delete")
                print(f"\n🔥 Deleting {len(all_nested_dirs)} directories in parallel...")
            
            def delete_subdir(path):
                """Delete a subdirectory."""
                try:
                    dbutils.fs.rm(path, True)
                    # Strip trailing slash before getting name
                    name = path.rstrip('/').split('/')[-1]
                    parent = path.rstrip('/').split('/')[-2] if len(path.rstrip('/').split('/')) > 1 else ''
                    display_name = f"{parent}/{name}" if parent else name
                    return {'success': True, 'path': path, 'name': display_name}
                except Exception as e:
                    name = path.rstrip('/').split('/')[-1]
                    parent = path.rstrip('/').split('/')[-2] if len(path.rstrip('/').split('/')) > 1 else ''
                    display_name = f"{parent}/{name}" if parent else name
                    return {'success': False, 'path': path, 'name': display_name, 'error': str(e)}
            
            # Delete all nested directories in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(delete_subdir, dir_path) for dir_path in all_nested_dirs]
                
                for future in as_completed(futures):
                    result = future.result()
                    if result['success']:
                        deleted_count += 1
                        subdirs.append(result['name'])
                        if debug:
                            print(f"   ✅ {result['name']}")
                    else:
                        failed_count += 1
                        if debug:
                            print(f"   ❌ {result['name']}: {result['error']}")
            
            if debug:
                print(f"\n   Deleted {deleted_count} directories")
                if failed_count > 0:
                    print(f"   Failed: {failed_count} directories")
                print()
        
        # Delete remaining files at root level
        if files:
            if debug:
                print(f"🗑️  Deleting {len(files)} files at root level...")
            
            for file_path in files:
                try:
                    dbutils.fs.rm(file_path, False)
                    deleted_count += 1
                except Exception as e:
                    failed_count += 1
                    if debug:
                        print(f"   ⚠️  Failed to delete {file_path.split('/')[-1]}: {e}")
        
        # Finally, try to delete the parent directory
        if debug:
            print(f"🧹 Cleaning up parent directory...")
        
        try:
            dbutils.fs.rm(checkpoint_path, True)
            if debug:
                print(f"   ✅ Deleted parent: {checkpoint_path}")
        except Exception as e:
            if debug:
                print(f"   ℹ️  Parent cleanup: {e}")
        
        elapsed = time.time() - start_time
        
        if debug:
            print()
            print("=" * 80)
            print("DELETION SUMMARY")
            print("=" * 80)
            print(f"✅ Successfully deleted: {deleted_count} items")
            if failed_count > 0:
                print(f"❌ Failed to delete: {failed_count} items")
            print(f"⏱️  Time: {elapsed:.1f}s")
            
            # Show performance comparison
            if len(dirs) > 100:
                sequential_estimate = len(dirs) * 0.5  # Estimate 0.5s per dir sequentially
                speedup = sequential_estimate / elapsed if elapsed > 0 else 1
                print(f"📊 Estimated sequential time: {sequential_estimate:.1f}s")
                print(f"⚡ Speedup: {speedup:.1f}x faster")
            
            print("=" * 80)
        
        return {
            'success': failed_count == 0,
            'deleted_count': deleted_count,
            'failed_count': failed_count,
            'elapsed_seconds': elapsed,
            'subdirs': subdirs
        }
        
    except Exception as e:
        elapsed = time.time() - start_time
        if debug:
            print(f"\n❌ Error during deletion: {e}")
            print(f"⏱️  Time before error: {elapsed:.1f}s")
        
        return {
            'success': False,
            'deleted_count': deleted_count,
            'failed_count': failed_count + 1,
            'elapsed_seconds': elapsed,
            'subdirs': subdirs,
            'error': str(e)
        }


class VolumePathComponents:
    """
    Container for parsed volume path components.
    
    This class provides explicit, named access to volume path parts instead of
    using tuples, making the code more readable and maintainable.
    
    Path Structure:
        /Volumes/{unity_catalog}/{unity_schema}/{volume_name}/{format}/{crdb_catalog}/{crdb_schema}/{scenario}/{timestamp?}
        
        Example:
        /Volumes/main/robert_lee_cockroachdb/parquet_files/parquet/defaultdb/public/test-parquet_usertable_no_split/1767895046
        │        │    │                       │             │       │         │      │                                  │
        │        │    │                       │             │       │         │      │                                  └─ timestamp (optional)
        │        │    │                       │             │       │         │      └─ scenario
        │        │    │                       │             │       │         └─ crdb_schema
        │        │    │                       │             │       └─ crdb_catalog
        │        │    │                       │             └─ format
        │        │    │                       └─ volume_name
        │        │    └─ unity_schema
        │        └─ unity_catalog
        └─ Volumes prefix
    
    Attributes:
        volume_base: Path up to and including the volume name
                    Example: 'dbfs:/Volumes/catalog/schema/volume'
        path_prefix: Path after volume, without timestamp
                    Example: 'format/catalog/schema/test-scenario'
        timestamp: The timestamp if present, None otherwise
                  Example: '1767895046' or None
    
    Properties:
        full_path: Reconstructed full path including timestamp if present
        has_timestamp: Boolean indicating if timestamp is present
        format_type: Data format extracted from path (e.g., 'parquet', 'json')
        crdb_catalog: CockroachDB catalog/database name (e.g., 'defaultdb')
        crdb_schema: CockroachDB schema name (e.g., 'public')
        scenario: Test scenario name (e.g., 'test-parquet_usertable_no_split')
    """
    
    def __init__(self, volume_base: str, path_prefix: str, timestamp: str = None):
        self.volume_base = volume_base
        self.path_prefix = path_prefix
        self.timestamp = timestamp
        
        # Parse path_prefix into components for easy access
        # Expected format: format/catalog/schema/scenario
        self._path_parts = path_prefix.rstrip('/').split('/')
    
    @property
    def full_path(self) -> str:
        """Reconstruct the full path including timestamp if present."""
        if self.timestamp:
            return f"{self.volume_base}/{self.path_prefix}/{self.timestamp}"
        else:
            return f"{self.volume_base}/{self.path_prefix}"
    
    @property
    def has_timestamp(self) -> bool:
        """Check if this path includes a timestamp."""
        return self.timestamp is not None
    
    @property
    def format_type(self) -> str:
        """
        Extract format type from path.
        
        Returns:
            Format type (e.g., 'parquet', 'json')
            
        Raises:
            ValueError: If path doesn't have enough components
        """
        if len(self._path_parts) < 1:
            raise ValueError(f"Path prefix '{self.path_prefix}' doesn't contain format")
        return self._path_parts[0]
    
    @property
    def crdb_catalog(self) -> str:
        """
        Extract CockroachDB catalog/database name from path.
        
        Returns:
            Catalog name (e.g., 'defaultdb')
            
        Raises:
            ValueError: If path doesn't have enough components
        """
        if len(self._path_parts) < 2:
            raise ValueError(f"Path prefix '{self.path_prefix}' doesn't contain catalog")
        return self._path_parts[1]
    
    @property
    def crdb_schema(self) -> str:
        """
        Extract CockroachDB schema name from path.
        
        Returns:
            Schema name (e.g., 'public')
            
        Raises:
            ValueError: If path doesn't have enough components
        """
        if len(self._path_parts) < 3:
            raise ValueError(f"Path prefix '{self.path_prefix}' doesn't contain schema")
        return self._path_parts[2]
    
    @property
    def scenario(self) -> str:
        """
        Extract test scenario name from path.
        
        Returns:
            Scenario name (e.g., 'test-parquet_usertable_no_split')
            
        Raises:
            ValueError: If path doesn't have enough components
        """
        if len(self._path_parts) < 4:
            raise ValueError(f"Path prefix '{self.path_prefix}' doesn't contain scenario")
        return self._path_parts[3]
    
    def __repr__(self) -> str:
        return f"VolumePathComponents(volume_base='{self.volume_base}', path_prefix='{self.path_prefix}', timestamp={self.timestamp})"
    
    def __str__(self) -> str:
        return self.full_path


def parse_volume_path(volume_path: str) -> VolumePathComponents:
    """
    Parse a volume path into components (volume_base, path_prefix, timestamp).
    
    This centralizes the logic for splitting Unity Catalog volume paths to avoid
    code duplication across functions. Handles both timestamped and non-timestamped paths.
    
    Args:
        volume_path: Full volume path to parse
                    Examples:
                    - Without timestamp: 'dbfs:/Volumes/catalog/schema/volume/format/catalog/schema/test-scenario'
                    - With timestamp: 'dbfs:/Volumes/catalog/schema/volume/format/catalog/schema/test-scenario/1767895046'
    
    Returns:
        VolumePathComponents object with attributes:
        - volume_base: Path up to and including the volume name
                      Example: 'dbfs:/Volumes/catalog/schema/volume'
        - path_prefix: Path after volume, WITHOUT timestamp
                      Example: 'format/catalog/schema/test-scenario'
        - timestamp: The timestamp if present, None otherwise
                    Example: '1767895046' or None
        - has_timestamp: Boolean property indicating if timestamp is present
        - full_path: Property that reconstructs the full path
    
    Raises:
        ValueError: If the path format is invalid
    
    Examples:
        ```python
        # Without timestamp
        components = parse_volume_path(
            'dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-scenario'
        )
        print(components.volume_base)  # 'dbfs:/Volumes/main/schema/volume'
        print(components.path_prefix)  # 'json/defaultdb/public/test-scenario'
        print(components.timestamp)    # None
        print(components.has_timestamp) # False
        
        # With timestamp
        components = parse_volume_path(
            'dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-scenario/1767895046'
        )
        print(components.volume_base)   # 'dbfs:/Volumes/main/schema/volume'
        print(components.path_prefix)   # 'json/defaultdb/public/test-scenario'
        print(components.timestamp)     # '1767895046'
        print(components.has_timestamp) # True
        print(components.full_path)     # Full reconstructed path
        ```
    """
    path_parts = volume_path.rstrip('/').split('/')
    
    # Find the /Volumes/.../ part
    volumes_index = -1
    for i, part in enumerate(path_parts):
        if part == 'Volumes' and i > 0:
            volumes_index = i - 1  # Include the protocol part (e.g., 'dbfs:')
            break
    
    if volumes_index < 0:
        raise ValueError(
            f"Invalid volume_path format: {volume_path}\n"
            f"Expected: dbfs:/Volumes/catalog/schema/volume/path/prefix\n"
            f"The path must contain '/Volumes/' segment."
        )
    
    # Volume base is everything up to and including the volume name
    # Structure: protocol / 'Volumes' / catalog / schema / volume
    # That's: volumes_index + 1 (for 'Volumes') + 3 (for catalog/schema/volume) = volumes_index + 4
    volume_base_end = volumes_index + 1 + 4
    
    if len(path_parts) <= volume_base_end:
        raise ValueError(
            f"Invalid volume_path: missing path after volume name\n"
            f"Path: {volume_path}\n"
            f"Expected: dbfs:/Volumes/catalog/schema/volume/format/catalog/schema/test-scenario\n"
            f"Found only {len(path_parts)} parts, need at least {volume_base_end + 1}"
        )
    
    volume_base = '/'.join(path_parts[:volume_base_end])
    
    # Extract path prefix parts (everything after volume)
    prefix_parts = path_parts[volume_base_end:]
    
    # Check if last part is a timestamp (10-digit number)
    timestamp = None
    if prefix_parts and prefix_parts[-1].isdigit() and len(prefix_parts[-1]) == 10:
        timestamp = prefix_parts[-1]
        prefix_parts = prefix_parts[:-1]  # Remove timestamp from prefix
    
    path_prefix = '/'.join(prefix_parts)
    
    return VolumePathComponents(volume_base, path_prefix, timestamp)


class TestScenarioComponents:
    """
    Container for parsed test scenario components.
    
    This class provides explicit, named access to test scenario parts instead of
    using inline parsing, making the code more readable and maintainable.
    
    Test scenarios follow the naming convention from test_cdc_matrix.sh:
        test-{format}_{table_name}_{split_info}
    
    Examples:
        - test-parquet_simple_test_no_split
        - test-json_usertable_with_split
        - test-parquet_usertable_no_split
    
    Attributes:
        scenario_name: Original scenario name (e.g., 'test-parquet_simple_test_no_split')
        format: Data format ('parquet' or 'json')
        table_name: Table name (e.g., 'simple_test', 'usertable')
        has_split: Whether column families are split (True/False)
    
    Note:
        Catalog/schema are NOT part of the scenario name. They come from:
        - Defaults ('defaultdb'/'public')
        - Configuration files
        - Volume path structure
    """
    
    def __init__(
        self,
        scenario_name: str,
        format: str,
        table_name: str,
        has_split: bool
    ):
        self.scenario_name = scenario_name
        self.format = format
        self.table_name = table_name
        self.has_split = has_split
    
    @property
    def split_info(self) -> str:
        """Return split info as string ('with_split' or 'no_split')."""
        return 'with_split' if self.has_split else 'no_split'
    
    def __repr__(self) -> str:
        return (f"TestScenarioComponents(scenario_name='{self.scenario_name}', "
                f"format='{self.format}', table_name='{self.table_name}', "
                f"has_split={self.has_split})")
    
    def __str__(self) -> str:
        return self.scenario_name


def parse_test_scenario(scenario_name: str) -> TestScenarioComponents:
    """
    Parse a test scenario name into components.
    
    This centralizes the logic for parsing test_cdc_matrix.sh scenario names to avoid
    code duplication across notebooks and scripts.
    
    Test scenarios follow the naming convention:
        test-{format}_{table_name}_{split_info}
    
    Where:
        - format: 'parquet' or 'json'
        - table_name: Can be multi-word (e.g., 'simple_test', 'usertable')
        - split_info: 'with_split' or 'no_split'
    
    Args:
        scenario_name: Test scenario name (e.g., 'test-parquet_simple_test_no_split')
    
    Returns:
        TestScenarioComponents object with attributes:
        - scenario_name: Original scenario name
        - format: Data format ('parquet' or 'json')
        - table_name: Extracted table name (e.g., 'simple_test')
        - has_split: Whether column families are split (True/False)
        - split_info: Split info as string property ('with_split' or 'no_split')
    
    Note:
        Catalog/schema are NOT parsed from scenario name. Get them from:
        - Defaults: 'defaultdb'/'public'
        - Configuration files
        - Volume path structure (/Volumes/.../format/catalog/schema/...)
    
    Raises:
        ValueError: If the scenario name format is invalid
    
    Examples:
        ```python
        # Parse a Parquet scenario
        scenario = parse_test_scenario('test-parquet_simple_test_no_split')
        print(scenario.format)      # 'parquet'
        print(scenario.table_name)  # 'simple_test'
        print(scenario.has_split)   # False
        print(scenario.split_info)  # 'no_split'
        
        # Parse a JSON scenario with column families
        scenario = parse_test_scenario('test-json_usertable_with_split')
        print(scenario.format)      # 'json'
        print(scenario.table_name)  # 'usertable'
        print(scenario.has_split)   # True
        print(scenario.split_info)  # 'with_split'
        ```
    
    Integration Example:
        ```python
        # In a notebook:
        from cockroachdb import parse_test_scenario
        
        TEST_SCENARIO = "test-parquet_simple_test_no_split"
        
        # Parse scenario
        scenario = parse_test_scenario(TEST_SCENARIO)
        
        # Extract components
        SOURCE_TABLE = scenario.table_name  # 'simple_test'
        
        # Catalog/schema come from configuration or defaults
        CRDB_CATALOG = "defaultdb"  # Not in scenario name
        CRDB_SCHEMA = "public"      # Not in scenario name
        
        print(f"Testing {scenario.format} format")
        print(f"Table: {SOURCE_TABLE}")
        print(f"Split: {scenario.split_info}")
        ```
    """
    # Remove 'test-' prefix if present
    if scenario_name.startswith('test-'):
        scenario_name_clean = scenario_name[5:]  # Remove 'test-'
    else:
        scenario_name_clean = scenario_name
    
    # Split by underscore
    # Expected format: {format}_{table_name}_{split_word1}_{split_word2}
    # Examples:
    #   parquet_simple_test_no_split → ['parquet', 'simple', 'test', 'no', 'split']
    #   json_usertable_with_split → ['json', 'usertable', 'with', 'split']
    parts = scenario_name_clean.split('_')
    
    if len(parts) < 4:
        raise ValueError(
            f"Invalid test scenario format: '{scenario_name}'\n"
            f"Expected: test-{{format}}_{{table}}_{{split_info}}\n"
            f"Examples:\n"
            f"  - test-parquet_simple_test_no_split\n"
            f"  - test-json_usertable_with_split\n"
            f"  - test-parquet_usertable_no_split\n"
            f"\n"
            f"Found only {len(parts)} parts after splitting by '_', need at least 4"
        )
    
    # Extract format (first part)
    format_str = parts[0]
    if format_str not in ['parquet', 'json']:
        raise ValueError(
            f"Invalid format in scenario: '{format_str}'\n"
            f"Expected 'parquet' or 'json'\n"
            f"Scenario: {scenario_name}"
        )
    
    # Extract split info (last 2 parts: 'with_split' or 'no_split')
    split_word1 = parts[-2]
    split_word2 = parts[-1]
    split_info_str = f"{split_word1}_{split_word2}"
    
    if split_info_str == 'with_split':
        has_split = True
    elif split_info_str == 'no_split':
        has_split = False
    else:
        raise ValueError(
            f"Invalid split info in scenario: '{split_info_str}'\n"
            f"Expected 'with_split' or 'no_split'\n"
            f"Scenario: {scenario_name}"
        )
    
    # Extract table name (middle parts between format and split info)
    # Parts: [format, table_part1, table_part2, ..., split_word1, split_word2]
    # Table name: parts[1:-2]
    table_parts = parts[1:-2]
    
    if not table_parts:
        raise ValueError(
            f"Could not extract table name from scenario: '{scenario_name}'\n"
            f"Expected format: test-{{format}}_{{table}}_{{split_info}}\n"
            f"Make sure the table name is between format and split info"
        )
    
    table_name = '_'.join(table_parts)
    
    return TestScenarioComponents(
        scenario_name=scenario_name,
        format=format_str,
        table_name=table_name,
        has_split=has_split
    )


def get_timestamped_path(
    volume_base: str,
    path_prefix: str,
    version: int = 0,
    dbutils = None
) -> str:
    """
    Get timestamped path for a test scenario based on established conventions.
    
    With timestamp-based path isolation introduced in test_cdc_matrix.sh (line 343),
    each test run writes to a unique directory with a timestamp:
        {format}/{catalog}/{schema}/test-{test_name}/{timestamp}/
    
    This method helps find the right timestamped path for a given test scenario.
    
    Path Convention:
        {volume_base}/{format}/{catalog}/{schema}/test-{test_name}/{timestamp}/
    
    Example Structure:
        dbfs:/Volumes/main/schema/volume/
        ├── json/defaultdb/public/test-json_usertable_with_split/
        │   ├── 1767823340/  ← version=0 (latest)
        │   ├── 1767823100/  ← version=1 (2nd newest)
        │   └── 1767822800/  ← version=-1 (oldest)
        └── parquet/defaultdb/public/test-parquet_usertable_no_split/
            ├── 1767823500/
            └── 1767823200/
    
    Args:
        volume_base: Base volume path (e.g., 'dbfs:/Volumes/catalog/schema/volume')
        path_prefix: Path prefix without timestamp 
                    (e.g., 'json/defaultdb/public/test-json_usertable_with_split')
        version: Which version to return:
                 0 = latest/newest (default)
                 1 = second newest
                 2 = third newest, etc.
                 -1 = oldest
                 -2 = second oldest, etc.
        dbutils: DBUtils instance (REQUIRED)
    
    Returns:
        Full path with timestamp, e.g.:
        'dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_usertable_with_split/1767823340'
    
    Raises:
        ValueError: If no timestamped directories found or version out of range
        RuntimeError: If dbutils not available
    
    Example:
        ```python
        from cockroachdb import get_timestamped_path
        
        # Get latest test run
        latest_path = get_timestamped_path(
            volume_base='dbfs:/Volumes/main/schema/volume',
            path_prefix='json/defaultdb/public/test-json_usertable_with_split',
            version=0,
            dbutils=dbutils
        )
        # Returns: .../test-json_usertable_with_split/1767823340
        
        # Get oldest test run
        oldest_path = get_timestamped_path(
            volume_base='dbfs:/Volumes/main/schema/volume',
            path_prefix='json/defaultdb/public/test-json_usertable_with_split',
            version=-1,
            dbutils=dbutils
        )
        # Returns: .../test-json_usertable_with_split/1767822800
        
        # Get second newest
        second_path = get_timestamped_path(
            volume_base='dbfs:/Volumes/main/schema/volume',
            path_prefix='json/defaultdb/public/test-json_usertable_with_split',
            version=1,
            dbutils=dbutils
        )
        ```
    
    Integration Example:
        ```python
        # Use with analyze_volume_changefeed_files
        from cockroachdb import get_timestamped_path, analyze_volume_changefeed_files
        
        # Get latest test run path
        test_path = get_timestamped_path(
            volume_base='dbfs:/Volumes/main/schema/volume',
            path_prefix='json/defaultdb/public/test-json_usertable_with_split',
            dbutils=dbutils
        )
        
        # Analyze it
        stats = analyze_volume_changefeed_files(
            test_path,
            primary_key_columns=['ycsb_key'],
            dbutils=dbutils
        )
        ```
    """
    if dbutils is None:
        raise RuntimeError(
            "DBUtils is required for listing volume directories.\n"
            "Pass 'dbutils' parameter from your notebook.\n"
            "Example: get_timestamped_path(..., dbutils=dbutils)"
        )
    
    # Construct full prefix path
    full_prefix = f"{volume_base.rstrip('/')}/{path_prefix.strip('/')}"
    
    try:
        # List timestamped subdirectories
        # Add trailing slash to ensure we're listing the directory contents
        prefix_to_list = full_prefix if full_prefix.endswith('/') else f"{full_prefix}/"
        items = dbutils.fs.ls(prefix_to_list)
    except Exception as e:
        raise ValueError(
            f"Path prefix not found: {full_prefix}\n"
            f"Error: {e}\n\n"
            f"Make sure the path follows the convention:\n"
            f"  {{format}}/{{catalog}}/{{schema}}/test-{{test_name}}\n"
            f"Example: 'json/defaultdb/public/test-json_usertable_with_split'"
        )
    
    # Filter for timestamp directories (numeric names)
    timestamp_dirs = []
    debug_items = []
    
    for item in items:
        # Extract name from path or name attribute
        # item.name can be empty or just '/' for some implementations
        # Fallback to extracting from path if name is empty
        dir_name = item.name.rstrip('/') if item.name else ''
        
        # If name is empty, extract from path
        if not dir_name:
            # Extract last component from path
            # Example: '/Volumes/.../test-scenario/1769022634/' -> '1769022634'
            path_parts = item.path.rstrip('/').split('/')
            dir_name = path_parts[-1] if path_parts else ''
        
        # Debug: Track all items for diagnostics (but limit to first 20 for readability)
        if len(debug_items) < 20:
            debug_items.append({
                'name': dir_name,
                'path': item.path,
                'is_numeric': dir_name.isdigit(),
                'length': len(dir_name)
            })
        
        # Quick filter: Only process items with numeric names (10 digits = timestamp)
        if not (dir_name.isdigit() and len(dir_name) == 10):
            continue
        
        # Check if directory (handle different FileInfo implementations)
        # Priority order: isDir() method > path ends with '/' > try to list it
        is_dir = False
        try:
            # Try calling/accessing isDir
            is_dir = item.isDir() if callable(item.isDir) else item.isDir
        except (AttributeError, TypeError):
            # Fallback 1: Check if path ends with '/'
            if item.path.endswith('/'):
                is_dir = True
            else:
                # Fallback 2: Try to list it (will fail if it's a file)
                try:
                    dbutils.fs.ls(item.path)
                    is_dir = True
                except:
                    # It's a file, not a directory
                    is_dir = False
        
        if is_dir:
            timestamp_dirs.append({
                'name': dir_name,
                'path': item.path.rstrip('/'),  # Normalize path
                'timestamp': int(dir_name)
            })
    
    if not timestamp_dirs:
        # No timestamp directories found - provide detailed diagnostics
        debug_info = "\n".join([f"  - {item['name']}: is_numeric={item['is_numeric']}, length={item['length']}, path={item['path']}" 
                                for item in debug_items])  # Show all debug items
        
        raise ValueError(
            f"No timestamped directories found in: {full_prefix}\n\n"
            f"Expected directory structure:\n"
            f"  {full_prefix}/\n"
            f"  ├── 1769022634/  (10-digit Unix timestamp directories)\n"
            f"  ├── 1769022500/\n"
            f"  └── 1769022000/\n\n"
            f"Found {len(debug_items)} items in parent directory:\n{debug_info}\n\n"
            f"Possible causes:\n"
            f"  1. Test data not yet synced - run: test_cdc_matrix.sh\n"
            f"  2. Wrong path format - check path_prefix parameter\n"
            f"  3. Items have empty names - code now extracts from path (should be fixed)\n"
            f"  4. Items not recognized as directories by dbutils.fs.ls()\n\n"
            f"To debug further, try in notebook:\n"
            f"  items = dbutils.fs.ls('{prefix_to_list}')\n"
            f"  print(f'Total items: {{len(items)}}')\n"
            f"  for item in items[:10]:\n"
            f"      name = item.name.rstrip('/') if item.name else item.path.rstrip('/').split('/')[-1]\n"
            f"      print(f'  name={{name!r}}, isDir={{item.isDir()}}, path={{item.path}}')\n"
        )
    
    # Sort by timestamp (newest first)
    timestamp_dirs.sort(key=lambda x: x['timestamp'], reverse=True)
    
    # Select version
    if version >= 0:
        # Positive version: 0=newest, 1=2nd newest, etc.
        if version >= len(timestamp_dirs):
            available = len(timestamp_dirs)
            raise ValueError(
                f"Version {version} not found. Only {available} version(s) available.\n"
                f"Available versions: 0 to {available - 1} (0=newest, {available - 1}=oldest)\n"
                f"Timestamps: {[d['name'] for d in timestamp_dirs]}"
            )
        selected = timestamp_dirs[version]
    else:
        # Negative version: -1=oldest, -2=2nd oldest, etc.
        abs_version = abs(version)
        if abs_version > len(timestamp_dirs):
            available = len(timestamp_dirs)
            raise ValueError(
                f"Version {version} not found. Only {available} version(s) available.\n"
                f"Available versions: -1 to -{available} (-1=oldest, -{available}=newest)\n"
                f"Timestamps: {[d['name'] for d in reversed(timestamp_dirs)]}"
            )
        # Reverse index for negative versions
        selected = timestamp_dirs[-(abs_version)]
    
    return selected['path'].rstrip('/')


def merge_column_family_fragments(
    df,
    primary_key_columns: List[str],
    metadata_columns: List[str] = None,
    debug: bool = False,
    is_streaming: bool = None,
    deduplicate_to_latest_state: bool = False
):
    """
    Merge column family fragments into complete rows.
    
    When split_column_families=true, CockroachDB writes one Parquet file per column family,
    resulting in multiple fragment records per logical row. This function merges these
    fragments by grouping on primary key and taking the first non-null value for each column.
    
    **Streaming vs Batch Mode:**
    - **Streaming DataFrames** (from Autoloader): Always applies merge (can't detect beforehand)
    - **Batch DataFrames** (from spark.read): Auto-detects fragmentation, skips if not needed
    - Set `is_streaming=True` to force streaming mode (skips detection)
    - Set `is_streaming=False` to force batch mode (enables detection)
    
    **Deduplication Mode (NEW):**
    - `deduplicate_to_latest_state=False` (default): Merges fragments within same event, preserves all events
    - `deduplicate_to_latest_state=True`: Coalesces columns across time + deduplicates to latest state
      * Use when you have multiple UPDATE events for the same key
      * Preserves old values for columns not touched by newer events
      * Example: Event1 has field3=3, Event2 updates field0 but leaves field3=NULL
        → Result keeps field3=3 from Event1 (not NULL from Event2)
    
    Args:
        df: Spark DataFrame with potential column family fragments
        primary_key_columns: List of primary key column names (e.g., ['ycsb_key'])
        metadata_columns: Optional list of metadata columns to preserve
                         (default: __crdb__*, _cdc_*, _source_*, _rescued_data)
        debug: Enable debug output showing merge statistics
        is_streaming: Optional boolean to force streaming/batch mode
                     (default: auto-detect based on df.isStreaming)
        deduplicate_to_latest_state: If True, coalesce columns across time and deduplicate to latest row
                                    (default: False - preserves all CDC events)
        
    Returns:
        Merged Spark DataFrame with complete rows
        
    Example (Standard Mode - Preserves All Events):
        ```python
        from cockroachdb import merge_column_family_fragments
        
        # Read batch data
        df_raw = spark.read.parquet("dbfs:/Volumes/catalog/schema/volume")
        
        # Merge - auto-detects fragmentation, preserves all CDC events
        df_merged = merge_column_family_fragments(
            df_raw,
            primary_key_columns=['ycsb_key'],
            debug=True
        )
        ```
        
    Example (Streaming):
        ```python
        # Read streaming data (Autoloader)
        df_raw = spark.readStream.format("cloudFiles").load(...)
        
        # Add transformations
        df_transformed = df_raw.withColumn(...)
        
        # Merge - always applies (can't detect on streaming)
        df_merged = merge_column_family_fragments(
            df_transformed,
            primary_key_columns=['ycsb_key'],
            debug=True
        )
        
        # Write to Delta
        df_merged.writeStream.toTable(...)
        ```
        
    Example (Deduplication Mode - Latest State with Value Preservation):
        ```python
        # For staging → target MERGE scenarios where you want latest state
        # and need to preserve old column values when newer events don't touch them
        
        # Read staging data (may have multiple UPDATE events per key)
        df_staging = spark.read.table("staging_table")
        
        # Merge + deduplicate to latest state (preserves old values)
        df_latest = merge_column_family_fragments(
            df_staging,
            primary_key_columns=['ycsb_key'],
            deduplicate_to_latest_state=True,  # ← NEW MODE
            debug=True
        )
        
        # Result: Latest row per key with all column values preserved
        # - field0 from latest event where field0 was updated
        # - field3 from earlier event (if latest event didn't touch field3)
        ```
        
    **Technical Details:**
    
    *Standard Mode (default):*
    - Uses `first(col, ignorenulls=True)` to combine NULL values from different fragments
    - Each fragment has the PK + data for ONE column family (other columns are NULL)
    - Groups by PK + timestamp + operation to preserve ALL CDC events
    - For non-split tables, this is a harmless no-op (groupBy preserves all data)
    
    *Deduplication Mode (deduplicate_to_latest_state=True):*
    - Uses `last(col, ignorenulls=True)` over window to coalesce columns across time
    - Then deduplicates to keep only the latest row per PK
    - Preserves old values for columns not touched by newer UPDATE events
    - Essential for handling CockroachDB column families with partial updates
    
    **Performance:**
    - Requires a shuffle operation (groupBy)
    - For large datasets, consider repartitioning by PK first
    - Adaptive Query Execution (AQE) helps optimize automatically
    """
    from pyspark.sql import functions as F
    
    # Auto-detect streaming mode if not explicitly set
    if is_streaming is None:
        is_streaming = df.isStreaming
    
    # Default metadata columns to preserve
    if metadata_columns is None:
        metadata_columns = [
            '__crdb__event_type', '__crdb__updated', '_rescued_data',
            '_cdc_operation', '_cdc_timestamp', '_cdc_updated', '_source_file', '_processing_time',
            '_metadata',  # Unity Catalog metadata
            # JSON envelope columns (should not be merged as data columns)
            'after', 'before', 'key', 'updated',
            # Debug columns
            '_after_json', '_before_json', '_debug_after_first_10', '_debug_before_first_10'
        ]
    
    # Get all columns from DataFrame
    all_columns = df.columns
    
    # Identify data columns (everything except PK and metadata)
    data_columns = [
        col for col in all_columns 
        if col not in primary_key_columns and col not in metadata_columns
        and not col.startswith('__crdb__')
        and not col.startswith('_cdc_')
        and not col.startswith('_source_')
        and not col.startswith('_rescued_')
    ]
    
    if debug:
        mode_str = "Streaming" if is_streaming else "Batch"
        print(f"\n🔍 Column Family Merge ({mode_str} Mode)")
        print(f"   Primary key columns: {primary_key_columns}")
        print(f"   Metadata columns: {len(metadata_columns)} columns")
        print(f"   Data columns: {len(data_columns)} columns")
        if len(data_columns) <= 10:
            print(f"     {data_columns}")
        else:
            print(f"     {data_columns[:5]}... (showing first 5)")
        
        # DIAGNOSTIC: If no data columns, show what we have
        if len(data_columns) == 0:
            print(f"\n⚠️  WARNING: No data columns found!")
            print(f"   All columns in DataFrame: {all_columns}")
            print(f"   Metadata columns list: {metadata_columns}")
    
    # For batch mode: Check if merge is needed
    # For streaming mode: Always merge (can't count streaming DataFrames)
    if not is_streaming:
        try:
            # Determine timestamp column for fragmentation detection
            timestamp_col_for_check = None
            if '_cdc_timestamp' in all_columns:
                timestamp_col_for_check = '_cdc_timestamp'
            elif '_cdc_updated' in all_columns:
                timestamp_col_for_check = '_cdc_updated'
            elif '__crdb__updated' in all_columns:
                timestamp_col_for_check = '__crdb__updated'
            elif 'updated' in all_columns:
                timestamp_col_for_check = 'updated'
            
            # Try to detect fragmentation
            total_rows = df.count()
            
            if timestamp_col_for_check and '_cdc_operation' in all_columns:
                # Check for fragmentation at (PK + timestamp + operation) level
                # This preserves all distinct CDC events (same key can have UPDATE + DELETE at same timestamp)
                unique_events = df.select(primary_key_columns + [timestamp_col_for_check, '_cdc_operation']).distinct().count()
            elif timestamp_col_for_check:
                # Fallback: check at (PK + timestamp) level
                unique_events = df.select(primary_key_columns + [timestamp_col_for_check]).distinct().count()
            elif '_cdc_operation' in all_columns:
                # Fallback: check at (PK + operation) level
                unique_events = df.select(primary_key_columns + ['_cdc_operation']).distinct().count()
            else:
                # Fallback: check at PK level only
                unique_events = df.select(primary_key_columns).distinct().count()
            
            if debug:
                print(f"\n📊 Fragmentation Detection:")
                print(f"   Total rows: {total_rows:,}")
                if timestamp_col_for_check and '_cdc_operation' in all_columns:
                    print(f"   Unique events (PK + timestamp + operation): {unique_events:,}")
                elif timestamp_col_for_check:
                    print(f"   Unique events (PK + timestamp): {unique_events:,}")
                elif '_cdc_operation' in all_columns:
                    print(f"   Unique events (PK + operation): {unique_events:,}")
                else:
                    print(f"   Unique keys (PK only): {unique_events:,}")
                print(f"   Duplication ratio: {total_rows / unique_events if unique_events > 0 else 0:.1f}x")
            
            # If no duplicates, return original DataFrame
            if total_rows == unique_events:
                if debug:
                    print(f"\n✅ No column family fragmentation detected")
                    print(f"   Returning original DataFrame unchanged")
                return df
            
            if debug:
                print(f"\n🔧 Column family fragmentation detected!")
                print(f"   Merging {total_rows:,} fragments into {unique_events:,} distinct CDC events...")
        except Exception as e:
            # If detection fails (e.g., actually streaming), proceed with merge
            if debug:
                print(f"\n⚠️  Detection failed (treating as streaming): {e}")
                print(f"   Proceeding with merge...")
    else:
        if debug:
            print(f"\n🔧 Streaming mode: Applying merge")
            print(f"   (Cannot detect fragmentation in streaming DataFrames)")
            print(f"   - If column families exist: fragments will be merged")
            print(f"   - If no column families: merge is harmless no-op")
    
    # ============================================================================
    # DEDUPLICATION MODE: Coalesce columns across time + deduplicate to latest state
    # ============================================================================
    if deduplicate_to_latest_state:
        from pyspark.sql.window import Window
        
        if debug:
            print(f"\n🔄 Applying cross-time coalescing + deduplication...")
            print(f"   (Preserves latest non-NULL value per column across all events)")
        
        # Determine timestamp column for ordering
        timestamp_col_for_coalesce = None
        if '_cdc_timestamp' in all_columns:
            timestamp_col_for_coalesce = '_cdc_timestamp'
        elif '_cdc_updated' in all_columns:
            timestamp_col_for_coalesce = '_cdc_updated'
        elif '__crdb__updated' in all_columns:
            timestamp_col_for_coalesce = '__crdb__updated'
        elif 'updated' in all_columns:
            timestamp_col_for_coalesce = 'updated'
        
        if not timestamp_col_for_coalesce:
            if debug:
                print(f"   ⚠️  No timestamp column found - cannot coalesce across time")
                print(f"   Falling back to standard merge mode")
        else:
            # Step 1: Coalesce columns across time (keep latest non-NULL value per column)
            if debug:
                print(f"   Step 1: Coalescing columns by primary keys: {primary_key_columns}...")
                print(f"           Using last_value(col, ignorenulls=True) per column")
            
            # Window spec: partition by PK, order by timestamp, look at all rows
            window_spec_coalesce = (Window.partitionBy(*primary_key_columns)
                .orderBy(F.col(timestamp_col_for_coalesce))
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
            
            # For each data column, coalesce to latest non-NULL value
            for col in data_columns:
                df = df.withColumn(
                    col,
                    F.last(F.col(col), ignorenulls=True).over(window_spec_coalesce)
                )
            
            # Also coalesce _cdc_operation to the LATEST value (for DELETE handling)
            if '_cdc_operation' in all_columns:
                df = df.withColumn(
                    "_cdc_operation",
                    F.last(F.col("_cdc_operation"), ignorenulls=True).over(window_spec_coalesce)
                )
            
            if debug:
                print(f"   ✅ Columns coalesced (latest non-NULL value per column)")
            
            # Step 2: Deduplicate by primary key (keep LATEST row, which now has ALL coalesced columns)
            if debug:
                print(f"   Step 2: Deduplicating by primary keys: {primary_key_columns}...")
            
            window_spec_dedup = Window.partitionBy(*primary_key_columns).orderBy(F.col(timestamp_col_for_coalesce).desc())
            df_merged = (df
                .withColumn("_row_num", F.row_number().over(window_spec_dedup))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )
            
            if debug:
                print(f"   ✅ Deduplication complete!")
                print(f"      Result: Latest state per primary key with all column values preserved")
            
            return df_merged
    
    # ============================================================================
    # STANDARD MODE: Merge fragments within events, preserve all CDC events
    # ============================================================================
    
    # Build aggregation expressions for merging column family fragments
    # CRITICAL: For CDC data, we must preserve ALL events for a key (SNAPSHOT, UPDATE, DELETE)
    # So we group by BOTH primary_key AND timestamp to:
    #   1. Merge fragments WITHIN the same CDC event (same PK + same timestamp)
    #   2. Preserve ALL CDC events for a key (different timestamps)
    agg_exprs = []
    
    # Determine which timestamp column to use for grouping
    timestamp_col = None
    if '_cdc_timestamp' in all_columns:
        timestamp_col = '_cdc_timestamp'
    elif '_cdc_updated' in all_columns:
        timestamp_col = '_cdc_updated'
    elif '__crdb__updated' in all_columns:
        timestamp_col = '__crdb__updated'
    elif 'updated' in all_columns:
        timestamp_col = 'updated'
    
    # Determine grouping columns
    if timestamp_col and '_cdc_operation' in all_columns:
        # Group by PK + timestamp + operation to preserve all distinct CDC events
        # This is critical: same key can have UPDATE and DELETE at same timestamp!
        group_by_cols = primary_key_columns + [timestamp_col, '_cdc_operation']
        
        # For aggregation, use first() with ignorenulls to combine NULL values from fragments
        # (Each fragment has data for ONE column family, other families are NULL)
        for col in data_columns:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        
        # Metadata columns: also use first()
        # Don't aggregate columns that are already in the grouping key
        for col in metadata_columns:
            if col in all_columns and col not in group_by_cols:
                agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    elif timestamp_col:
        # Fallback: Group by PK + timestamp only
        group_by_cols = primary_key_columns + [timestamp_col]
        
        # For aggregation, use first() with ignorenulls to combine NULL values from fragments
        # (Each fragment has data for ONE column family, other families are NULL)
        for col in data_columns:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        
        # Metadata columns: also use first()
        # Don't aggregate columns that are already in the grouping key
        for col in metadata_columns:
            if col in all_columns and col not in group_by_cols:
                agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    else:
        # No timestamp column - group by PK + operation if available (best effort)
        if '_cdc_operation' in all_columns:
            group_by_cols = primary_key_columns + ['_cdc_operation']
        else:
            group_by_cols = primary_key_columns
        
        for col in data_columns:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        
        for col in metadata_columns:
            if col in all_columns and col not in group_by_cols:
                agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    
    # Group by primary key + timestamp + operation and aggregate
    if not agg_exprs:
        # No columns to aggregate means all data is in grouping columns
        # This shouldn't happen in normal CDC scenarios, but handle it gracefully
        if debug:
            print(f"\n⚠️  No additional columns to aggregate beyond grouping key")
            print(f"   Using distinct on grouping columns: {group_by_cols}")
        df_merged = df.select(*group_by_cols).distinct()
    else:
        df_merged = df.groupBy(*group_by_cols).agg(*agg_exprs)
    
    if debug:
        print(f"\n✅ Merge transformation applied!")
        if is_streaming:
            print(f"   Streaming DataFrame merged")
            print(f"   (Actual counts will be visible after writeStream completes)")
        else:
            print(f"   Batch DataFrame merged")
    
    return df_merged


def load_and_merge_cdc_to_delta(
    source_table: str,
    volume_path: str,
    target_table_path: str,
    spark = None,
    dbutils = None,
    crdb_config: dict = None,
    catalog: str = None,
    schema: str = None,
    clear_checkpoint: bool = False,
    verify: bool = True,
    compare_source: bool = True,
    debug: bool = True,
    version: int = None
) -> dict:
    """
    Automated CDC testing: Load, merge, and write changefeed data from Volume to Delta table.
    
    This function combines all testing steps into one call:
    - Auto-detects primary keys from CockroachDB
    - Auto-detects column families
    - Resolves timestamped paths (if volume_path is a prefix)
    - Loads from Unity Catalog Volume with Autoloader
    - Applies CDC transformations
    - Merges column family fragments if needed
    - Writes to Delta table
    - Verifies results
    - Compares with source files
    
    Args:
        source_table: CockroachDB table name (e.g., 'usertable')
        volume_path: Unity Catalog Volume path. Can be either:
                    1. Full path with timestamp: 'dbfs:/Volumes/.../test-scenario/1767823340'
                    2. Path prefix (requires version param): 'dbfs:/Volumes/.../test-scenario'
        target_table_path: Fully qualified Delta table name (e.g., 'main.schema.table_delta')
        spark: SparkSession instance (optional, will create if not provided)
        dbutils: DBUtils instance (optional, required for Spark Connect)
        crdb_config: CockroachDB credentials (optional, for auto-detection)
        catalog: Catalog name for CockroachDB connection (optional)
        schema: Schema name for CockroachDB connection (optional)
        clear_checkpoint: Whether to clear checkpoint before processing
        verify: Whether to verify results after writing
        compare_source: Whether to compare with source files
        debug: Enable debug output
        version: Controls timestamp resolution behavior:
                - None (default): Use exact path as provided (no timestamp resolution)
                - 0: Auto-resolve to latest timestamped directory
                - 1: Auto-resolve to second newest timestamped directory
                - -1: Auto-resolve to oldest timestamped directory
    
    Path Formats:
        1. **Exact path (version=None)**:
           Use the path exactly as provided, with or without timestamp:
           - With timestamp: 'dbfs:/Volumes/.../test-scenario/1767895046/'
           - Without timestamp: 'dbfs:/Volumes/.../test-scenario/' (legacy data)
           
        2. **Auto-resolve timestamp (version=int)**:
           Automatically finds and appends timestamp directory:
           - Provide: 'dbfs:/Volumes/.../test-scenario'
           - Resolves to: 'dbfs:/Volumes/.../test-scenario/1767895046/' (based on version)
           
        Recommended:
        - Use version=None for explicit paths or legacy data
        - Use version=0 for latest test data from test_cdc_matrix.sh
        
    Returns:
        Dictionary with results:
        {
            'success': bool,
            'primary_keys': List[str],
            'has_column_families': bool,
            'delta_count': int,
            'source_count': int,
            'match': bool,
            'query': StreamingQuery  # Spark streaming query object
        }
        
    Examples:
        ```python
        from cockroachdb import load_and_merge_cdc_to_delta, load_crdb_config
        
        # Load credentials
        crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
        
        # Option 1: Auto-resolve timestamp (recommended for test_cdc_matrix.sh data)
        result = load_and_merge_cdc_to_delta(
            source_table='simple_test',
            volume_path='dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_simple_test_no_split',
            target_table_path='main.schema.simple_test_delta',
            spark=spark,
            dbutils=dbutils,
            crdb_config=crdb_config,
            catalog='defaultdb',
            schema='public',
            version=0  # Auto-find latest timestamp (0=latest, 1=2nd newest, -1=oldest)
        )
        # Resolves to: .../test-json_simple_test_no_split/1767895046/
        
        # Option 2: Explicit path with timestamp
        result = load_and_merge_cdc_to_delta(
            source_table='simple_test',
            volume_path='dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_simple_test_no_split/1767895046',
            target_table_path='main.schema.simple_test_delta',
            spark=spark,
            dbutils=dbutils,
            crdb_config=crdb_config,
            catalog='defaultdb',
            schema='public',
            version=None  # Use exact path (no timestamp resolution)
        )
        
        # Option 3: Legacy path without timestamp
        result = load_and_merge_cdc_to_delta(
            source_table='simple_test',
            volume_path='dbfs:/Volumes/main/schema/volume/old_data/simple_test',
            target_table_path='main.schema.simple_test_delta',
            spark=spark,
            dbutils=dbutils,
            crdb_config=crdb_config,
            catalog='defaultdb',
            schema='public',
            version=None  # Use exact path (legacy data)
        )
        
        print(f"✅ Success: {result['success']}")
        print(f"📊 Delta: {result['delta_count']:,} rows")
        print(f"📊 Source: {result['source_count']:,} rows")
        print(f"✅ Match: {result['match']}")
        ```
    """
    from pyspark.sql import SparkSession, functions as F
    
    # Validate spark and dbutils are provided (REQUIRED - NO FALLBACKS)
    if spark is None:
        raise RuntimeError(
            "SparkSession is required. Pass 'spark' parameter from your notebook/DLT pipeline.\n"
            "Example: load_and_merge_cdc_to_delta(..., spark=spark, dbutils=dbutils)"
        )
    
    if dbutils is None:
        raise RuntimeError(
            "DBUtils is required. Pass 'dbutils' parameter from your notebook/DLT pipeline.\n"
            "Example: load_and_merge_cdc_to_delta(..., spark=spark, dbutils=dbutils)"
        )
    
    # ========================================================================
    # Step 0a: Resolve Timestamped Path (if needed)
    # ========================================================================
    # Parse the volume path to extract components
    # Uses centralized parse_volume_path() to handle both formats
    
    try:
        components = parse_volume_path(volume_path)
        
        if debug:
            print(f"📂 Parsed volume path:")
            print(f"   Volume base: {components.volume_base}")
            print(f"   Path prefix: {components.path_prefix}")
            print(f"   Timestamp in path: {components.timestamp or 'None'}")
            print(f"   Version parameter: {version}")
    except ValueError as e:
        raise ValueError(
            f"Failed to parse volume path:\n{e}\n\n"
            f"Expected format: dbfs:/Volumes/catalog/schema/volume/format/catalog/schema/test-scenario\n"
            f"Or with timestamp: dbfs:/Volumes/catalog/schema/volume/format/.../test-scenario/1767895046"
        )
    
    # Resolve the full path based on version parameter
    if version is None:
        # version=None: Use exact path as provided (no timestamp resolution)
        resolved_volume_path = components.full_path
        if debug:
            if components.has_timestamp:
                print(f"✅ Using explicit path with timestamp: {components.timestamp}")
            else:
                print(f"✅ Using explicit path (no timestamp)")
                print(f"   💡 To use latest test data, set version=0")
    else:
        # version=int: Auto-resolve timestamp from available test runs
        if debug:
            print(f"🔍 Auto-resolving timestamped path (version={version})...")
        
        try:
            resolved_volume_path = get_timestamped_path(
                volume_base=components.volume_base,
                path_prefix=components.path_prefix,
                version=version,
                dbutils=dbutils
            )
            
            resolved_timestamp = resolved_volume_path.rstrip('/').split('/')[-1]
            if debug:
                print(f"   ✅ Resolved to timestamp: {resolved_timestamp}")
                print(f"   Full path: {resolved_volume_path}")
        except ValueError as e:
            # Provide helpful error message
            raise ValueError(
                f"Failed to auto-resolve timestamped path:\n"
                f"{e}\n\n"
                f"Options:\n"
                f"1. Set version=None to use exact path: {components.full_path}\n"
                f"2. Run test_cdc_matrix.sh to generate timestamped test data\n"
                f"3. Provide full path with timestamp in volume_path parameter"
            )
    
    # Use resolved path for all subsequent operations
    volume_path = resolved_volume_path
    
    # Use supplied parameters or extract from volume path
    # Volume paths follow: /Volumes/catalog/schema/volume/format/catalog/schema/scenario/timestamp/
    # or simpler: /Volumes/catalog/schema/volume/scenario/timestamp/
    effective_catalog = catalog
    effective_schema = schema
    effective_table = source_table
    
    # For CockroachDB fallback: Extract test table name from path_prefix if it looks like test data
    # Example path_prefix: "json/defaultdb/public/test-json_usertable_with_split"
    # Scenario name: "test-json_usertable_with_split" → test table: "test_json_usertable_with_split"
    test_table_name = None
    if components.path_prefix:
        prefix_parts = components.path_prefix.rstrip('/').split('/')
        scenario_part = prefix_parts[-1]  # Last component is the scenario
        if scenario_part.startswith('test-'):
            # Convert scenario format to table format: "test-json_table_split" → "test_json_table_split"
            test_table_name = scenario_part.replace('test-', 'test_', 1).replace('-', '_')
            if debug:
                print(f"🔍 Detected test table name from path: {test_table_name}")
    
    if debug:
        print("=" * 80)
        print("AUTOMATED CDC TESTING")
        print("=" * 80)
        
        # Git marker for code version tracking
        try:
            # Get directory of this file for git commands
            try:
                code_dir = os.path.dirname(os.path.abspath(__file__))
            except NameError:
                # __file__ not available in some contexts (e.g., notebook cells)
                code_dir = os.getcwd()
            
            git_hash = subprocess.check_output(
                ['git', 'rev-parse', '--short', 'HEAD'],
                cwd=code_dir,
                stderr=subprocess.DEVNULL,
                timeout=2
            ).decode('utf-8').strip()
            git_branch = subprocess.check_output(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                cwd=code_dir,
                stderr=subprocess.DEVNULL,
                timeout=2
            ).decode('utf-8').strip()
            print(f"📌 Code Version: {git_branch}@{git_hash}")
        except Exception:
            # Git not available, not a git repo, or timeout
            print(f"📌 Code Version: (git info unavailable)")
        
        print(f"Source table: {effective_table}")
        print(f"Volume path: {volume_path}")
        print(f"Target table: {target_table_path}")
        if effective_catalog:
            print(f"CockroachDB catalog: {effective_catalog}")
        if effective_schema:
            print(f"CockroachDB schema: {effective_schema}")
        print()
    
    # Extract catalog/schema/table from target path
    target_parts = target_table_path.split('.')
    if len(target_parts) != 3:
        raise ValueError(f"target_table_path must be fully qualified: catalog.schema.table")
    
    target_catalog, target_schema, target_table = target_parts
    checkpoint_path = f"{volume_path}/_checkpoints"
    
    # ========================================================================
    # Step 0: Auto-detect Format and Validate Volume Path
    # ========================================================================
    # Extract format from volume path (parquet or json)
    try:
        file_format = components.format_type  # 'parquet' or 'json'
        if debug:
            print(f"🔍 Auto-detected format: {file_format}")
    except:
        # Fallback: assume parquet if can't detect
        file_format = 'parquet'
        if debug:
            print(f"⚠️  Could not detect format from path, assuming: {file_format}")
    
    # Set file extension and cloudFiles format based on detected format
    if file_format == 'json':
        file_extension = '.ndjson'
        cloudfiles_format = 'json'
    else:
        file_extension = '.parquet'
        cloudfiles_format = 'parquet'
    
    if debug:
        print(f"🔍 Validating volume path...")
        print(f"   File format: {file_format}")
        print(f"   File extension: {file_extension}")
    
    try:
        # List files in volume path (recursively to handle date subdirectories)
        def list_data_files_recursive(path):
            """Recursively list data files, skipping metadata."""
            files = []
            try:
                items = dbutils.fs.ls(path)
                for item in items:
                    # Skip metadata directories (check path since item.name can be empty)
                    if '/_metadata/' in item.path:
                        continue
                    
                    # Extract directory/file name from path (handle empty item.name)
                    item_name = item.name if item.name else item.path.rstrip('/').split('/')[-1]
                    
                    # Skip items starting with underscore
                    if item_name.startswith('_'):
                        continue
                    
                    # Check if it's a data file
                    if item_name.endswith(file_extension):
                        files.append(item)
                    else:
                        # Assume it's a directory, recurse into it
                        files.extend(list_data_files_recursive(item.path))
            except Exception:
                pass  # Directory might not exist or be accessible
            return files
        
        data_files = list_data_files_recursive(volume_path)
        
        if len(data_files) == 0:
            error_msg = f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ ❌ VOLUME PATH IS EMPTY                                                    ║
╚════════════════════════════════════════════════════════════════════════════╝

📂 Path: {volume_path}

🔍 The directory exists but contains no {file_extension} files!

💡 SOLUTIONS:

1️⃣  Run test_cdc_matrix.sh to generate test data:
   cd sources/cockroachdb/scripts
   ./test_cdc_matrix.sh
   
   This will automatically sync data to scenarios like:
   • test-parquet_usertable_with_split
   • test-parquet_usertable_no_split
   • test-json_usertable_with_split
   • etc.

2️⃣  Change TEST_SCENARIO to an existing scenario:
   Run this diagnostic in your notebook to see available scenarios:
   
   files = dbutils.fs.ls("dbfs:/Volumes/{target_catalog}/{target_schema}/...")
   for f in files:
       if f.isDir():
           print(f"   {{f.name}}")

3️⃣  Migrate existing files to this location:
   Use: notebooks/oneoffs/migrate_volume_to_hierarchy.ipynb

═══════════════════════════════════════════════════════════════════════════
"""
            raise FileNotFoundError(error_msg)
        
        if debug:
            print(f"   ✅ Found {len(data_files)} {file_format} file(s)")
            print()
    
    except FileNotFoundError as e:
        # Re-raise our custom error
        raise e
    except Exception as e:
        # Path doesn't exist or other error
        error_msg = f"""
╔════════════════════════════════════════════════════════════════════════════╗
║ ❌ VOLUME PATH DOES NOT EXIST                                              ║
╚════════════════════════════════════════════════════════════════════════════╝

📂 Path: {volume_path}

❌ Error: {str(e)[:200]}

💡 SOLUTIONS:

1️⃣  Check your TEST_SCENARIO variable in the notebook:
   
   Current configuration:
   • Volume path: {volume_path}
   • Source table: {effective_table}
   
   Make sure TEST_SCENARIO matches a directory that exists in your volume.

2️⃣  Run diagnostic to see available scenarios:
   
   # Add this cell to your notebook:
   from cockroachdb import parse_volume_path
   components = parse_volume_path("{volume_path}")
   try:
       formats = dbutils.fs.ls(f"{{components.volume_base}}/parquet/defaultdb/public/")
       print("Available scenarios:")
       for f in formats:
           if f.isDir() and not f.name.startswith('_'):
               print(f"   ✅ {{f.name}}")
   except:
       print("   ❌ parquet/defaultdb/public/ structure doesn't exist")

3️⃣  Generate test data:
   cd sources/cockroachdb/scripts
   ./test_cdc_matrix.sh

═══════════════════════════════════════════════════════════════════════════
"""
        raise FileNotFoundError(error_msg)
    
    # ========================================================================
    # Step 1: Load Primary Keys from Schema File (REQUIRED)
    # ========================================================================
    primary_keys = None
    has_column_families = False
    
    if debug:
        print("🔍 Loading schema from volume...")
    
    # Try to load schema file from volume (REQUIRED)
    try:
        # Use the connector's method to load schema from volume
        temp_connector = LakeflowConnect({'volume_path': volume_path, 'spark': spark, 'dbutils': dbutils})
        schema_info = temp_connector._load_schema_from_volume(volume_path, spark=spark, dbutils=dbutils)
        
        if schema_info:
            primary_keys = schema_info.get('primary_keys', [])
            has_column_families = schema_info.get('has_column_families', False)
            
            if debug:
                print(f"   ✅ Schema file found")
                print(f"   Primary keys: {primary_keys}")
                print(f"   Has column families: {has_column_families}")
                print()
        else:
            if debug:
                print(f"   ⚠️  Schema file not found in volume")
                print()
    except Exception as e:
        if debug:
            print(f"   ⚠️  Failed to load schema from volume: {e}")
            print()
    
    # Fallback: Try CockroachDB if schema not found
    if not primary_keys and crdb_config and effective_catalog and effective_schema:
        try:
            if debug:
                print("🔍 Fallback: Querying CockroachDB for metadata...")
            
            # Create connector
            connector = create_connector(crdb_config, effective_catalog, effective_schema)
            
            # For test data, try the test table name first (e.g., test_json_usertable_with_split)
            # Otherwise, fall back to the base table name (e.g., usertable)
            query_table = test_table_name if test_table_name else effective_table
            
            if debug and test_table_name:
                print(f"   Querying test table: {query_table}")
            
            # Get primary keys
            metadata = connector.read_table_metadata(query_table, {})
            primary_keys = metadata.get('primary_keys', [])
            
            # Check for column families
            has_column_families = connector._has_multiple_column_families(query_table, {})
            
            if debug:
                print(f"   ✅ Got metadata from CockroachDB")
                print(f"   Primary keys: {primary_keys}")
                print(f"   Has column families: {has_column_families}")
                print()
        except Exception as e:
            if debug:
                print(f"   ⚠️  CockroachDB query failed: {e}")
                # If test table query failed and we have a test table name, try base table as fallback
                if test_table_name:
                    try:
                        print(f"   🔄 Retrying with base table: {effective_table}")
                        metadata = connector.read_table_metadata(effective_table, {})
                        primary_keys = metadata.get('primary_keys', [])
                        has_column_families = connector._has_multiple_column_families(effective_table, {})
                        
                        print(f"   ✅ Got metadata from CockroachDB (base table)")
                        print(f"   Primary keys: {primary_keys}")
                        print(f"   Has column families: {has_column_families}")
                        print()
                    except Exception as e2:
                        print(f"   ⚠️  Base table query also failed: {e2}")
                        print()
                else:
                    print()
    
    # Error if still no primary keys
    if not primary_keys:
        raise ValueError(
            f"Could not determine primary keys for table '{effective_table}'.\n\n"
            f"Schema file is required but not found in: {volume_path}/_schema.json\n\n"
            f"Solutions:\n"
            f"1. Recreate changefeed using cockroachdb.py (auto-generates schema)\n"
            f"2. Provide crdb_config parameter to query CockroachDB directly\n"
            f"3. Run test_cdc_matrix.sh to generate test data with schemas"
        )
    
    # ========================================================================
    # Step 2: Clear Checkpoint (Optional)
    # ========================================================================
    if clear_checkpoint:
        if debug:
            print("=" * 80)
            print("FAST CHECKPOINT CLEARING")
            print("=" * 80)
        
        import time
        start_time = time.time()
        items_deleted = 0
        
        try:
            # Use parallel delete for speed (32x faster!)
            result = parallel_delete_checkpoint(
                checkpoint_path=checkpoint_path,
                dbutils=dbutils,
                max_workers=20,
                debug=debug
            )
            items_deleted = result['deleted_count']
            
            # Drop the table
            spark.sql(f"DROP TABLE IF EXISTS {target_table_path}")
            if debug:
                print(f"\n✅ Dropped table: {target_table_path}")
        except Exception as e:
            if debug:
                print(f"   ⚠️  Clear failed (may not exist): {e}")
        
        elapsed = time.time() - start_time
        if debug:
            print()
            print("=" * 80)
            print("CLEAR COMPLETE")
            print("=" * 80)
            print(f"⏱️  Total time: {elapsed:.1f}s")
            print(f"📊 Items deleted: {items_deleted}")
            print("=" * 80)
        print()
    
    # ========================================================================
    # Step 3: Load with Autoloader
    # ========================================================================
    if debug:
        print(f"📥 Loading data with Autoloader ({file_format} format)...")
    
    df_raw = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", cloudfiles_format)
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")  # Read files in subdirectories (e.g., date dirs)
        .option("pathGlobFilter", f"*{effective_table}*{file_extension}")
        .load(volume_path)
    )
    
    if debug:
        print(f"   ✅ Autoloader configured")
        print()
    
    # ========================================================================
    # Step 4: Detect Snapshot Cutoff (for JSON files)
    # ========================================================================
    snapshot_cutoff = None
    if file_format == 'json':
        # For JSON, detect snapshot cutoff by finding max timestamp in first files
        if debug:
            print("🔍 Detecting JSON snapshot cutoff timestamp...")
        
        try:
            # List files in volume
            all_files = dbutils.fs.ls(volume_path)
            
            # CRITICAL: Only look at SNAPSHOT files (sequence 00000000)
            # CDC files (sequence 00000001+) should NOT influence the cutoff
            # Snapshot files have pattern: *-00000000-*
            snapshot_files = [f for f in all_files 
                             if f.name.endswith(('.ndjson', '.json')) 
                             and '-00000000-' in f.name]
            
            if not snapshot_files:
                # Fallback: if no explicit snapshot files, use first file only
                # (This handles cases where sequence number pattern doesn't exist)
                all_json = sorted([f for f in all_files if f.name.endswith(('.ndjson', '.json'))])
                snapshot_files = all_json[:1] if all_json else []
            
            max_timestamp = None
            for file_info in snapshot_files:
                try:
                    df_sample = spark.read.json(file_info.path)
                    if 'updated' in df_sample.columns:
                        # Get max timestamp from this file
                        file_max = df_sample.agg({"updated": "max"}).collect()[0][0]
                        if file_max:
                            file_max_str = str(file_max)
                            if max_timestamp is None or file_max_str > max_timestamp:
                                max_timestamp = file_max_str
                except:
                    continue
            
            snapshot_cutoff = max_timestamp
            if debug:
                if snapshot_cutoff:
                    print(f"   📍 JSON snapshot cutoff: {snapshot_cutoff}")
                    print(f"   📁 Detected from {len(snapshot_files)} snapshot file(s)")
                else:
                    print(f"   ⚠️  No snapshot cutoff detected (no snapshot files found)")
        except Exception as e:
            if debug:
                print(f"   ⚠️  Could not detect snapshot cutoff: {e}")
    
    # ========================================================================
    # Step 5: Apply CDC Transformations
    # ========================================================================
    if debug:
        print("🔧 Applying CDC transformations...")
    
    # Use shared CDC transformation method
    temp_connector = LakeflowConnect({'volume_path': volume_path, 'spark': spark, 'dbutils': dbutils})
    
    # Set snapshot cutoff if detected
    if snapshot_cutoff:
        temp_connector._snapshot_cutoff_timestamps[effective_table] = snapshot_cutoff
    
    df_enriched = temp_connector._add_cdc_metadata_to_dataframe(
        df_raw,
        table_name=effective_table,
        primary_key_columns=primary_keys
    )
    
    if debug:
        print(f"   ✅ CDC metadata added")
        print()
    
    # ========================================================================
    # Step 5: Write CDC Events to Temp Table (Before Column Family Merge)
    # ========================================================================
    # UNIFIED APPROACH (both JSON and Parquet):
    # Write raw events to temp table BEFORE column family merge to avoid
    # streaming aggregations that prevent using append mode
    if debug:
        print("💾 Writing CDC events to temp table...")
        print("   (Column family merge will happen in batch mode)")
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    
    # Step 1: Write all raw CDC events to temp table using append mode
    # We write BEFORE column family merge to avoid aggregations in streaming
    temp_table_path = f"{target_catalog}.{target_schema}.{target_table}_temp_all"
    
    if debug:
        print(f"   🚀 Step 1: Writing raw CDC events to temp table...")
    
    query = (df_enriched.writeStream
        .format("delta")
        .outputMode("append")  # Works because no aggregations yet
        .option("checkpointLocation", f"{checkpoint_path}/delta")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(temp_table_path)
    )
    
    query.awaitTermination()
    
    if debug:
        print(f"   ✅ Raw CDC events written to temp table")
    
    # Step 2: Read temp table and merge column families in batch mode
    if debug:
        print(f"   🚀 Step 2: Merging column family fragments in batch mode...")
    
    df_raw_events = spark.table(temp_table_path)
    
    if debug:
        # DIAGNOSTIC: Show what columns we have before merge
        all_cols = df_raw_events.columns
        print(f"   🔍 Columns in temp table: {len(all_cols)} total")
        # Show first 15 columns for diagnostics
        print(f"      {all_cols[:15]}")
        if len(all_cols) > 15:
            print(f"      ... and {len(all_cols) - 15} more")
        
        # CRITICAL DIAGNOSTIC: Count operations BEFORE merge
        print(f"   🔍 CDC operations in temp table (BEFORE merge):")
        if '_cdc_operation' in all_cols:
            op_counts_before = df_raw_events.groupBy("_cdc_operation").count().collect()
            for row in op_counts_before:
                print(f"      {row['_cdc_operation']}: {row['count']}")
            
            # DIAGNOSTIC: Check if DELETE timestamps are unique
            timestamp_cols = [c for c in ['updated', '_cdc_timestamp', '__crdb__updated'] if c in all_cols]
            if timestamp_cols and primary_keys:
                ts_col = timestamp_cols[0]
                # Check if primary key columns exist in the DataFrame
                pk_cols_exist = all(pk in all_cols for pk in primary_keys)
                if pk_cols_exist:
                    deletes_df = df_raw_events.filter(F.col("_cdc_operation") == "DELETE")
                    total_deletes = deletes_df.count()
                    if total_deletes > 0:
                        unique_delete_combos = deletes_df.select(primary_keys + [ts_col, '_cdc_operation']).distinct().count()
                        print(f"   🔍 DELETE key extraction analysis:")
                        print(f"      Total DELETE rows: {total_deletes}")
                        print(f"      Unique (key + {ts_col} + operation): {unique_delete_combos}")
                        
                        # Show sample ycsb_key values
                        sample_keys = deletes_df.select(primary_keys[0]).limit(5).collect()
                        sample_key_values = [row[primary_keys[0]] for row in sample_keys]
                        print(f"      Sample {primary_keys[0]} values: {sample_key_values[:5]}")
                        
                        if total_deletes != unique_delete_combos:
                            print(f"      ⚠️  WARNING: {total_deletes - unique_delete_combos} DELETEs have duplicate (key+timestamp+operation)!")
                        else:
                            print(f"      ✅ All DELETEs have unique (key+timestamp+operation)")
                else:
                    print(f"   ⚠️  Primary key columns {primary_keys} not found in DataFrame!")
                    print(f"      Available columns: {all_cols[:20]}...")
        else:
            print(f"      ⚠️  No _cdc_operation column found!")
    
    df_all_events = merge_column_family_fragments(
        df_raw_events,
        primary_key_columns=primary_keys,
        debug=debug
    )
    
    print()
    
    # ========================================================================
    # Step 6: Apply CDC Merge with DELETE Support (Batch Mode)
    # ========================================================================
    # UNIFIED APPROACH (both JSON and Parquet):
    # Use batch Delta merge with explicit DELETE handling via whenMatchedDelete()
    if debug:
        print("💾 Writing CDC data to Delta table...")
        print(f"   🚀 Step 3: Applying CDC merge with DELETE support...")
        event_counts = df_all_events.groupBy("_cdc_operation").count().collect()
        print(f"   🔍 Events after column family merge:")
        for row in event_counts:
            print(f"      {row['_cdc_operation']}: {row['count']}")
    
    table_exists = spark.catalog.tableExists(target_table_path)
    
    if not table_exists:
        # First run: Create table with proper DELETE handling
        # Get keys that have DELETE events
        delete_keys = df_all_events.filter(F.col("_cdc_operation") == "DELETE") \
            .select(*primary_keys) \
            .distinct()
        
        # Get all non-DELETE rows
        active_rows = df_all_events.filter(F.col("_cdc_operation") != "DELETE")
        
        # Exclude rows with keys that are deleted
        rows_after_delete = active_rows.join(
            delete_keys,
            on=primary_keys,
            how="left_anti"
        )
        
        # CRITICAL: For initial table creation, keep only LATEST state per key
        # We may have multiple events for same key (SNAPSHOT + UPDATE), but
        # the final table should only have one row per key (the latest state)
        
        # Add row number partitioned by PK, ordered by timestamp DESC
        window_spec = Window.partitionBy(*primary_keys).orderBy(F.col("_cdc_timestamp").desc())
        final_rows = (rows_after_delete
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
        
        if debug:
            delete_count = delete_keys.count()
            active_count = active_rows.count()
            after_delete_count = rows_after_delete.count()
            final_count = final_rows.count()
            print(f"   🔍 Keys to delete: {delete_count}")
            print(f"   🔍 Non-DELETE rows: {active_count}")
            print(f"   🔍 After excluding DELETEd keys: {after_delete_count}")
            print(f"   🔍 After deduplication (latest per key): {final_count}")
            print(f"   🔍 Rows removed by DELETE: {active_count - after_delete_count}")
            print(f"   🔍 Duplicate events removed: {after_delete_count - final_count}")
            print(f"   📝 Creating initial table: {final_count:,} rows")
        
        final_rows.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table_path)
    else:
        # Incremental: Use Delta merge
        if debug:
            print(f"   🔄 Merging incremental changes...")
        
        delta_table = DeltaTable.forName(spark, target_table_path)
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        
        delta_table.alias("target").merge(
            df_all_events.alias("source"),
            merge_condition
        ).whenMatchedDelete(
            condition = "source._cdc_operation = 'DELETE'"
        ).whenMatchedUpdateAll(
            condition = "source._cdc_operation IN ('UPDATE', 'INSERT', 'SNAPSHOT')"
        ).whenNotMatchedInsertAll(
            condition = "source._cdc_operation IN ('INSERT', 'SNAPSHOT')"
        ).execute()
    
    # Clean up temp table
    spark.sql(f"DROP TABLE IF EXISTS {temp_table_path}")
    
    if debug:
        print(f"   ✅ CDC merge complete!")
        print()
    
    # ========================================================================
    # Step 7: Verify Results (Optional)
    # ========================================================================
    delta_count = 0
    if verify:
        if debug:
            print("✅ Verifying results...")
        
        df_delta = spark.table(target_table_path)
        delta_count = df_delta.count()
        
        if debug:
            print(f"   📊 Delta table: {delta_count:,} rows")
            
            # Show operation breakdown
            operation_counts = df_delta.groupBy("_cdc_operation").count().collect()
            for row in operation_counts:
                print(f"      {row['_cdc_operation']}: {row['count']:,}")
            print()
    
    # ========================================================================
    # Step 8: Compare with Source Files (Optional)
    # ========================================================================
    source_count = 0
    match = False
    
    if compare_source:
        if debug:
            print("📊 Comparing with source files...")
        
        try:
            source_stats = analyze_volume_changefeed_files(
                volume_path, 
                primary_key_columns=primary_keys,
                debug=debug,  # Pass through debug parameter
                spark=spark,
                dbutils=dbutils
            )
            source_count = source_stats.get('unique_keys', 0)
            
            if debug:
                print(f"   📊 Source files: {source_stats['file_count']}")
                print(f"   📊 Unique keys (deduplicated): {source_count:,}")
                
                # Show total events before deduplication
                total_events = (source_stats.get('snapshot', 0) + 
                               source_stats.get('insert', 0) + 
                               source_stats.get('update', 0) + 
                               source_stats.get('delete', 0))
                if total_events > source_count:
                    print(f"   📊 Total events (raw): {total_events:,} (includes updates to same keys)")
                
                print(f"   📊 UPSERT events: {source_stats.get('snapshot', 0) + source_stats.get('update', 0):,}")
                print(f"   📊 DELETE events: {source_stats.get('delete', 0):,}")
                print()
                
                print(f"📊 Comparison:")
                print(f"   Delta: {delta_count:,}")
                print(f"   Source: {source_count:,}")
                
            match = (delta_count == source_count)
            
            if match:
                if debug:
                    print(f"\n   ✅✅✅ PERFECT MATCH! ✅✅✅")
            else:
                diff = delta_count - source_count
                if debug:
                    print(f"\n   ⚠️  MISMATCH: {diff:+,} rows")
            
            print()
        except Exception as e:
            if debug:
                print(f"   ⚠️  Comparison failed: {e}")
                print()
    
    # ========================================================================
    # Return Results
    # ========================================================================
    if debug:
        print("=" * 80)
        print("TEST COMPLETE")
        print("=" * 80)
    
    return {
        'success': True,
        'primary_keys': primary_keys,
        'has_column_families': has_column_families,
        'delta_count': delta_count,
        'source_count': source_count,
        'match': match,
        'query': query,
        'destination_table': target_table_path,
        'checkpoint_path': checkpoint_path,  # Added for cleanup
        'resolved_volume_path': volume_path  # Show actual path used (after timestamp resolution)
    }


def cleanup_test_checkpoint(
    volume_path: str,
    target_table_path: str = None,
    dbutils = None,
    version: int = 0,
    drop_table: bool = False,
    debug: bool = True,
    spark = None
):
    """
    Clean up checkpoint files after testing to prepare for next test run.
    
    This is useful for iterative testing where you want to inspect results
    before cleaning up for the next test cycle.
    
    Args:
        volume_path: Path to volume (same as load_and_merge_cdc_to_delta)
        target_table_path: Fully qualified table name (catalog.schema.table)
                          Only needed if drop_table=True
        dbutils: Databricks utils object (required)
        version: Which timestamped path to use (0=latest, 1=second newest, -1=oldest)
        drop_table: If True, also drop the Delta table
        debug: Show progress messages
        spark: SparkSession (only needed if drop_table=True)
    
    Returns:
        Dictionary with cleanup results:
        {
            'success': bool,
            'items_deleted': int,
            'table_dropped': bool
        }
    
    Examples:
        ```python
        # After running test and inspecting results:
        from cockroachdb import cleanup_test_checkpoint
        
        # Option 1: Clean checkpoint only (keep table for inspection)
        cleanup_test_checkpoint(
            volume_path=VOLUME_PATH,
            dbutils=dbutils,
            version=0
        )
        
        # Option 2: Clean checkpoint AND drop table (full reset)
        cleanup_test_checkpoint(
            volume_path=VOLUME_PATH,
            target_table_path=TARGET_TABLE_PATH,
            dbutils=dbutils,
            spark=spark,
            version=0,
            drop_table=True
        )
        
        # Option 3: Use result from previous test
        result = load_and_merge_cdc_to_delta(...)
        # ... inspect results ...
        cleanup_test_checkpoint(
            volume_path=VOLUME_PATH,
            target_table_path=result['destination_table'],
            dbutils=dbutils,
            spark=spark,
            version=0,
            drop_table=True
        )
        ```
    """
    import time
    
    if dbutils is None:
        raise RuntimeError(
            "DBUtils is required. Pass 'dbutils' parameter from your notebook.\n"
            "Example: cleanup_test_checkpoint(..., dbutils=dbutils)"
        )
    
    if drop_table and spark is None:
        raise RuntimeError(
            "SparkSession is required when drop_table=True.\n"
            "Example: cleanup_test_checkpoint(..., spark=spark, drop_table=True)"
        )
    
    # Resolve volume path (same logic as load function)
    try:
        components = parse_volume_path(volume_path)
    except ValueError as e:
        raise ValueError(f"Failed to parse volume path:\n{e}")
    
    # Resolve timestamp if needed
    if version is None:
        resolved_volume_path = components.full_path
    else:
        try:
            resolved_volume_path = get_timestamped_path(
                volume_base=components.volume_base,
                path_prefix=components.path_prefix,
                version=version,
                dbutils=dbutils
            )
        except ValueError as e:
            raise ValueError(f"Failed to resolve timestamped path:\n{e}")
    
    checkpoint_path = f"{resolved_volume_path}/_checkpoints"
    
    if debug:
        print("=" * 80)
        print("CLEANUP TEST CHECKPOINT")
        print("=" * 80)
        print(f"Volume path: {resolved_volume_path}")
        print(f"Checkpoint: {checkpoint_path}")
        if drop_table and target_table_path:
            print(f"Table to drop: {target_table_path}")
        print("=" * 80)
        print()
    
    start_time = time.time()
    items_deleted = 0
    table_dropped = False
    
    try:
        # Delete checkpoint using parallel delete
        if debug:
            print("🧹 Deleting checkpoint files...")
        
        result = parallel_delete_checkpoint(
            checkpoint_path=checkpoint_path,
            dbutils=dbutils,
            max_workers=20,
            debug=debug
        )
        items_deleted = result['deleted_count']
        
        if debug:
            print(f"   ✅ Deleted {items_deleted} checkpoint items")
            print()
    except Exception as e:
        if debug:
            print(f"   ⚠️  Checkpoint delete failed (may not exist): {e}")
            print()
    
    # Drop table if requested
    if drop_table and target_table_path:
        try:
            if debug:
                print(f"🗑️  Dropping table: {target_table_path}")
            
            spark.sql(f"DROP TABLE IF EXISTS {target_table_path}")
            table_dropped = True
            
            if debug:
                print(f"   ✅ Table dropped")
                print()
        except Exception as e:
            if debug:
                print(f"   ⚠️  Table drop failed: {e}")
                print()
    
    elapsed = time.time() - start_time
    
    if debug:
        print("=" * 80)
        print("CLEANUP COMPLETE")
        print("=" * 80)
        print(f"⏱️  Time: {elapsed:.1f}s")
        print(f"📊 Items deleted: {items_deleted}")
        if drop_table:
            print(f"🗑️  Table dropped: {table_dropped}")
        print("=" * 80)
        print()
    
    return {
        'success': True,
        'items_deleted': items_deleted,
        'table_dropped': table_dropped
    }


def collect_all_records(
    connector: 'LakeflowConnect',
    table_name: str,
    table_options: Dict[str, str] = None,
    max_batches: int = None,
    debug: bool = False
) -> Dict[str, Any]:
    """
    Convenience function to read all records from a table using iterator pattern.
    
    This wraps the manual while-loop pattern into a single function call.
    Handles batch iteration, offset management, and statistics collection automatically.
    
    **Use Cases:**
    - Testing and prototyping CDC workflows
    - Collecting data for analysis or comparison
    - Batch processing of small to medium datasets
    
    **Important:**
    - All records are loaded into memory - use with caution for large tables
    - For production streaming, use `load_and_merge_cdc_to_delta()` instead
    - Connector automatically applies deduplication and DELETE filtering
    
    Args:
        connector: LakeflowConnect instance (any mode: VOLUME, AZURE_*, DIRECT)
        table_name: Name of table to read
        table_options: Optional table-specific options (default: {})
        max_batches: Safety limit for number of batches (None = unlimited, recommended for DIRECT mode)
        debug: Print progress messages during iteration
        
    Returns:
        Dictionary with:
            - records: List[Dict] - All records (deduplicated, DELETEs filtered)
            - batch_count: int - Number of batches read
            - operations: Dict[str, int] - Count of each CDC operation type
            - primary_keys: List[str] - Primary key columns
            - metadata: Dict - Full table metadata
            
    Example:
        ```python
        from cockroachdb import LakeflowConnect, ConnectorMode, collect_all_records
        
        # Initialize connector
        connector = LakeflowConnect({
            'mode': ConnectorMode.VOLUME.value,
            'volume_path': '/Volumes/main/schema/volume/path/1234567890',
            'spark': spark,
            'dbutils': dbutils
        })
        
        # Collect all records
        result = collect_all_records(
            connector=connector,
            table_name='usertable',
            max_batches=100,  # Safety limit for DIRECT mode
            debug=True
        )
        
        print(f"Read {len(result['records']):,} records")
        print(f"Primary keys: {result['primary_keys']}")
        print(f"CDC operations: {result['operations']}")
        ```
        
    Raises:
        ValueError: If table not found or invalid configuration
        RuntimeError: If connector not properly initialized
    """
    if table_options is None:
        table_options = {}
    
    # Get metadata (includes primary keys, ingestion type)
    try:
        metadata = connector.read_table_metadata(table_name, table_options)
        primary_keys = metadata.get('primary_keys', [])
    except Exception as e:
        raise ValueError(f"Failed to get metadata for table '{table_name}': {e}")
    
    if debug:
        print(f"📖 Reading '{table_name}' using iterator pattern...")
        print(f"   Mode: {connector.mode}")
        print(f"   Primary keys: {primary_keys}")
        print()
    
    # Initialize iteration state
    start_offset = {"cursor": ""}
    all_records = []
    batch_count = 0
    
    # Read all batches
    while True:
        try:
            record_iterator, end_offset = connector.read_table(
                table_name=table_name,
                start_offset=start_offset,
                table_options=table_options
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read batch {batch_count + 1}: {e}")
        
        # Collect records from this batch
        batch_records = list(record_iterator)
        
        if not batch_records:
            if debug:
                print(f"✅ No more records. Finished reading.")
            break
        
        batch_count += 1
        all_records.extend(batch_records)
        
        if debug:
            cursor_preview = str(end_offset.get('cursor', 'N/A'))[:20]
            print(f"   Batch {batch_count}: {len(batch_records):,} records (cursor: {cursor_preview}...)")
        
        # Check if done (cursor didn't advance)
        if end_offset.get('cursor') == start_offset.get('cursor'):
            if debug:
                print(f"✅ Cursor unchanged. Finished reading.")
            break
        
        # Update offset for next iteration
        start_offset = end_offset
        
        # Safety limit check (especially important for DIRECT mode to prevent infinite loops)
        if max_batches and batch_count >= max_batches:
            if debug:
                print(f"⚠️  Safety limit reached ({max_batches} batches). Stopping.")
            break
    
    # Collect CDC operation statistics
    operations = {}
    for record in all_records:
        op = record.get('_cdc_operation', 'UNKNOWN')
        operations[op] = operations.get(op, 0) + 1
    
    if debug:
        print(f"\n📊 Summary:")
        print(f"   Total records: {len(all_records):,}")
        print(f"   Total batches: {batch_count}")
        print(f"   CDC Operations:")
        for op, count in sorted(operations.items()):
            print(f"      {op}: {count:,}")
        print()
    
    return {
        'records': all_records,
        'batch_count': batch_count,
        'operations': operations,
        'primary_keys': primary_keys,
        'metadata': metadata
    }
