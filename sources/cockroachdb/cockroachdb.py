from typing import Dict, List, Iterator, Any
import json
import psycopg2
import psycopg2.extras
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DoubleType, BooleanType, DateType, TimestampType, BinaryType,
    DecimalType, ArrayType
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


class LakeflowConnect:
    """CockroachDB connector using sinkless changefeeds for CDC."""
    
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the CockroachDB connector.
        
        Supports two connection modes:
        
        1. Connection URL (recommended - single parameter like other connectors):
            - connection_url: PostgreSQL connection string
              Format: postgresql://user:password@host:port/database?sslmode=require
        
        2. Individual parameters (legacy - may not work with Unity Catalog):
            - host: CockroachDB host
            - port: CockroachDB port (default: 26257)
            - database: Database name
            - user: Username
            - password: Password (can be empty for insecure mode)
            - sslmode: SSL mode (default: 'require')
            - schema: Schema name (default: 'public')
        """
        self.conn = None  # Initialize conn first for __del__
        
        # Debug: Print what options we actually receive from Unity Catalog
        print("=" * 80)
        print("🔍 DEBUG: CockroachDB Connector __init__ called")
        print("=" * 80)
        print(f"Options received from Unity Catalog connection:")
        print(f"  Total options: {len(options)}")
        print(f"\nALL OPTIONS (raw dump):")
        for key in sorted(options.keys()):
            # Mask ONLY passwords for security
            if "password" in key.lower():
                value = "***REDACTED***"
            else:
                # Show everything else, even if potentially sensitive
                # This is for debugging - we need to see what UC actually passes
                value = options[key]
            print(f"  {key}: {value}")
        print("=" * 80)
        
        # Schema is always from options (not from connection)
        self.schema = options.get("schema", "public")
        
        # CRITICAL: Unity Catalog passes connection NAME, not credentials!
        # We must fetch credentials from Unity Catalog using the connection name
        
        connection_name = options.get("databricks.connection")
        
        if connection_name:
            print(f"✓ Found Unity Catalog connection: {connection_name}")
            print(f"  Fetching credentials from Unity Catalog...")
            self._fetch_credentials_from_uc(connection_name, options)
        # Fallback: Direct credential modes (for local testing without Unity Catalog)
        elif options.get("token"):
            print("✓ Using 'token' parameter (direct testing mode)")
            self._parse_connection_url(options.get("token"))
        elif options.get("connection_url"):
            print("✓ Using 'connection_url' parameter (direct testing mode)")
            self._parse_connection_url(options.get("connection_url"))
        elif options.get("host"):
            print("✓ Using individual parameters (direct testing mode)")
            self.host = options.get("host")
            self.port = int(options.get("port", "26257"))
            self.database = options.get("database")
            self.user = options.get("user")
            self.password = options.get("password", "")
            self.sslmode = options.get("sslmode", "require")
        else:
            print("❌ No recognized connection parameters found!")
            print(f"   Available keys: {sorted(options.keys())}")
            self.host = None
            self.database = None
            self.user = None
    
    def _fetch_credentials_from_uc(self, connection_name: str, options: Dict[str, str]) -> None:
        """
        Fetch connection credentials from Unity Catalog.
        
        Unity Catalog stores credentials securely. We fetch them using Databricks SDK.
        """
        try:
            # Initialize Databricks SDK client
            # It will use the same authentication as the Databricks CLI/environment
            w = WorkspaceClient()
            
            print(f"  Fetching connection details...")
            connection = w.connections.get(connection_name)
            
            print(f"  Connection type: {connection.connection_type}")
            print(f"  Connection options available: {list(connection.options.keys()) if connection.options else []}")
            
            # Extract credentials from connection options
            conn_opts = connection.options or {}
            
            # Try token-based connection (GitHub-style)
            if "token" in conn_opts:
                print(f"  ✅ Found 'token' parameter, parsing as connection URL")
                self._parse_connection_url(conn_opts["token"])
                return
            
            # Try connection_url
            if "connection_url" in conn_opts:
                print(f"  ✅ Found 'connection_url' parameter")
                self._parse_connection_url(conn_opts["connection_url"])
                return
            
            # Try individual parameters
            if "host" in conn_opts:
                print(f"  ✅ Found individual database parameters")
                self.host = conn_opts.get("host")
                self.port = int(conn_opts.get("port", "26257"))
                self.database = conn_opts.get("database")
                self.user = conn_opts.get("user")
                self.password = conn_opts.get("password", "")
                self.sslmode = conn_opts.get("sslmode", "require")
                return
            
            # No credentials found
            print(f"  ❌ No credentials found in Unity Catalog connection!")
            print(f"     Available options: {sorted(conn_opts.keys())}")
            self.host = None
            self.database = None
            self.user = None
            
        except Exception as e:
            print(f"  ❌ Failed to fetch credentials from Unity Catalog: {e}")
            print(f"     This might be a permissions issue or SDK configuration problem")
            self.host = None
            self.database = None
            self.user = None
        
        # Validate required parameters (password can be empty)
        if not all([self.host, self.database, self.user is not None]):
            print("\n❌ ERROR: Missing required connection parameters!")
            print(f"  host: {self.host}")
            print(f"  database: {self.database}")
            print(f"  user: {self.user}")
            print(f"\nPossible causes:")
            print(f"  1. Unity Catalog is not passing individual parameters (host, port, etc.)")
            print(f"  2. Try using 'connection_url' parameter instead (single string)")
            print(f"     Format: postgresql://user:password@host:port/database?sslmode=require")
            raise ValueError("Missing required connection parameters: host, database, user")
    
    def _parse_connection_url(self, url: str) -> None:
        """Parse PostgreSQL connection URL into individual components."""
        import re
        from urllib.parse import urlparse, parse_qs
        
        # Parse URL: postgresql://user:password@host:port/database?params
        parsed = urlparse(url)
        
        self.user = parsed.username
        self.password = parsed.password or ""
        self.host = parsed.hostname
        self.port = parsed.port or 26257
        self.database = parsed.path.lstrip("/").split("?")[0]
        
        # Parse query parameters
        query_params = parse_qs(parsed.query)
        self.sslmode = query_params.get("sslmode", ["require"])[0]
        
        print(f"  Parsed from connection_url:")
        print(f"    host: {self.host}")
        print(f"    port: {self.port}")
        print(f"    database: {self.database}")
        print(f"    user: {self.user}")
        print(f"    password: {'***' if self.password else '(empty)'}")
        print(f"    sslmode: {self.sslmode}")
        
        self._init_connection()
    
    def _init_connection(self) -> None:
        """Initialize connection to CockroachDB."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                sslmode=self.sslmode
            )
            self.conn.set_session(autocommit=True)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to CockroachDB: {str(e)}")
    
    def _ensure_connection(self) -> None:
        """Ensure connection is alive, reconnect if needed."""
        try:
            if self.conn is None or self.conn.closed:
                self._init_connection()
        except Exception:
            self._init_connection()
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the specified schema.
        
        Returns:
            List of table names
        """
        self._ensure_connection()
        
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (self.schema,))
            tables = [row[0] for row in cur.fetchall()]
        
        return tables
    
    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a CockroachDB table.
        
        Args:
            table_name: Name of the table
            table_options: Additional options (not used currently)
        
        Returns:
            StructType representing the table schema
        """
        # Validate table exists
        if table_name not in self.list_tables():
            raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
        
        self._ensure_connection()
        
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
        
        with self.conn.cursor() as cur:
            cur.execute(query, (self.schema, table_name))
            columns = cur.fetchall()
        
        fields = []
        for col in columns:
            col_name, data_type, char_max_len, num_precision, num_scale, is_nullable = col
            nullable = (is_nullable == 'YES')
            spark_type = self._map_cockroachdb_type_to_spark(
                data_type, char_max_len, num_precision, num_scale
            )
            fields.append(StructField(col_name, spark_type, nullable))
        
        # Add CDC metadata fields (connector-added, not in source database)
        # These are derived from CockroachDB's native changefeed columns:
        #   - CockroachDB returns: (key, value, updated, topic)
        #   - Connector adds: _cdc_key, _cdc_updated, _cdc_operation
        # This follows Lakeflow CDC conventions for Delta Lake integration
        fields.append(StructField("_cdc_key", ArrayType(StringType()), True))      # Mapped from changefeed 'key' column
        fields.append(StructField("_cdc_updated", StringType(), True))             # Mapped from changefeed 'updated' column
        fields.append(StructField("_cdc_operation", StringType(), True))           # Derived from changefeed 'value' column
        
        return StructType(fields)
    
    def _map_cockroachdb_type_to_spark(
        self, data_type: str, char_max_len: int, num_precision: int, num_scale: int
    ) -> Any:
        """Map CockroachDB/PostgreSQL types to Spark types."""
        data_type_lower = data_type.lower()
        
        # Integer types
        if data_type_lower in ('bigint', 'int8', 'serial', 'bigserial'):
            return LongType()
        elif data_type_lower in ('integer', 'int', 'int4', 'int2', 'smallint'):
            return LongType()  # Prefer LongType over IntegerType
        
        # Floating point types
        elif data_type_lower in ('double precision', 'float8', 'float', 'real', 'float4'):
            return DoubleType()
        
        # Decimal types
        elif data_type_lower in ('numeric', 'decimal'):
            if num_precision and num_scale is not None:
                return DecimalType(num_precision, num_scale)
            else:
                return DecimalType(38, 18)  # Default precision
        
        # Boolean
        elif data_type_lower in ('boolean', 'bool'):
            return BooleanType()
        
        # String types
        elif data_type_lower in ('character varying', 'varchar', 'character', 'char', 'text', 'string'):
            return StringType()
        
        # Binary types
        elif data_type_lower in ('bytea', 'bytes'):
            return BinaryType()
        
        # Date and time types
        elif data_type_lower == 'date':
            return DateType()
        elif data_type_lower in ('timestamp without time zone', 'timestamp'):
            return TimestampType()
        elif data_type_lower in ('timestamp with time zone', 'timestamptz'):
            return TimestampType()
        elif data_type_lower in ('time', 'time without time zone', 'time with time zone'):
            return StringType()  # Represent time as string
        elif data_type_lower == 'interval':
            return StringType()
        
        # UUID
        elif data_type_lower == 'uuid':
            return StringType()
        
        # Network types
        elif data_type_lower in ('inet', 'cidr', 'macaddr'):
            return StringType()
        
        # JSON types
        elif data_type_lower in ('json', 'jsonb'):
            return StringType()  # Store JSON as string
        
        # Array types
        elif data_type_lower == 'ARRAY':
            return ArrayType(StringType())  # Default to string array
        
        # Default fallback
        else:
            return StringType()
    
    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Get metadata for a table.
        
        Args:
            table_name: Name of the table
            table_options: Additional options (not used currently)
        
        Returns:
            Dictionary with metadata:
                - primary_keys: List of primary key column names
                - cursor_field: Field to use for cursor (_cdc_updated)
                - ingestion_type: Always 'cdc' for changefeeds
        """
        # Validate table exists
        if table_name not in self.list_tables():
            raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
        
        self._ensure_connection()
        
        # Get primary key columns
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
        
        with self.conn.cursor() as cur:
            cur.execute(pk_query, (self.schema, table_name))
            pk_columns = [row[0] for row in cur.fetchall()]
        
        if not pk_columns:
            raise ValueError(f"Table '{table_name}' has no primary key. CockroachDB changefeeds require a primary key.")
        
        return {
            "primary_keys": pk_columns,
            "cursor_field": "_cdc_updated",  # Use CDC timestamp as cursor
            "ingestion_type": "cdc"  # All CockroachDB tables support CDC via changefeeds
        }
    
    def read_table(
        self, table_name: str, start_offset: Dict, table_options: Dict[str, str]
    ) -> (Iterator[Dict], Dict):
        """
        Read table data using CockroachDB changefeed.
        
        Args:
            table_name: Name of the table
            start_offset: Starting cursor position (timestamp)
            table_options: Additional options:
                - cursor: Resume cursor (CockroachDB timestamp)
                - include_diff: Include before/after values (boolean)
                - select_query: Custom SELECT for filtered changefeed
                - resolved_interval: Resolved timestamp interval (default: '10s')
                - batch_size: Number of events to read before returning (default: 1000)
        
        Returns:
            Iterator of change events and end offset
        """
        # Validate table exists
        if table_name not in self.list_tables():
            raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
        
        # Parse options
        cursor = start_offset.get("cursor") if start_offset else None
        include_diff = table_options.get("include_diff", "false").lower() == "true"
        select_query = table_options.get("select_query")
        resolved_interval = table_options.get("resolved_interval", "10s")
        batch_size = int(table_options.get("batch_size", "1000"))
        
        # Override cursor from table_options if provided
        if table_options.get("cursor"):
            cursor = table_options.get("cursor")
        
        # Build changefeed query
        changefeed_options = []
        
        # Include initial scan (existing rows) if requested
        # 'only' = return existing rows then stop (batch mode - no streaming options)
        # 'yes' = return existing rows then stream new changes (CDC mode - includes streaming options)
        initial_scan = table_options.get("initial_scan", "only")  # Default to 'only' for faster testing
        
        if initial_scan.lower() == "only":
            # Batch/snapshot mode: initial_scan='only' cannot be combined with updated/resolved
            changefeed_options.append(f"initial_scan='only'")
            # Don't add 'updated' or 'resolved' - they're incompatible with initial_scan='only'
        elif initial_scan.lower() == "yes":
            # Streaming CDC mode: include streaming options
            changefeed_options.append(f"initial_scan='yes'")
            changefeed_options.append("updated")  # Include timestamps
            changefeed_options.append(f"resolved='{resolved_interval}'")
        else:
            # Default streaming mode (no initial_scan specified)
            changefeed_options.append("updated")  # Include timestamps
            changefeed_options.append(f"resolved='{resolved_interval}'")
        
        if include_diff:
            changefeed_options.append("diff")
        
        # Add split_column_families for tables with multiple column families
        # This is required for some tables (like YCSB usertable)
        split_families = table_options.get("split_column_families", "true")
        if split_families.lower() == "true":
            changefeed_options.append("split_column_families")
        
        if cursor:
            changefeed_options.append(f"cursor='{cursor}'")
        
        options_str = ", ".join(changefeed_options)
        
        # Determine changefeed target
        if select_query:
            # Use CDC query (filtered changefeed)
            target = f"({select_query})"
        else:
            # Use table name
            if self.schema != 'public':
                target = f"{self.schema}.{table_name}"
            else:
                target = table_name
        
        changefeed_query = f"EXPERIMENTAL CHANGEFEED FOR {target} WITH {options_str}"
        
        # Debug logging (can be disabled in production)
        import os
        if os.getenv("DEBUG_CHANGEFEED"):
            print(f"[DEBUG] Changefeed query: {changefeed_query}")
            print(f"[DEBUG] Batch size: {batch_size}")
            print(f"[DEBUG] Start offset: {start_offset}")
        
        # Execute changefeed
        self._ensure_connection()
        
        def event_generator():
            """Generator that yields changefeed events.
            
            CockroachDB changefeed returns different columns based on options:
                - Without 'updated': (key, value) - 2 columns
                - With 'updated': (key, value, updated, topic) - 4 columns
            """
            changefeed_cursor = self.conn.cursor()
            changefeed_cursor.execute(changefeed_query)
            
            event_count = 0
            last_resolved = None
            
            # Detect if 'updated' option is used (affects result structure)
            has_updated = initial_scan.lower() != "only"  # 'only' mode doesn't support 'updated'
            
            try:
                for row in changefeed_cursor:
                    if has_updated:
                        # Format with 'updated': (table, key, value, updated, topic)
                        table_name = row[0]
                        key_json = row[1]
                        value_json = row[2]
                        updated = row[3]
                        topic = row[4] if len(row) > 4 else None
                        
                        # Check if this is a resolved timestamp event
                        if key_json is None and value_json is None:
                            # Resolved timestamp row - use as watermark
                            last_resolved = updated
                            continue
                    else:
                        # Format without 'updated': (table, key, value)
                        table_name = row[0]
                        key_json = row[1]
                        value_json = row[2]
                        updated = None  # No timestamp in initial_scan='only' mode
                        topic = None
                    
                    # Parse JSON columns (handle None, empty strings, and memoryview objects)
                    # psycopg2 may return bytea columns as memoryview objects
                    def parse_json_column(json_data):
                        """Parse JSON from string, bytes, or memoryview."""
                        if json_data is None:
                            return None
                        
                        # Convert memoryview or bytes to string
                        if isinstance(json_data, memoryview):
                            json_data = json_data.tobytes().decode('utf-8')
                        elif isinstance(json_data, bytes):
                            json_data = json_data.decode('utf-8')
                        
                        # Check for empty or whitespace-only strings
                        if not isinstance(json_data, str) or json_data.strip() == '':
                            return None
                        
                        try:
                            return json.loads(json_data)
                        except json.JSONDecodeError:
                            return None
                    
                    key = parse_json_column(key_json) or []
                    value = parse_json_column(value_json)
                    
                    # Transform to standardized format using native columns
                    transformed_event = self._transform_changefeed_event_native(
                        key, value, updated, include_diff
                    )
                    yield transformed_event
                    
                    event_count += 1
                    
                    # Stop after batch_size events
                    if event_count >= batch_size:
                        break
            finally:
                changefeed_cursor.close()
            
            # Return the last resolved timestamp as the new cursor
            return last_resolved
        
        # Create iterator and collect events
        events = []
        last_resolved = None
        
        try:
            gen = event_generator()
            for event in gen:
                events.append(event)
        except StopIteration as e:
            last_resolved = e.value if hasattr(e, 'value') else None
        
        # Determine end offset
        end_offset = start_offset.copy() if start_offset else {}
        if last_resolved:
            end_offset["cursor"] = last_resolved
        
        return iter(events), end_offset
    
    def _transform_changefeed_event_native(
        self, key: list, value: Dict, updated: str, include_diff: bool
    ) -> Dict:
        """
        Transform CockroachDB's native changefeed columns into Lakeflow CDC format.
        
        CockroachDB changefeeds natively return different columns based on options:
            - Without 'updated' option: (key, value) - 2 columns
            - With 'updated' option: (key, value, updated, topic) - 4 columns
        
        This method maps them to Lakeflow CDC conventions (fields added by connector):
            - _cdc_key: Mapped from 'key' column
            - _cdc_updated: Mapped from 'updated' column (or None if not available)
            - _cdc_operation: Derived from 'value' column content
        
        Args:
            key: Primary key values (from changefeed 'key' column)
            value: Row data (from changefeed 'value' column)
            updated: MVCC timestamp (from changefeed 'updated' column, or None for snapshot queries)
            include_diff: Whether diff (before/after) is included
        
        Returns:
            Dictionary with original columns + connector-added CDC metadata fields
        """
        result = {}
        
        # Add CDC metadata fields (connector-added, not in source database)
        result["_cdc_key"] = key                    # Mapped from changefeed 'key' column
        result["_cdc_updated"] = updated            # Mapped from changefeed 'updated' column (None in snapshot mode)
        
        # Derive operation type from changefeed 'value' column content
        # (This field is added by the connector for Lakeflow CDC compatibility)
        if value is None:
            # DELETE operation - changefeed returns null value for deletes
            result["_cdc_operation"] = "DELETE"
            # For deletes, we only have the key (no row data)
        elif include_diff and "before" in value:
            # UPDATE operation (has both before and after)
            result["_cdc_operation"] = "UPDATE"
            after = value.get("after", {})
            result.update(after)  # Add original table columns
        else:
            # INSERT or UPDATE (only after value available)
            after = value.get("after") if value else None
            if after:
                result["_cdc_operation"] = "INSERT"  # Could be UPDATE without diff
                result.update(after)  # Add original table columns
        
        return result
    
    def __del__(self):
        """Close connection when object is destroyed."""
        if self.conn and not self.conn.closed:
            self.conn.close()

