from typing import Dict, List, Iterator, Any
import json
import os
import ssl
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DoubleType, BooleanType, DateType, TimestampType, BinaryType,
    DecimalType, ArrayType
)

# NOTE: Do NOT import pg8000 or psycopg2 at module level!
# They must be imported lazily inside _get_connection() to avoid Spark serialization issues
# pg8000 is installed at runtime in ingest.py, so it won't be available during module serialization


class LakeflowConnect:
    """CockroachDB connector using sinkless changefeeds for CDC."""
    
    # Class variable to share snapshot timestamp across tables in multi-table pipelines
    # This ensures all tables use the same timestamp for snapshot consistency
    _shared_snapshot_timestamp = None
    
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
            - schema: Schema name (default: 'public')
        """
        # Note: We do NOT create a connection in __init__ to avoid Spark serialization issues
        # Connection will be created lazily in methods that need it
        
        # Debug: Print what options we actually receive from Unity Catalog
        print("=" * 80)
        print("🔍 DEBUG: CockroachDB Connector __init__ called")
        print("=" * 80)
        print(f"Options received from Spark/Unity Catalog:")
        print(f"  Total options: {len(options)}")
        print(f"\nALL OPTIONS (raw dump - this is what Spark passes to the connector):")
        for key in sorted(options.keys()):
            # Mask sensitive fields for security
            if any(sensitive in key.lower() for sensitive in ["password", "token"]):
                value = "***REDACTED***"
            else:
                value = repr(options[key])
            print(f"  {key}: {value}")
        print("=" * 80)
        print(f"\nLooking for credentials in options...")
        print(f"  Has 'token'? {('token' in options)}")
        print(f"  Has 'base_url'? {('base_url' in options)}")
        print(f"  Has 'host'? {('host' in options)}")
        print("=" * 80)
        
        # Schema from connection (fixed by Unity Catalog)
        self.schema = options.get("schema", "public")
        
        # Try different credential modes
        token = options.get("token")
        base_url = options.get("base_url")
        
        if token and base_url:
            print(f"✓ Mode 1: GitHub-style parameters (token + base_url)")
            print(f"  token: {token.split(':')[0]}:*** (username:password)")
            print(f"  base_url: {base_url}")
            print(f"  Reconstructing full connection URL...")
            
            if base_url.startswith("postgresql://"):
                full_url = f"postgresql://{token}@{base_url[13:]}"
                print(f"  Reconstructed URL: postgresql://{token.split(':')[0]}:***@{base_url[13:]}")
                self._parse_connection_url(full_url)
            else:
                print(f"  ❌ Unexpected base_url format: {base_url}")
                raise ValueError(f"Invalid base_url format: {base_url}")
        
        elif options.get("host"):
            print("✓ Mode 2: Individual parameters (local testing mode)")
            self.host = options.get("host")
            self.port = int(options.get("port", "26257"))
            self.database = options.get("database")
            self.user = options.get("user")
            self.password = options.get("password", "")
            self.sslmode = options.get("sslmode", "require")
        
        else:
            print("❌ No recognized connection parameters found!")
            self.host = None
            self.database = None
            self.user = None
        
        print(f"  Note: Connection will be created lazily when needed (not in __init__)")
    
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
        
        print(f"  Parsed from connection_url:")
        print(f"    host: {self.host}")
        print(f"    port: {self.port}")
        print(f"    database: {self.database}")
        print(f"    user: {self.user}")
        print(f"    password: {'***' if self.password else '(empty)'}")
        print(f"    sslmode: {self.sslmode}")
    
    def _get_connection(self, table_options: Dict[str, str] = None):
        """Create and return a new connection to CockroachDB."""
        try:
            # LAZY IMPORT: Import drivers here (not at module level)
            import pg8000
            
            print(f"\n🔍 DEBUG: Creating connection using pg8000...")
            print(f"  host={self.host}, port={self.port}, database={self.database}")
            
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
            
            print("✅ Connected using pg8000 (pure Python driver)")
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
    
    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """Read table metadata."""
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
        """Read data from a CockroachDB table using changefeeds."""
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
        
        changefeed_options.append("split_column_families")
        
        if cursor and effective_initial_scan.lower() != "only":
            changefeed_options.append(f"cursor='{cursor}'")
        
        options_str = ", ".join(changefeed_options)
        target = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
        changefeed_query = f"EXPERIMENTAL CHANGEFEED FOR {target} WITH {options_str}"
        
        conn = self._get_connection(table_options)
        
        # Capture timestamp for multi-table consistency
        if effective_initial_scan.lower() == "only":
            is_multi_table = table_options.get("multi_table_pipeline", "false").lower() == "true"
            
            debug_cursor = self._create_cursor(conn)
            debug_cursor.execute("SELECT cluster_logical_timestamp()::string")
            current_ts = debug_cursor.fetchone()[0]
            debug_cursor.close()
            
            if is_multi_table and LakeflowConnect._shared_snapshot_timestamp:
                self._snapshot_start_timestamp = LakeflowConnect._shared_snapshot_timestamp
                print(f"\n💡 Reusing shared snapshot timestamp: {self._snapshot_start_timestamp}")
            else:
                self._snapshot_start_timestamp = current_ts
                print(f"\n💡 Captured snapshot start timestamp: {current_ts}")
                if is_multi_table:
                    LakeflowConnect._shared_snapshot_timestamp = current_ts
        
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
            except Exception as e:
                print(f"  ⚠️  Warning: Could not set statement_timeout: {e}")
            
            try:
                changefeed_cursor.execute(changefeed_query)
                print(f"✅ Query submitted")
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
                    print(f"\n✅ Changefeed timed out (EXPECTED - no changes)")
                    try:
                        changefeed_cursor.close()
                    except:
                        pass
                    return cursor
                else:
                    print(f"\n❌ Unexpected error: {e}")
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
                                print(f"   ✅ Caught up at: {last_resolved}")
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
                
                if is_timeout:
                    print(f"\n⏰ Query timed out (EXPECTED)")
                else:
                    print(f"\n❌ Error: {e}")
                    try:
                        changefeed_cursor.close()
                    except:
                        pass
                    raise
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
        
        end_offset = start_offset.copy() if start_offset else {}
        
        if last_resolved:
            end_offset["cursor"] = last_resolved
        elif hasattr(self, '_snapshot_start_timestamp') and self._snapshot_start_timestamp:
            end_offset["cursor"] = self._snapshot_start_timestamp
        
        # Print manual test command
        if end_offset.get("cursor"):
            print(f"💡 Manual Test:")
            print(f"psql $COCKROACHDB_URL << 'EOF'")
            print(f"EXPERIMENTAL CHANGEFEED FOR {table_name}")
            print(f"  WITH initial_scan='no', updated, resolved='1s',")
            print(f"       split_column_families, cursor='{end_offset['cursor']}';")
            print(f"EOF")
        
        return iter(events), end_offset
    
    def _transform_changefeed_event_native(
        self, key: list, value: Dict, updated: str
    ) -> Dict:
        """Transform CockroachDB changefeed event."""
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
