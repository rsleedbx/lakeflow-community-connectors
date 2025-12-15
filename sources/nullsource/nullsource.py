from typing import Dict, List, Iterator
import re
from pyspark.sql.types import StructType, StructField, LongType


class LakeflowConnect:
    """Minimal null source connector that returns a configurable number of rows."""
    
    def __init__(self, options: Dict[str, str]) -> None:
        """Initialize the null source connector."""
        self.options = options
        self.offset_pk = 0
        
        # Parse num_tables from connection options (default: 1)
        num_tables_str = options.get("num_tables", None)
        if num_tables_str is None:
            self.num_tables = 1
            print(f"nullsource: using default num_tables=1")
        else:
            self.num_tables = int(num_tables_str)
            print(f"nullsource: using num_tables={self.num_tables} from options")
        
        if self.num_tables <= 0:
            raise ValueError(f"num_tables must be positive, got {self.num_tables}")

    def _is_valid_table_name(self, table_name: str) -> bool:
        """Check if table name matches the expected pattern: 'intpk' or 'intpk_NNNNN' where NNNNN is 5-digit zero-padded number >= 2."""
        if table_name == "intpk":
            return True
        match = re.match(r'^intpk_(\d{5})$', table_name)
        if match:
            table_num = int(match.group(1))
            return table_num >= 2
        return False
    
    def _get_table_list(self, num_tables: int) -> List[str]:
        """Helper to generate table list based on num_tables."""
        if num_tables == 1:
            return ["intpk"]
        else:
            # First table is "intpk", additional tables are "intpk_00002", "intpk_00003", etc.
            return ["intpk"] + [f"intpk_{i:05d}" for i in range(2, num_tables + 1)]
    
    def list_tables(self) -> List[str]:
        """Return a list of available tables based on num_tables connection option."""
        return self._get_table_list(self.num_tables)

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """Return the schema for the specified table."""
        if not self._is_valid_table_name(table_name):
            raise ValueError(
                f"Invalid table name: {table_name}. "
                f"Expected 'intpk' or 'intpk_NNNNN' where NNNNN is a 5-digit zero-padded number >= 2"
            )
        
        return StructType([
            StructField("pk", LongType(), False)
        ])

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, str]:
        """Return metadata for the specified table."""
        if not self._is_valid_table_name(table_name):
            raise ValueError(
                f"Invalid table name: {table_name}. "
                f"Expected 'intpk' or 'intpk_NNNNN' where NNNNN is a 5-digit zero-padded number >= 2"
            )
        
        return {
            "primary_keys": ["pk"],
            "ingestion_type": "append"
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read data from the specified table."""
        if not self._is_valid_table_name(table_name):
            raise ValueError(
                f"Invalid table name: {table_name}. "
                f"Expected 'intpk' or 'intpk_NNNNN' where NNNNN is a 5-digit zero-padded number >= 2"
            )
        
        # Get num_rows from table_options, default to 1000
        num_rows = int(table_options.get("num_rows", 1000))
        if num_rows <= 0:
            raise ValueError(f"num_rows must be positive, got {num_rows}")

        # Call the helper function to get the iterator
        data_iterator = self._read_helper(table_name, start_offset, num_rows=num_rows)

        # Calculate the next offset based on how many rows were produced
        current_offset = (
            int(start_offset.get("offset", self.offset_pk))
            if start_offset
            else self.offset_pk
        )
        next_offset = current_offset + num_rows

        return data_iterator, {"offset": next_offset}

    def _read_helper(
        self,
        table_name: str,
        start_offset: dict,
        num_rows: int = 1000,
    ) -> Iterator[dict]:
        """Generate rows for the specified table."""
        if not self._is_valid_table_name(table_name):
            raise ValueError(
                f"Invalid table name: {table_name}. "
                f"Expected 'intpk' or 'intpk_NNNNN' where NNNNN is a 5-digit zero-padded number >= 2"
            )

        current_offset = (
            int(start_offset.get("offset", self.offset_pk))
            if start_offset
            else self.offset_pk
        )

        for i in range(num_rows):
            yield {"pk": current_offset}
            current_offset += 1

