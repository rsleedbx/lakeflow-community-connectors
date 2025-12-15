from typing import Dict, List, Iterator
from pyspark.sql.types import StructType, StructField, LongType


class LakeflowConnect:
    """Minimal null source connector that returns a configurable number of rows."""
    
    def __init__(self, options: Dict[str, str]) -> None:
        """Initialize the null source connector."""
        self.options = options
        self.offset_pk = 0

    def list_tables(self) -> List[str]:
        """Return a list of available tables."""
        return ["intpk"]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """Return the schema for the specified table."""
        if table_name != "intpk":
            raise ValueError(f"Unsupported table: {table_name}")
        
        return StructType([
            StructField("pk", LongType(), False)
        ])

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, str]:
        """Return metadata for the specified table."""
        if table_name != "intpk":
            raise ValueError(f"Unsupported table: {table_name}")
        
        return {
            "primary_keys": ["pk"],
            "ingestion_type": "append"
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read data from the specified table."""
        if table_name != "intpk":
            raise ValueError(f"Unsupported table: {table_name}")
        
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
        if table_name != "intpk":
            raise ValueError(f"Unsupported table: {table_name}")

        current_offset = (
            int(start_offset.get("offset", self.offset_pk))
            if start_offset
            else self.offset_pk
        )

        for i in range(num_rows):
            yield {"pk": current_offset}
            current_offset += 1

