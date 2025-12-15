"""Manual test script for nullsource connector."""

from sources.nullsource.nullsource import LakeflowConnect


def test_nullsource():
    """Test the nullsource connector manually."""
    print("=" * 60)
    print("Testing nullsource connector")
    print("=" * 60)
    
    # Initialize connector
    connector = LakeflowConnect({})
    print("\n✓ Connector initialized")
    
    # Test list_tables
    tables = connector.list_tables()
    assert tables == ["intpk"], f"Expected ['intpk'], got {tables}"
    print(f"✓ list_tables() returned: {tables}")
    
    # Test get_table_schema
    table_options = {}
    schema = connector.get_table_schema("intpk", table_options)
    print(f"✓ get_table_schema() returned: {schema}")
    assert len(schema.fields) == 1, f"Expected 1 field, got {len(schema.fields)}"
    assert schema.fields[0].name == "pk", f"Expected field 'pk', got '{schema.fields[0].name}'"
    assert schema.fields[0].dataType.typeName() == "long", f"Expected type 'long', got '{schema.fields[0].dataType.typeName()}'"
    
    # Test read_table_metadata
    metadata = connector.read_table_metadata("intpk", table_options)
    assert metadata["primary_keys"] == ["pk"], f"Expected primary_keys=['pk'], got {metadata['primary_keys']}"
    assert metadata["ingestion_type"] == "append", f"Expected ingestion_type='append', got {metadata['ingestion_type']}"
    print(f"✓ read_table_metadata() returned: {metadata}")
    
    # Test read_table with num_rows
    table_options = {"num_rows": 10}
    data_iter, offset = connector.read_table("intpk", {}, table_options)
    rows = list(data_iter)
    assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"
    assert rows[0] == {"pk": 0}, f"Expected first row {{'pk': 0}}, got {rows[0]}"
    assert rows[-1] == {"pk": 9}, f"Expected last row {{'pk': 9}}, got {rows[-1]}"
    print(f"✓ read_table() with num_rows=10 returned {len(rows)} rows")
    print(f"  First row: {rows[0]}")
    print(f"  Last row: {rows[-1]}")
    print(f"  Offset: {offset}")
    
    # Test incremental read with offset
    data_iter2, offset2 = connector.read_table("intpk", offset, table_options)
    rows2 = list(data_iter2)
    assert len(rows2) == 10, f"Expected 10 rows in second read, got {len(rows2)}"
    assert rows2[0] == {"pk": 10}, f"Expected first row {{'pk': 10}}, got {rows2[0]}"
    assert rows2[-1] == {"pk": 19}, f"Expected last row {{'pk': 19}}, got {rows2[-1]}"
    print(f"✓ Second read with offset={offset} returned {len(rows2)} rows")
    print(f"  First row: {rows2[0]}")
    print(f"  Last row: {rows2[-1]}")
    print(f"  New offset: {offset2}")
    
    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    test_nullsource()

