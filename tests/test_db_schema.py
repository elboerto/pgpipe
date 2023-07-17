from unittest.mock import MagicMock, patch

from pgpipe.core import ChunkType, DbSchema


@patch("pgpipe.core.psycopg.connect")
def test_retrieve_db_schema(mock_connect):
    # Mock the psycopg connection and cursor
    mock_conn = MagicMock()
    mock_cur = MagicMock()

    # Set the return values for the mock objects
    mock_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock the database schema
    schema = "public"
    table = "test_table"
    row_count = (10,)
    primary_keys = ["id"]
    data_type_lut = {"id": "int"}
    first_row = 0
    last_row = row_count[0]

    # Set the return values for the cursor's execute and fetchall methods
    mock_cur.fetchone.side_effect = [row_count]

    mock_cur.fetchall.side_effect = [
        [(table,)],  # Return the table name
        [primary_keys],  # Return the primary keys
        data_type_lut,  # Return the data types of the primary keys
    ]

    # Call the retrieve_db_schema method
    db_schema = DbSchema.retrieve_db_schema("source_db_dsn", [schema])

    # Check that the returned DbSchema object contains the correct data
    assert len(db_schema) == 1
    table_schema = db_schema[0]

    assert table_schema.db_schema == schema
    assert table_schema.table == table
    assert table_schema.index == primary_keys
    assert table_schema.first_row == first_row
    assert table_schema.last_row == last_row
    assert table_schema.chunk_type == ChunkType.Offset
