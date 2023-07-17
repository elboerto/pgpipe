from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pytest

from pgpipe.core import Chunks
from pgpipe.models import ChunkType, Status, TableSchema


@pytest.fixture
def sample_table_schemas() -> List[TableSchema]:
    return [
        TableSchema(
            db_schema="public",
            table="table1",
            index=["id"],
            chunk_type=ChunkType.Offset,
            first_row=1,
            last_row=1000,
        ),
        TableSchema(
            db_schema="public",
            table="table2",
            index=["timestamp"],
            chunk_type=ChunkType.Time,
            first_row=datetime(2022, 1, 1),
            last_row=datetime(2022, 1, 10),
        ),
        TableSchema(
            db_schema="public",
            table="table3",
            index=[],
            chunk_type=ChunkType.Deactivate,
            first_row=0,
            last_row=0,
        ),
    ]


def test_prepare_chunks(sample_table_schemas):
    chunk_size_offset = 200
    chunk_size_timedelta = timedelta(days=1)

    chunks = Chunks.prepare_chunks(sample_table_schemas, chunk_size_offset, chunk_size_timedelta)

    assert len(chunks) == 16

    for chunk in chunks:
        if chunk.table == "table1":
            assert chunk.type == ChunkType.Offset
            assert chunk.index == ["id"]
        elif chunk.table == "table2":
            assert chunk.type == ChunkType.Time
            assert chunk.index == ["timestamp"]
        elif chunk.table == "table3":
            assert chunk.type == ChunkType.Deactivate
            assert chunk.index is None


def test_chunks_items(sample_table_schemas):
    chunk_file = Path("test_chunks.json")
    chunk_size_offset = 200
    chunk_size_timedelta = timedelta(days=1)
    chunks_meta_data = Chunks.prepare_chunks(sample_table_schemas, chunk_size_offset, chunk_size_timedelta)
    chunks = Chunks(chunks_meta_data, chunk_file)

    for chunk in chunks.items():
        assert chunk.status == Status.InTransit

    for chunk in chunks.meta_data:
        assert chunk.status == Status.Done

    chunk_file.unlink()


def test_chunks_load_and_save(sample_table_schemas):
    chunk_size_offset = 200
    chunk_size_timedelta = timedelta(days=1)
    chunks_meta_data = Chunks.prepare_chunks(sample_table_schemas, chunk_size_offset, chunk_size_timedelta)
    chunks = Chunks(chunks_meta_data, Path("test_chunks.json"))

    # Save the chunks
    chunks.safe()

    # Load the chunks from the file
    loaded_chunks = Chunks.load(Path("test_chunks.json"))

    assert len(loaded_chunks.meta_data) == len(chunks.meta_data)
    for original_chunk, loaded_chunk in zip(chunks.meta_data, loaded_chunks.meta_data):
        assert original_chunk == loaded_chunk

    # Clean up the test file
    Path("test_chunks.json").unlink()
