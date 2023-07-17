import json
import sys

from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import psycopg

from loguru import logger
from psycopg import Cursor
from psycopg.errors import ForeignKeyViolation
from psycopg.sql import SQL, Composed, Identifier, Literal
from pydantic import PostgresDsn

from pgpipe.config import Settings
from pgpipe.models import ChunkMetaData, ChunkType, Status, TableSchema
from pgpipe.queries import data_type_query, first_chunk_element_query, last_chunk_element_query, primary_key_query


class PgPipe:
    """
    PgPipe: Synchronize data between two PostgreSQL databases.

    PgPipe is responsible for synchronizing data between two PostgreSQL databases. It is designed to handle large
    amounts of data. The synchronization process is divided into three main steps:

    1. Read source database metadata (such as table schemas, row counts and primary keys). Metadata should be manually
    reviewed to ensure the correct chunking types have been selected by PgPipe. The order of tables should be
    rearranged to mitigate foreign key constraint errors.
    2. Prepare chunk metadata, which helps track the transfer progress and allows the process to be restarted from
    checkpoints if needed.
    3. Start the data transfer, transferring data sequentially based on the prepared metadata.

    It is crucial to ensure the correct order of tables to satisfy foreign key dependencies. This can be achieved by
    manually adjusting the index of metadata entries. If your tables cannot be transferred independently this tool will
    not work for your use case, because table are transferred in chunks.

    The tool supports two types of chunking methods for data synchronization: offset-based and time-range-based
    chunking. The selection of the chunking method depends on the primary key. If the first element of the primary key
    is of a timestamp type, the tool will use time-range-based chunking. In other cases, offset-based chunking will be
    applied. If there is no unique primary index, the tool will deactivate chunking during the data transfer process.
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        logger.remove()
        logger.add(sys.stderr, level=settings.log_level)
        logger.debug(settings)

        self._db_schema: Optional[DbSchema] = None
        self._chunks: Optional[Chunks] = None

    def create_db_schema(self):
        """
        Generates and saves the schema of the source database.

        This method retrieves the schema of the source database and saves it into a DbSchema object. The DbSchema object
        includes metadata about tables in the database such as their names, schemas, row counts, and primary keys.
        This information is then saved to a file whose path is specified in the settings.

        This function does not return anything. If the function is called when the schema has already been generated,
        the old schema will be overwritten.
        """
        db_schema_data = DbSchema.retrieve_db_schema(self._settings.pgpipe_source_dsn, self._settings.schemas)
        self._db_schema = DbSchema(db_schema_data, self._settings.db_schema_path)
        self._db_schema.safe()

    def prepare_chunks(self):
        """
        Prepares chunk metadata for the data transfer.

        This method prepares chunk metadata based on the source database schema. The chunk metadata includes information
        about the chunks of data that will be transferred from the source database to the destination database. Each
        chunk represents a portion of a table in the database. The size of the chunks is determined by the settings.

        This function does not return anything. If the function is called when chunk metadata has already been
        generated, the old chunk metadata will be overwritten.
        """
        if self._db_schema is None:
            self._db_schema = DbSchema.load(self._settings.db_schema_path)

        chunk_meta_data = Chunks.prepare_chunks(
            self._db_schema.schema_data, self._settings.chunk_size_offset, self._settings.chunk_size_timedelta
        )

        self._chunks = Chunks(chunk_meta_data, self._settings.chunk_data_path)
        self._chunks.safe()

    def transfer_data(self):
        """
        Transfers data from the source database to the destination database.

        This method transfers data from the source database to the destination database in chunks based on the chunk
        metadata. The data is transferred in the order specified in the chunk metadata. If a ForeignKeyViolation error
        occurs during the data transfer, the error is logged and raised.

        This function does not return anything. If the function is called when there is no chunk metadata or schema
        information, these will be loaded from files whose paths are specified in the settings.
        """
        if self._db_schema is None:
            self._db_schema = DbSchema.load(self._settings.db_schema_path)

        if self._chunks is None:
            self._chunks = Chunks.load(self._settings.chunk_data_path)

        for chunk in self._chunks.items():
            logger.info(
                f"Transferring data from {chunk.db_schema}.{chunk.table} for chunk: "
                f"{chunk.range_from} -> {chunk.range_to}"
            )

            try:
                self._transfer_copy(
                    source_dsn=self._settings.pgpipe_source_dsn,
                    destination_dsn=self._settings.pgpipe_destination_dsn,
                    chunk=chunk,
                )
            except ForeignKeyViolation as e:
                logger.error("Foreign Key Violation. This could be solved by reordering the db schema file.")
                raise e

    @classmethod
    def _transfer_copy(cls, source_dsn: PostgresDsn, destination_dsn: PostgresDsn, chunk: ChunkMetaData):
        """
        Transfers a chunk of data from the source database to the destination database.

        This method generates the appropriate SQL queries to fetch a chunk of data from the source database and
        insert it into the destination database, based on the chunk's type and properties. The chunk type can be
        either Offset, Time, or Deactivate, which determine the chunking strategy used.

        If the chunk type is Offset, data is fetched and inserted based on row offsets. If the chunk type is Time,
        data is fetched and inserted based on time ranges. If the chunk type is Deactivate, all data from the specified
        table is fetched and inserted without any chunking.

        The data transfer process is performed using the psycopg library's copy functionality for efficient data
        transfer. This method does not return anything.

        :param source_dsn: The data source name of the source database.
        :param destination_dsn: The data source name of the destination database.
        :param chunk: The metadata of the chunk to be transferred.
        :raises ValueError: If the chunk type is unknown.
        """
        # Offset based chunking
        if chunk.type == ChunkType.Offset:
            if (
                not isinstance(chunk.index, list)
                or not isinstance(chunk.range_from, int)
                or not isinstance(chunk.range_to, int)
            ):
                raise TypeError("Incompatible type of chunk")
            sql_source, sql_destination = cls._transfer_offset_chunk_query(
                schema=chunk.db_schema,
                table=chunk.table,
                index=chunk.index,
                offset=int(chunk.range_from),
                limit=int(chunk.range_to - chunk.range_from + 1),
            )

        # Time based chunking
        elif chunk.type == ChunkType.Time:
            if (
                not isinstance(chunk.index, str)
                or not isinstance(chunk.range_from, datetime)
                or not isinstance(chunk.range_to, datetime)
            ):
                raise TypeError("Incompatible type of chunk")
            sql_source, sql_destination = cls._transfer_time_chunk_query(
                schema=chunk.db_schema,
                table=chunk.table,
                index=chunk.index,
                range_from=chunk.range_from,
                range_to=chunk.range_to,
            )

        # Chunking deactivated
        elif chunk.type == ChunkType.Deactivate:
            sql_source, sql_destination = cls._transfer_no_chunk_query(schema=chunk.db_schema, table=chunk.table)

        else:
            raise ValueError(f"Unknown chunk type {chunk.type}")

        with psycopg.connect(source_dsn.__str__()) as conn_s, psycopg.connect(destination_dsn.__str__()) as conn_d:
            with conn_s.cursor().copy(sql_source) as copy_s, conn_d.cursor().copy(sql_destination) as copy_d:
                for data in copy_s:
                    copy_d.write(data)

    @staticmethod
    def _transfer_offset_chunk_query(
        schema: str, table: str, index: List[str], offset: int, limit: int
    ) -> Tuple[Composed, Composed]:
        """
        Generates SQL queries for offset-based chunking.

        This method generates a pair of SQL queries for copying a chunk of data from a source table to a destination
        table. The chunk is determined by an offset and a limit, and rows are ordered by the specified index. The first
        query is used to select and copy data from the source table, and the second query is used to copy data into
        the destination table.

        :param schema: The schema of the table.
        :param table: The name of the table.
        :param index: The index to order the rows by.
        :param offset: The offset to start the chunk from.
        :param limit: The maximum number of rows in the chunk.
        :return: A pair of SQL queries for copying data from the source table to the destination table.
        """
        query_part_a = SQL("COPY (") + SQL("SELECT * FROM {}.{}").format(Identifier(schema), Identifier(table))

        if len(index) > 0:
            query_part_b = SQL(" ORDER BY ") + SQL(", ").join([Identifier(key) for key in index])
        else:
            query_part_b = SQL("") + SQL("")

        query_part_c = SQL(" OFFSET {} LIMIT {}").format(offset, limit) + SQL(") TO STDOUT (FORMAT BINARY);")

        sql_source = query_part_a + query_part_b + query_part_c
        sql_destination = SQL("COPY {}.{} FROM STDIN (FORMAT BINARY);").format(Identifier(schema), Identifier(table))

        return sql_source, sql_destination

    @staticmethod
    def _transfer_time_chunk_query(
        schema: str, table: str, index: str, range_from: datetime, range_to: datetime
    ) -> Tuple[Composed, Composed]:
        """
        Generates SQL queries for time-based chunking.

        This method generates a pair of SQL queries for copying a chunk of data from a source table to a destination
        table. The chunk is determined by a time range, and rows are filtered by the specified index. The first query
        is used to select and copy data from the source table, and the second query is used to copy data into the
        destination table.

        :param schema: The schema of the table.
        :param table: The name of the table.
        :param index: The index to filter the rows by.
        :param range_from: The start of the time range.
        :param range_to: The end of the time range.
        :return: A pair of SQL queries for copying data from the source table to the destination table.
        """
        sql_source = (
            SQL("COPY (")
            + SQL("SELECT * FROM {}.{}").format(Identifier(schema), Identifier(table))
            + SQL(" WHERE {} >= {}").format(Identifier(index), Literal(range_from))
            + SQL(" AND {} < {}").format(Identifier(index), Literal(range_to))
            + SQL(") TO STDOUT (FORMAT BINARY);")
        )

        sql_destination = SQL("COPY {}.{} FROM STDIN (FORMAT BINARY);").format(Identifier(schema), Identifier(table))

        return sql_source, sql_destination

    @staticmethod
    def _transfer_no_chunk_query(schema: str, table: str) -> Tuple[Composed, Composed]:
        """
        Generates SQL queries for transferring all data from a table.

        This method generates a pair of SQL queries for copying all data from a source table to a destination table.
        The first query is used to select and copy data from the source table, and the second query is used to copy
        data into the destination table.

        :param schema: The schema of the table.
        :param table: The name of the table.
        :return: A pair of SQL queries for copying data from the source table to the destination table.
        """
        sql_source = (
            SQL("COPY (")
            + SQL("SELECT * FROM {}.{}").format(Identifier(schema), Identifier(table))
            + SQL(") TO STDOUT (FORMAT BINARY);")
        )

        sql_destination = SQL("COPY {}.{} FROM STDIN (FORMAT BINARY);").format(Identifier(schema), Identifier(table))

        return sql_source, sql_destination


class DbSchema:
    """
    DbSchema: Handles operations related to the database schema.

    The DbSchema class is responsible for extracting, loading, and saving the schema of a database.
    It creates a metadata representation of a database schema that can be serialized to and from a JSON file.
    It also provides methods for extracting table schemas from a PostgreSQL database.

    Attributes:
        schema_data (List[TableSchema]): List of table schemas.
        _file_path (Path): The path to the file where the schema data is stored.
    """

    def __init__(self, schema_data: List[TableSchema], file_path: Path):
        """
        Initialize a DbSchema object.

        :param schema_data: List of table schemas.
        :param file_path: The path to the file where the schema data is stored.
        """
        self.schema_data = schema_data
        self._file_path = file_path

    @classmethod
    def retrieve_db_schema(cls, source_db_dsn: PostgresDsn, schemas: List[str]) -> List[TableSchema]:
        """
        Retrieve the schema of the database.

        :param source_db_dsn: The data source name (DSN) of the source database.
        :param schemas: List of schemas in the database.
        :return: A list of TableSchema objects representing the schema of the database.
        """
        db_schema = []
        with psycopg.connect(source_db_dsn.__str__()) as conn:
            with conn.cursor() as cur:
                for schema in schemas:
                    # fetch tables
                    cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s;", (schema,))
                    tables = [tn[0] for tn in cur.fetchall()]

                    for table in tables:
                        logger.info(f"Reading table schema from {schema}.{table}")

                        # query exact row count and primary_keys
                        count, primary_keys = cls._query_count_and_primary_keys(cur, schema, table)

                        # deactivate chunking if no unique index exists
                        if len(primary_keys) == 0:
                            logger.warning(f"No unique primary key found for {schema}.{table}! Chunking deactivated.")
                            db_schema.append(
                                TableSchema(
                                    db_schema=schema,
                                    table=table,
                                    chunk_type=ChunkType.Deactivate,
                                    index=None,
                                    first_row=None,
                                    last_row=None,
                                )
                            )
                            continue

                        # query data type lut of index
                        data_type_lut = cls._query_index_data_type_lut(cur, schema, table, primary_keys)

                        # set chunk type to time based chunking if
                        # data type of first primary index component is of type timestamp
                        if "timestamp" in data_type_lut[primary_keys[0]]:  # chunk_type=time
                            logger.info(
                                f"Detected timestamp in first element of primary key of {schema}.{table}, "
                                f"using chunk_type=time"
                            )

                            first_row, last_row = cls._query_time_range(cur, schema, table, primary_keys)

                            table_schema = TableSchema(
                                db_schema=schema,
                                table=table,
                                index=primary_keys[0],
                                first_row=first_row,
                                last_row=last_row,
                                chunk_type=ChunkType.Time,
                            )

                        # set chunk type to offset based chunking, index must be unique
                        else:
                            table_schema = TableSchema(
                                db_schema=schema,
                                table=table,
                                index=primary_keys,
                                first_row=0,
                                last_row=count,
                                chunk_type=ChunkType.Offset,
                            )
                        db_schema.append(table_schema)
        return db_schema

    @staticmethod
    def _query_count_and_primary_keys(cur: Cursor, schema: str, table: str) -> Tuple[int, List[str]]:
        """
        Query the count and primary keys of a table.

        :param cur: The cursor object.
        :param schema: The schema of the table.
        :param table: The name of the table.
        :return: A tuple containing the count of rows and a list of primary keys.
        """
        # query count
        cur.execute(SQL("SELECT COUNT(*) FROM {}.{};").format(Identifier(schema), Identifier(table)))
        res = cur.fetchone()
        if res is None:
            raise ConnectionError("Could not retrieve row count")
        count = res[0]

        # query primary key
        cur.execute(primary_key_query.format(schema + "." + table))
        primary_keys = [row[0] for row in cur.fetchall()]

        return count, primary_keys

    @staticmethod
    def _query_index_data_type_lut(cur: Cursor, schema: str, table: str, primary_keys: List[str]) -> Dict[str, str]:
        """
        Query the data types of the indexes of a table.

        :param cur: The cursor object.
        :param schema: The schema of the table.
        :param table: The name of the table.
        :param primary_keys: The list of primary keys of the table.
        :return: A dictionary with the index names as keys and their data types as values.
        """
        cur.execute(
            data_type_query.format(table, schema) + SQL(", ").join([Literal(pk) for pk in primary_keys]) + SQL(");")
        )
        data_type_lut = dict(cur.fetchall())

        return data_type_lut

    @staticmethod
    def _query_time_range(cur: Cursor, schema: str, table: str, primary_keys: List[str]) -> Tuple[datetime, datetime]:
        """
        Query the time range of a table.

        :param cur: The cursor object.
        :param schema: The schema of the table.
        :param table: The name of the table.
        :param primary_keys: The list of primary keys of the table.
        :return: A tuple containing the start and end time of the table.
        """
        cur.execute(
            first_chunk_element_query.format(
                Identifier(primary_keys[0]), Identifier(schema), Identifier(table), Identifier(primary_keys[0])
            )
        )
        res = cur.fetchone()
        if res is None:
            raise ConnectionError("Could not retrieve first timestamp")
        first_timestamp = res[0]
        first_timestamp_floor = datetime.combine(first_timestamp.date(), time(0, 0))

        cur.execute(
            last_chunk_element_query.format(
                Identifier(primary_keys[0]), Identifier(schema), Identifier(table), Identifier(primary_keys[0])
            )
        )
        res = cur.fetchone()
        if res is None:
            raise ConnectionError("Could not retrieve last timestamp")
        last_timestamp = res[0]
        last_timestamp_ceil = datetime.combine(last_timestamp.date() + timedelta(days=1), time(0, 0))

        return first_timestamp_floor, last_timestamp_ceil

    @classmethod
    def load(cls, file_path: Path) -> "DbSchema":
        """
        Load the database schema from a file.

        :param file_path: The path to the file where the schema data is stored.
        :return: A DbSchema object containing the loaded schema data.
        """
        try:
            with open(file_path) as f:
                data = json.load(f)
        except FileNotFoundError as e:
            logger.error("Source db schema file does not exist. Read db schema first!")
            raise e

        schema_data = [TableSchema(**ts) for ts in data]

        return cls(schema_data, file_path)

    def safe(self):
        """
        Save the schema data to a file.

        This method serializes the schema data to a JSON format and writes it to the file specified in self._file_path.
        """
        data = [json.loads(schema.json()) for schema in self.schema_data]

        with open(self._file_path, "w") as f:
            json.dump(data, f, indent=2)


class Chunks:
    """
    Chunks: Handles operations related to the chunks of a database.

    The Chunks class is responsible for managing the chunks of a database.
    It creates a metadata representation of the chunks that can be serialized to and from a JSON file.
    It also provides methods for preparing, loading, and saving chunk metadata.

    Attributes:
        meta_data (List[ChunkMetaData]): List of chunk metadata.
        _file_path (Path): The path to the file where the chunk metadata is stored.
    """

    def __init__(self, meta_data: List[ChunkMetaData], file_path: Path):
        """
        Initialize a Chunks object.

        :param meta_data: List of chunk metadata.
        :param file_path: The path to the file where the chunk metadata is stored.
        """
        self.meta_data = meta_data
        self._file_path = file_path

    def items(self) -> Iterator[ChunkMetaData]:
        """
        Generate an iterator over the chunk metadata.

        This method yields chunks that have not been processed yet, and sets their status to 'Done' once processed.
        The chunk metadata is saved after each chunk is processed.

        :return: An iterator over the chunk metadata.
        """
        for chunk in self.meta_data:
            if chunk.status == Status.Done:
                continue
            chunk.status = Status.InTransit
            yield chunk
            chunk.status = Status.Done
            self.safe()

    @classmethod
    def prepare_chunks(
        cls, db_schema: List[TableSchema], chunk_size_offset: int, chunk_size_timedelta: timedelta
    ) -> List[ChunkMetaData]:
        """
        Prepare the chunks based on the database schema and chunk sizes.

        This method creates chunk metadata for each table in the database schema. The type of chunking depends
        on the type specified in the table schema.

        :param db_schema: The database schema.
        :param chunk_size_offset: The size of offset-based chunks.
        :param chunk_size_timedelta: The size of time-based chunks.
        :return: A list of chunk metadata.
        """
        chunks = []
        for table in db_schema:
            # No chunking
            if table.chunk_type == ChunkType.Deactivate:
                logger.warning(
                    f"No unique index specified for {table.db_schema}.{table.table}! "
                    f"Chunking is deactivated for this table."
                )
                chunks.append(
                    ChunkMetaData(
                        db_schema=table.db_schema,
                        table=table.table,
                        type=ChunkType.Deactivate,
                        index=None,
                        range_from=None,
                        range_to=None,
                    )
                )
                continue

            # Offset based chunking
            elif table.chunk_type == ChunkType.Offset:
                if not isinstance(table.first_row, int) or not isinstance(table.last_row, int):
                    raise TypeError("Incompatible type of chunk")
                for range_from in range(table.first_row, table.last_row, chunk_size_offset):
                    range_to = range_from + chunk_size_offset - 1
                    chunk_meta_data = ChunkMetaData(
                        db_schema=table.db_schema,
                        table=table.table,
                        type=ChunkType.Offset,
                        index=table.index,
                        range_from=range_from,
                        range_to=range_to,
                    )
                    chunks.append(chunk_meta_data)

            # Time based chunking
            elif table.chunk_type == ChunkType.Time:
                if not isinstance(table.first_row, datetime) or not isinstance(table.last_row, datetime):
                    raise TypeError("Incompatible type of chunk")
                start_dt = table.first_row
                while start_dt <= table.last_row:
                    end_dt = start_dt + chunk_size_timedelta
                    chunk_meta_data = ChunkMetaData(
                        db_schema=table.db_schema,
                        table=table.table,
                        type=ChunkType.Time,
                        index=table.index,
                        range_from=start_dt,
                        range_to=end_dt,
                    )
                    chunks.append(chunk_meta_data)
                    start_dt += chunk_size_timedelta

            else:
                raise ValueError(f"Unknown chunk type {table.chunk_type}")

        return chunks

    @classmethod
    def load(cls, file_path: Path) -> "Chunks":
        """
        Load the chunk metadata from a file.

        :param file_path: The path to the file where the chunk metadata is stored.
        :return: A Chunks object containing the loaded chunk metadata.
        """
        try:
            with open(file_path) as f:
                data = json.load(f)
        except FileNotFoundError as e:
            logger.error("Chunk data file does not exist. Prepare chunks first!")
            raise e

        meta_data = [ChunkMetaData(**chunk) for chunk in data]

        return cls(meta_data, file_path)

    def safe(self):
        """
        Save the chunk metadata to a file.

        This method serializes the chunk metadata to a JSON format and writes it to the file specified in
        self._file_path.
        """
        data = [json.loads(chunk.model_dump_json()) for chunk in self.meta_data]

        with open(self._file_path, "w") as f:
            json.dump(data, f, indent=2)
