import argparse
import os

from datetime import timedelta

from pgpipe.config import Settings
from pgpipe.core import PgPipe


def run():
    parser = argparse.ArgumentParser(description=PgPipe.__doc__)
    subparsers = parser.add_subparsers(dest="command")

    # create_db_schema command
    subparsers.add_parser("create_db_schema", help="Step 1: Create the database schema.")

    # prepare_chunks command
    subparsers.add_parser("prepare_chunks", help="Step 2: Prepare chunks for data transfer.")

    # transfer_data command
    subparsers.add_parser("transfer_data", help="Step 3: Transfer data between databases.")

    # Settings
    parser.add_argument("--log-level", default="INFO", help="Log level (default: INFO)")
    parser.add_argument("--schemas", default=["public"], nargs="+", help="List of schemas (default: public)")
    parser.add_argument("--chunk-size-offset", type=int, default=50000, help="Chunk size offset (default: 50000)")
    parser.add_argument("--chunk-size-timedelta", type=int, default=1, help="Chunk size timedelta in days (default: 1)")
    parser.add_argument(
        "--db-schema-path",
        type=str,
        default="./db_schema.json",
        help="Path to the DB schema file (default: ./db_schema.json)",
    )
    parser.add_argument(
        "--chunk-data-path",
        type=str,
        default="./chunk_data.json",
        help="Path to the chunk data file (default: ./chunk_data.json)",
    )
    parser.add_argument(
        "--source-dsn",
        type=str,
        default=None,
        help="Source database DSN, if not set it is read from env PGPIPE_SOURCE_DSN",
    )
    parser.add_argument(
        "--destination-dsn",
        type=str,
        default=None,
        help="Destination database DSN, if not set it is read from env PGPIPE_DESTINATION_DSN",
    )

    args = parser.parse_args()

    if args.source_dsn is None:
        args.source_dsn = os.environ["PGPIPE_SOURCE_DSN"]

    if args.destination_dsn is None:
        args.destination_dsn = os.environ["PGPIPE_DESTINATION_DSN"]

    settings = Settings(
        log_level=args.log_level,
        schemas=args.schemas,
        chunk_size_offset=args.chunk_size_offset,
        chunk_size_timedelta=timedelta(days=args.chunk_size_timedelta),
        db_schema_path=args.db_schema_path,
        chunk_data_path=args.chunk_data_path,
        pgpipe_source_dsn=args.source_dsn,
        pgpipe_destination_dsn=args.destination_dsn,
    )

    pgpipe = PgPipe(settings)

    if args.command == "create_db_schema":
        pgpipe.create_db_schema()
    elif args.command == "prepare_chunks":
        pgpipe.prepare_chunks()
    elif args.command == "transfer_data":
        pgpipe.transfer_data()
    else:
        parser.print_help()
