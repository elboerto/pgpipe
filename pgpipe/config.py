from datetime import timedelta
from pathlib import Path
from typing import List

from pydantic import PostgresDsn
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    log_level: str = "INFO"

    schemas: List[str] = ["public"]
    chunk_size_offset: int = 50000
    chunk_size_timedelta: timedelta = timedelta(days=1)

    db_schema_path: Path = Path("./db_schema.json")
    chunk_data_path: Path = Path("./chunk_data.json")

    pgpipe_source_dsn: PostgresDsn
    pgpipe_destination_dsn: PostgresDsn
