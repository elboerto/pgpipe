from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel


class ChunkType(Enum):
    Offset = "offset"
    Time = "time"
    Deactivate = "deactivate"


class TableSchema(BaseModel):
    db_schema: str
    table: str
    index: Optional[Union[str, List[str]]]
    first_row: Optional[Union[int, datetime]]
    last_row: Optional[Union[int, datetime]]
    chunk_type: ChunkType


class Status(Enum):
    ToDo = "todo"
    InTransit = "in_transit"
    Done = "done"


class ChunkMetaData(BaseModel):
    db_schema: str
    table: str
    type: ChunkType
    index: Optional[Union[str, List[str]]]
    range_from: Optional[Union[int, datetime]]
    range_to: Optional[Union[int, datetime]]
    status: Status = Status.ToDo
