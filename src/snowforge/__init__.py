"""Snowforge: A Python library for managing Snowflake workflows."""

from .copy_into import CopyInto, CopyIntoSource, CopyIntoTarget
from .file_format import FileFormat, FileFormatBuilder
from .forge import Forge, SnowflakeConfig
from .put import InternalStage, Put
from .stage import Stage, StorageIntegration
from .stream import Stream, StreamBuilder
from .table import Table, TableBuilder
from .task import Task, TaskType, WarehouseSize

__version__ = "0.1.0-alpha.1"

__all__ = [
    "Forge",
    "SnowflakeConfig",
    "CopyInto",
    "CopyIntoSource",
    "CopyIntoTarget",
    "FileFormat",
    "FileFormatBuilder",
    "Put",
    "InternalStage",
    "Stage",
    "StorageIntegration",
    "Stream",
    "StreamBuilder",
    "Table",
    "TableBuilder",
    "Task",
    "TaskType",
    "WarehouseSize",
]
