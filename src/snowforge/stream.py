from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional

from snowforge.utilities import sql_format_dict, sql_quote_comment


class StreamMode(str, Enum):
    """Represents different modes for a stream."""

    APPEND_ONLY = "APPEND_ONLY"
    DEFAULT = "DEFAULT"
    INSERT_ONLY = "INSERT_ONLY"

    def __str__(self) -> str:
        """Returns the SQL representation of the stream mode."""
        return self.value


class StreamType(str, Enum):
    """Represents different types of streams."""

    DELTA = "DELTA"
    STANDARD = "STANDARD"

    def __str__(self) -> str:
        """Returns the SQL representation of the stream type."""
        return self.value


@dataclass
class Stream:
    """
    Represents a Snowflake stream configuration.

    A stream provides change data capture (CDC) for tables and views,
    allowing you to track changes made to the source object.

    Attributes:
        name: The name of the stream
        source: The source object for the stream
        append_only: Whether the stream is append-only
        comment: The comment for the stream
        insert_only: Whether the stream is insert-only
        is_create_if_not_exists: Whether the stream is created only if it doesn't exist
        is_create_or_replace: Whether the stream is created or replaced
        mode: The mode of the stream
        show_initial_rows: Whether to show initial rows
        tags: The tags for the stream
        type: The type of the stream
    """

    name: str
    source: str
    append_only: bool = False
    comment: Optional[str] = None
    insert_only: bool = False
    is_create_if_not_exists: bool = False
    is_create_or_replace: bool = False
    mode: Optional[StreamMode] = None
    show_initial_rows: bool = False
    tags: Dict[str, str] = field(default_factory=dict)
    type: Optional[StreamType] = None

    @classmethod
    def builder(cls, name: str) -> StreamBuilder:
        """Creates a new StreamBuilder instance."""
        return StreamBuilder(name=name)

    def to_sql(self) -> str:
        """Generates the SQL statement for the stream."""
        parts = []

        if self.is_create_or_replace:
            parts.append("CREATE OR REPLACE")
        else:
            parts.append("CREATE")

        parts.append("STREAM")

        if self.is_create_if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.name)

        if self.tags:
            parts.append(f"WITH TAG {sql_format_dict(self.tags)}")

        parts.append(f"ON TABLE {self.source}")

        if self.append_only:
            parts.append("APPEND_ONLY = TRUE")

        if self.show_initial_rows:
            parts.append("SHOW_INITIAL_ROWS = TRUE")

        if self.comment:
            parts.append(f"COMMENT = {sql_quote_comment(self.comment)}")

        return " ".join(parts)


class StreamBuilder:
    """Builder for Stream configuration."""

    def __init__(self, name: str):
        """Initializes a new StreamBuilder instance."""
        self.append_only: bool = False
        self.comment: Optional[str] = None
        self.insert_only: bool = False
        self.is_create_if_not_exists: bool = False
        self.is_create_or_replace: bool = False
        self.mode: Optional[StreamMode] = None
        self.name = name
        self.show_initial_rows: bool = False
        self.source: Optional[str] = None
        self.tags: Dict[str, str] = {}
        self.type: Optional[StreamType] = None

    def build(self) -> Stream:
        """Builds and returns a new Stream instance."""
        if self.name is None or self.name == "":
            raise ValueError("name must be set")
        if self.source is None or self.source == "":
            raise ValueError("source must be set")

        return Stream(
            append_only=self.append_only,
            comment=self.comment,
            insert_only=self.insert_only,
            is_create_if_not_exists=self.is_create_if_not_exists,
            is_create_or_replace=self.is_create_or_replace,
            mode=self.mode,
            name=self.name,
            show_initial_rows=self.show_initial_rows,
            source=self.source,
            tags=self.tags,
            type=self.type,
        )

    def with_append_only(self, append_only: bool = True) -> StreamBuilder:
        """Sets whether the stream is append-only."""
        self.append_only = append_only
        return self

    def with_comment(self, comment: str) -> StreamBuilder:
        """Sets the stream comment."""
        self.comment = comment
        return self

    def with_create_or_replace(self) -> StreamBuilder:
        """Sets the stream to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> StreamBuilder:
        """Sets the stream to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def with_insert_only(self, insert_only: bool = True) -> StreamBuilder:
        """Sets whether the stream is insert-only."""
        self.insert_only = insert_only
        return self

    def with_mode(self, mode: StreamMode) -> StreamBuilder:
        """Sets the stream mode."""
        self.mode = mode
        return self

    def with_show_initial_rows(self, show_initial_rows: bool = True) -> StreamBuilder:
        """Sets whether to show initial rows."""
        self.show_initial_rows = show_initial_rows
        return self

    def with_source(self, source: str) -> StreamBuilder:
        """Sets the source object."""
        self.source = source
        return self

    def with_tags(self, tags: Dict[str, str]) -> StreamBuilder:
        """Sets the stream tags."""
        self.tags = tags
        return self

    def with_type(self, stream_type: StreamType) -> StreamBuilder:
        """Sets the stream type."""
        self.type = stream_type
        return self
