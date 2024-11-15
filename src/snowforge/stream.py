from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


class StreamMode(str, Enum):
    """Represents different modes for a stream."""

    APPEND_ONLY = "APPEND_ONLY"
    INSERT_ONLY = "INSERT_ONLY"
    DEFAULT = "DEFAULT"


class StreamType(str, Enum):
    """Represents different types of streams."""

    STANDARD = "STANDARD"
    DELTA = "DELTA"


@dataclass
class Stream:
    """
    Represents a Snowflake stream configuration.

    A stream provides change data capture (CDC) for tables and views,
    allowing you to track changes made to the source object.
    """

    name: str
    source: str
    mode: Optional[StreamMode] = None
    type: Optional[StreamType] = None
    insert_only: bool = False
    show_initial_rows: bool = False
    append_only: bool = False
    comment: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    is_create_or_replace: bool = False
    is_create_if_not_exists: bool = False

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
            tag_parts = [f"{k} = '{v}'" for k, v in self.tags.items()]
            parts.append(f"WITH TAG ({', '.join(tag_parts)})")

        parts.append(f"ON TABLE {self.source}")

        if self.append_only:
            parts.append("APPEND_ONLY = TRUE")

        if self.show_initial_rows:
            parts.append("SHOW_INITIAL_ROWS = TRUE")

        if self.comment:
            parts.append(f"COMMENT = '{self.comment.replace(chr(39), chr(39)*2)}'")

        return " ".join(parts)


class StreamBuilder:
    """Builder for Stream configuration."""

    def __init__(self, name: str):
        self.name = name
        self.source: Optional[str] = None
        self.mode: Optional[StreamMode] = None
        self.type: Optional[StreamType] = None
        self.insert_only: bool = False
        self.show_initial_rows: bool = False
        self.append_only: bool = False
        self.comment: Optional[str] = None
        self.tags: Dict[str, str] = {}
        self.is_create_or_replace: bool = False
        self.is_create_if_not_exists: bool = False

    def with_source(self, source: str) -> StreamBuilder:
        """Sets the source object."""
        self.source = source
        return self

    def with_mode(self, mode: StreamMode) -> StreamBuilder:
        """Sets the stream mode."""
        self.mode = mode
        return self

    def with_type(self, stream_type: StreamType) -> StreamBuilder:
        """Sets the stream type."""
        self.type = stream_type
        return self

    def with_insert_only(self, insert_only: bool = True) -> StreamBuilder:
        """Sets whether the stream is insert-only."""
        self.insert_only = insert_only
        return self

    def with_show_initial_rows(self, show_initial_rows: bool = True) -> StreamBuilder:
        """Sets whether to show initial rows."""
        self.show_initial_rows = show_initial_rows
        return self

    def with_append_only(self, append_only: bool = True) -> StreamBuilder:
        """Sets whether the stream is append-only."""
        self.append_only = append_only
        return self

    def with_comment(self, comment: str) -> StreamBuilder:
        """Sets the stream comment."""
        self.comment = comment
        return self

    def with_tags(self, tags: Dict[str, str]) -> StreamBuilder:
        """Sets the stream tags."""
        self.tags = tags
        return self

    def with_create_or_replace(self) -> StreamBuilder:
        """Sets the stream to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> StreamBuilder:
        """Sets the stream to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def build(self) -> Stream:
        """Builds and returns a new Stream instance."""
        if self.name is None:
            raise ValueError("name must be set")
        if self.source is None:
            raise ValueError("source must be set")

        return Stream(
            name=self.name,
            source=self.source,
            mode=self.mode,
            type=self.type,
            insert_only=self.insert_only,
            show_initial_rows=self.show_initial_rows,
            append_only=self.append_only,
            comment=self.comment,
            tags=self.tags,
            is_create_or_replace=self.is_create_or_replace,
            is_create_if_not_exists=self.is_create_if_not_exists,
        )
