from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional


class CopyIntoTarget:
    """Represents the target of a COPY INTO statement."""

    def __init__(self, name: str, target_type: str):
        self.name = name
        self.target_type = target_type

    @classmethod
    def table(cls, name: str) -> CopyIntoTarget:
        """Creates a table target."""
        return cls(name, "table")

    @classmethod
    def stage(cls, name: str) -> CopyIntoTarget:
        """Creates a stage target."""
        return cls(name, "stage")

    def to_sql(self) -> str:
        return self.name


class CopyIntoSource:
    """Represents the source of a COPY INTO statement."""

    def __init__(self, name: str, source_type: str):
        self.name = name
        self.source_type = source_type

    @classmethod
    def table(cls, name: str) -> CopyIntoSource:
        """Creates a table source."""
        return cls(name, "table")

    @classmethod
    def stage(cls, name: str) -> CopyIntoSource:
        """Creates a stage source."""
        return cls(name, "stage")

    def to_sql(self) -> str:
        return self.name


@dataclass
class CopyIntoOptions:
    """Options for the COPY INTO command."""

    pattern: Optional[str] = None
    file_format: Optional[str] = None
    files: Optional[List[str]] = None
    pattern_type: Optional[str] = None
    validation_mode: Optional[str] = None
    return_failed_only: bool = False
    on_error: Optional[str] = None
    size_limit: Optional[int] = None
    purge: bool = False
    match_by_column_name: bool = False
    enforce_length: bool = False
    truncatecolumns: bool = False
    force: bool = False

    @classmethod
    def builder(cls) -> 'CopyIntoOptionsBuilder':
        """Creates a new CopyIntoOptionsBuilder instance."""
        return CopyIntoOptionsBuilder()

    def to_sql(self) -> str:
        parts = []

        if self.pattern:
            parts.append(f"PATTERN = '{self.pattern}'")
        if self.file_format:
            parts.append(f"FILE_FORMAT = {self.file_format}")
        if self.files:
            files_str = ", ".join(f"'{f}'" for f in self.files)
            parts.append(f"FILES = ({files_str})")
        if self.pattern_type:
            parts.append(f"PATTERN_TYPE = '{self.pattern_type}'")
        if self.validation_mode:
            parts.append(f"VALIDATION_MODE = {self.validation_mode}")
        if self.return_failed_only:
            parts.append("RETURN_FAILED_ONLY = TRUE")
        if self.on_error:
            parts.append(f"ON_ERROR = {self.on_error}")
        if self.size_limit:
            parts.append(f"SIZE_LIMIT = {self.size_limit}")
        if self.purge:
            parts.append("PURGE = TRUE")
        if self.match_by_column_name:
            parts.append("MATCH_BY_COLUMN_NAME = TRUE")
        if self.enforce_length:
            parts.append("ENFORCE_LENGTH = TRUE")
        if self.truncatecolumns:
            parts.append("TRUNCATECOLUMNS = TRUE")
        if self.force:
            parts.append("FORCE = TRUE")

        return " ".join(parts)


class CopyIntoOptionsBuilder:
    """Builder for CopyIntoOptions."""

    def __init__(self):
        self.pattern: Optional[str] = None
        self.file_format: Optional[str] = None
        self.files: Optional[List[str]] = None
        self.pattern_type: Optional[str] = None
        self.validation_mode: Optional[str] = None
        self.return_failed_only: bool = False
        self.on_error: Optional[str] = None
        self.size_limit: Optional[int] = None
        self.purge: bool = False
        self.match_by_column_name: bool = False
        self.enforce_length: bool = False
        self.truncatecolumns: bool = False
        self.force: bool = False

    def with_pattern(self, pattern: str) -> 'CopyIntoOptionsBuilder':
        """Sets the pattern for matching files."""
        self.pattern = pattern
        return self

    def with_file_format(self, file_format: str) -> 'CopyIntoOptionsBuilder':
        """Sets the file format specification."""
        self.file_format = file_format
        return self

    def with_files(self, files: List[str]) -> 'CopyIntoOptionsBuilder':
        """Sets the specific files to load."""
        self.files = files
        return self

    def with_pattern_type(self, pattern_type: str) -> 'CopyIntoOptionsBuilder':
        """Sets the pattern type."""
        self.pattern_type = pattern_type
        return self

    def with_validation_mode(self, validation_mode: str) -> 'CopyIntoOptionsBuilder':
        """Sets the validation mode."""
        self.validation_mode = validation_mode
        return self

    def with_return_failed_only(
        self, return_failed_only: bool
    ) -> 'CopyIntoOptionsBuilder':
        """Sets whether to return only failed records."""
        self.return_failed_only = return_failed_only
        return self

    def with_on_error(
        self,
        on_error: Literal[
            "CONTINUE",
            "SKIP_FILE",
            "SKIP_FILE_<num>",
            "SKIP_FILE_<num>%",
            "ABORT_STATEMENT",
        ],
    ) -> 'CopyIntoOptionsBuilder':
        """Sets the on_error behavior."""
        self.on_error = on_error
        return self

    def with_size_limit(self, size_limit: int) -> 'CopyIntoOptionsBuilder':
        """Sets the size limit for data to be loaded."""
        self.size_limit = size_limit
        return self

    def with_purge(self, purge: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to purge files after loading."""
        self.purge = purge
        return self

    def with_match_by_column_name(
        self, match_by_column_name: bool
    ) -> 'CopyIntoOptionsBuilder':
        """Sets whether to match columns by name."""
        self.match_by_column_name = match_by_column_name
        return self

    def with_enforce_length(self, enforce_length: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to enforce length constraints."""
        self.enforce_length = enforce_length
        return self

    def with_truncate_columns(self, truncatecolumns: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to truncate columns."""
        self.truncatecolumns = truncatecolumns
        return self

    def with_force(self, force: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to force the operation."""
        self.force = force
        return self

    def build(self) -> CopyIntoOptions:
        """Builds and returns a new CopyIntoOptions instance."""
        return CopyIntoOptions(
            pattern=self.pattern,
            file_format=self.file_format,
            files=self.files,
            pattern_type=self.pattern_type,
            validation_mode=self.validation_mode,
            return_failed_only=self.return_failed_only,
            on_error=self.on_error,
            size_limit=self.size_limit,
            purge=self.purge,
            match_by_column_name=self.match_by_column_name,
            enforce_length=self.enforce_length,
            truncatecolumns=self.truncatecolumns,
            force=self.force,
        )


@dataclass
class CopyInto:
    """
    Represents a COPY INTO statement.

    This class encapsulates all the information needed to construct
    a COPY INTO statement, including the target, source, and options.
    """

    target: CopyIntoTarget
    source: CopyIntoSource
    options: CopyIntoOptions = CopyIntoOptions()

    @classmethod
    def builder(cls) -> 'CopyIntoBuilder':
        """Creates a new CopyIntoBuilder instance."""
        return CopyIntoBuilder()

    def to_sql(self) -> str:
        """Generates the SQL statement for the COPY INTO command."""
        parts = ["COPY INTO", self.target.to_sql(), "FROM", self.source.to_sql()]

        options_sql = self.options.to_sql()
        if options_sql:
            parts.append(options_sql)

        return " ".join(parts)


class CopyIntoBuilder:
    """Builder for CopyInto."""

    def __init__(self):
        self.target: Optional[CopyIntoTarget] = None
        self.source: Optional[CopyIntoSource] = None
        self.options: CopyIntoOptions = CopyIntoOptions()

    def with_target(self, target: CopyIntoTarget) -> 'CopyIntoBuilder':
        """Sets the target for the COPY INTO statement."""
        self.target = target
        return self

    def with_source(self, source: CopyIntoSource) -> 'CopyIntoBuilder':
        """Sets the source for the COPY INTO statement."""
        self.source = source
        return self

    def with_options(self, options: CopyIntoOptions) -> 'CopyIntoBuilder':
        """Sets the options for the COPY INTO statement."""
        self.options = options
        return self

    def build(self) -> CopyInto:
        """Builds and returns a new CopyInto instance."""
        if self.target is None:
            raise ValueError("target must be set")
        if self.source is None:
            raise ValueError("source must be set")

        return CopyInto(target=self.target, source=self.source, options=self.options)
