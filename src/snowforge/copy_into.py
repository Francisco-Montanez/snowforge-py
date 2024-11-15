from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Literal, Optional

from snowforge.file_format import FileFormatSpecification


class OnError(Enum):
    CONTINUE = "CONTINUE"
    SKIP_FILE = "SKIP_FILE"
    ABORT_STATEMENT = "ABORT_STATEMENT"

    @classmethod
    def skip_file_num(cls, num: int) -> str:
        """Creates a SKIP_FILE_{num} option with the specified number."""
        return f"SKIP_FILE_{num}"

    @classmethod
    def skip_file_num_percent(cls, num: int) -> str:
        """Creates a SKIP_FILE_{num}% option with the specified percentage."""
        return f"SKIP_FILE_{num}%"

    def __str__(self) -> str:
        return self.value


class MatchByColumnName(Enum):
    CASE_SENSITIVE = "CASE_SENSITIVE"
    CASE_INSENSITIVE = "CASE_INSENSITIVE"
    NONE = "NONE"


class ValidationMode(Enum):
    RETURN_ERRORS = "RETURN_ERRORS"
    RETURN_ALL_ERRORS = "RETURN_ALL_ERRORS"

    @classmethod
    def return_n_rows(cls, n: int) -> str:
        """Creates a RETURN_{n}_ROWS option with the specified number of rows."""
        return f"RETURN_{n}_ROWS"

    def __str__(self) -> str:
        return self.value


class CopyIntoTarget:
    """A class representing the target destination for a Snowflake COPY INTO statement.

    This class encapsulates the target location (table or stage) where data will be copied to.

    Attributes:
        name (str): The name of the target object (table or stage)
        target_type (Literal["table", "stage"]): The type of target
    """

    def __init__(self, name: str, target_type: Literal["table", "stage"]):
        self.name = name
        self.target_type = target_type

    @classmethod
    def table(cls, name: str) -> 'CopyIntoTarget':
        """Creates a table target."""
        return cls(name, "table")

    @classmethod
    def stage(cls, name: str) -> CopyIntoTarget:
        """Creates a stage target."""
        return cls(name, "stage")

    def to_sql(self) -> str:
        return self.name


class CopyIntoSource:
    """A class representing the source location for a Snowflake COPY INTO statement.

    This class encapsulates the source location (table or stage) from where data will be copied.

    Attributes:
        name (str): The name of the source object (table or stage)
        source_type (Literal["table", "stage"]): The type of source
    """

    def __init__(self, name: str, source_type: Literal["table", "stage"]):
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
        if self.source_type == "stage":
            return f"@{self.name}"
        return self.name


@dataclass
class CopyIntoOptions:
    """Configuration options for a Snowflake COPY INTO command.

    This class provides a comprehensive set of options that can be used to customize
    the behavior of a COPY INTO operation.

    Attributes:
        enforce_length (bool): Whether to enforce length constraints
        file_format (Optional[str]): Snowflake file format specification
        files (Optional[List[str]]): Specific files to load
        force (bool): Whether to force the operation
        match_by_column_name (MatchByColumnName): Column matching strategy
        on_error (Optional[OnError]): Error handling behavior
        pattern (Optional[str]): File pattern to match source files
        pattern_type (Optional[str]): Type of pattern matching to use
        purge (bool): Whether to purge source files after loading
        return_failed_only (bool): Whether to return only failed records
        size_limit (Optional[int]): Maximum size of data to load
        truncate_columns (bool): Whether to truncate columns that exceed target length
        validation_mode (Optional[str]): Mode for validating data
    """

    enforce_length: bool = False
    file_processor: Optional[str] = None
    force: bool = False
    include_metadata: Optional[Dict[str, str]] = None
    load_uncertain_files: bool = False
    match_by_column_name: Optional[MatchByColumnName] = None
    on_error: Optional[OnError] = None
    purge: bool = False
    return_failed_only: bool = False
    size_limit: Optional[int] = None
    truncate_columns: bool = False

    @classmethod
    def builder(cls) -> 'CopyIntoOptionsBuilder':
        """Creates a new CopyIntoOptionsBuilder instance."""
        return CopyIntoOptionsBuilder()

    def to_sql(self) -> str:
        parts = []

        if self.return_failed_only:
            parts.append("RETURN_FAILED_ONLY = TRUE")
        if self.on_error:
            parts.append(f"ON_ERROR = {self.on_error.value}")
        if self.size_limit:
            parts.append(f"SIZE_LIMIT = {self.size_limit}")
        if self.purge:
            parts.append("PURGE = TRUE")
        if self.match_by_column_name:
            parts.append(f"MATCH_BY_COLUMN_NAME = {self.match_by_column_name.value}")
        if self.enforce_length:
            parts.append("ENFORCE_LENGTH = TRUE")
        if self.truncate_columns:
            parts.append("TRUNCATECOLUMNS = TRUE")
        if self.force:
            parts.append("FORCE = TRUE")
        if self.load_uncertain_files:
            parts.append("LOAD_UNCERTAIN_FILES = TRUE")
        if self.file_processor:
            parts.append(f"FILE_PROCESSOR = ({self.file_processor})")
        if self.include_metadata:
            parts.append(f"INCLUDE_METADATA = ({self.include_metadata})")

        return " ".join(parts)


class CopyIntoOptionsBuilder:
    """Builder class for constructing CopyIntoOptions with a fluent interface.

    This builder provides a method chaining approach to configure all available
    options for a COPY INTO statement.

    Example:
        >>> options = CopyIntoOptionsBuilder()\\
        ...     .with_pattern('*.csv')\\
        ...     .with_file_format('my_csv_format')\\
        ...     .with_on_error('CONTINUE')\\
        ...     .build()
    """

    def __init__(self):
        self.return_failed_only: bool = False
        self.on_error: Optional[OnError] = None
        self.size_limit: Optional[int] = None
        self.purge: bool = False
        self.match_by_column_name: Optional[MatchByColumnName] = None
        self.enforce_length: bool = False
        self.truncate_columns: bool = False
        self.force: bool = False
        self.load_uncertain_files: bool = False
        self.file_processor: Optional[str] = None
        self.include_metadata: Optional[Dict[str, str]] = None

    def with_return_failed_only(
        self, return_failed_only: bool
    ) -> 'CopyIntoOptionsBuilder':
        """Sets whether to return only failed records."""
        self.return_failed_only = return_failed_only
        return self

    def with_on_error(self, on_error: OnError) -> 'CopyIntoOptionsBuilder':
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
        self, match_by_column_name: MatchByColumnName
    ) -> 'CopyIntoOptionsBuilder':
        """Sets whether to match columns by name."""
        self.match_by_column_name = match_by_column_name
        return self

    def with_enforce_length(self, enforce_length: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to enforce length constraints."""
        self.enforce_length = enforce_length
        return self

    def with_truncate_columns(self, truncate_columns: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to truncate columns."""
        self.truncate_columns = truncate_columns
        return self

    def with_force(self, force: bool) -> 'CopyIntoOptionsBuilder':
        """Sets whether to force the operation."""
        self.force = force
        return self

    def with_load_uncertain_files(
        self, load_uncertain_files: bool
    ) -> 'CopyIntoOptionsBuilder':
        """Sets whether to load uncertain files."""
        self.load_uncertain_files = load_uncertain_files
        return self

    def with_file_processor(self, file_processor: str) -> 'CopyIntoOptionsBuilder':
        """Sets the file processor."""
        self.file_processor = file_processor
        return self

    def with_include_metadata(
        self, include_metadata: Dict[str, str]
    ) -> 'CopyIntoOptionsBuilder':
        """Sets the include metadata."""
        self.include_metadata = include_metadata
        return self

    def build(self) -> CopyIntoOptions:
        """Builds and returns a new CopyIntoOptions instance."""
        return CopyIntoOptions(
            return_failed_only=self.return_failed_only,
            on_error=self.on_error,
            size_limit=self.size_limit,
            purge=self.purge,
            match_by_column_name=self.match_by_column_name,
            enforce_length=self.enforce_length,
            truncate_columns=self.truncate_columns,
            force=self.force,
        )


@dataclass
class CopyInto:
    """Main class representing a Snowflake COPY INTO statement.

    This class combines target, source, and options to generate a complete
    COPY INTO SQL statement.

    Attributes:
        target (CopyIntoTarget): The destination for the copy operation
        source (CopyIntoSource): The source of the data
        options (CopyIntoOptions): Configuration options for the operation

    Example:
        >>> copy_into = CopyInto.builder()\\
        ...     .with_target(CopyIntoTarget.table("my_table"))\\
        ...     .with_source(CopyIntoSource.stage("my_stage"))\\
        ...     .with_options(CopyIntoOptions.builder()
        ...         .with_pattern('*.csv')
        ...         .build())\\
        ...     .build()
        >>> sql = copy_into.to_sql()
    """

    target: CopyIntoTarget
    source: CopyIntoSource
    options: CopyIntoOptions = CopyIntoOptions()
    pattern: Optional[str] = None
    file_format: Optional[FileFormatSpecification] = None
    files: Optional[List[str]] = None
    validation_mode: Optional[ValidationMode] = None

    @classmethod
    def builder(cls) -> 'CopyIntoBuilder':
        """Creates a new CopyIntoBuilder instance."""
        return CopyIntoBuilder()

    def to_sql(self) -> str:
        """Generates the SQL statement for the COPY INTO command."""
        parts = ["COPY INTO", self.target.to_sql(), "FROM", self.source.to_sql()]

        if self.pattern:
            parts.append(f"PATTERN = '{self.pattern}'")
        if self.file_format:
            parts.append(f"FILE_FORMAT = ({self.file_format.to_sql()})")
        if self.files:
            files_str = ", ".join(f"'{f}'" for f in self.files)
            parts.append(f"FILES = ({files_str})")
        if self.validation_mode:
            parts.append(f"VALIDATION_MODE = {self.validation_mode.value}")

        options_sql = self.options.to_sql()
        if options_sql:
            parts.append(options_sql)

        return " ".join(parts)


class CopyIntoBuilder:
    """Builder for CopyInto."""

    def __init__(self):
        self.target: Optional[CopyIntoTarget] = None
        self.source: Optional[CopyIntoSource] = None
        self.pattern: Optional[str] = None
        self.files: Optional[List[str]] = None
        self.file_format: Optional[FileFormatSpecification] = None
        self.options: CopyIntoOptions = CopyIntoOptions()
        self.validation_mode: Optional[ValidationMode] = None

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

    def with_pattern(self, pattern: str) -> 'CopyIntoBuilder':
        """Sets the file pattern (e.g., '*.csv', 'data_*.parquet') for matching source files."""
        self.pattern = pattern
        return self

    def with_file_format(
        self, file_format: FileFormatSpecification
    ) -> 'CopyIntoBuilder':
        """Sets the file format specification."""
        self.file_format = file_format
        return self

    def with_files(self, files: List[str]) -> 'CopyIntoBuilder':
        """Sets the specific files to load."""
        self.files = files
        return self

    def with_validation_mode(
        self, validation_mode: ValidationMode
    ) -> 'CopyIntoBuilder':
        """Sets the validation mode."""
        self.validation_mode = validation_mode
        return self

    def build(self) -> CopyInto:
        """Builds and returns a new CopyInto instance."""
        if self.target is None:
            raise ValueError("target must be set")
        if self.source is None:
            raise ValueError("source must be set")

        return CopyInto(
            target=self.target,
            source=self.source,
            options=self.options,
            pattern=self.pattern,
            files=self.files,
            file_format=self.file_format,
            validation_mode=self.validation_mode,
        )
