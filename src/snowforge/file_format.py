from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Literal, Optional, Union

from snowforge.utilities import (
    sql_escape_string,
    sql_format_boolean,
    sql_format_list,
    sql_quote_comment,
    sql_quote_string,
)


class BinaryFormat(Enum):
    """Supported binary format types for Snowflake file formats.

    These formats determine how binary data is interpreted and represented in files.

    Attributes:
        BASE64: Base64 encoding of binary data
        HEX: Hexadecimal representation of binary data
        UTF8: UTF-8 encoding of binary data
    """

    BASE64 = "BASE64"
    HEX = "HEX"
    UTF8 = "UTF8"

    def __str__(self) -> str:
        """Returns the string representation of the enum value."""
        return self.value


class CompressionType(str, Enum):
    """Compression algorithms supported by Snowflake file formats.

    Attributes:
        AUTO: Automatically detect compression type based on file extension
        BROTLI: Brotli compression (.br files)
        BZ2: BZIP2 compression (.bz2 files)
        DEFLATE: DEFLATE compression
        GZIP: GZIP compression (.gz files)
        LZO: LZO compression (Parquet files only)
        NONE: No compression
        RAW_DEFLATE: Raw DEFLATE compression without headers
        SNAPPY: Snappy compression (Parquet files only)
        ZSTD: Zstandard compression (.zst files)
    """

    AUTO = "AUTO"
    BROTLI = "BROTLI"
    BZ2 = "BZ2"
    DEFLATE = "DEFLATE"
    GZIP = "GZIP"
    LZO = "LZO"
    NONE = "NONE"
    RAW_DEFLATE = "RAWDEFLATE"
    SNAPPY = "SNAPPY"
    ZSTD = "ZSTD"

    def __str__(self) -> str:
        """Returns the string representation of the enum value."""
        return self.value


StandardCompressionType = Literal[
    CompressionType.AUTO,
    CompressionType.BROTLI,
    CompressionType.BZ2,
    CompressionType.DEFLATE,
    CompressionType.GZIP,
    CompressionType.NONE,
    CompressionType.RAW_DEFLATE,
    CompressionType.ZSTD,
]

ParquetCompressionType = Literal[
    CompressionType.AUTO,
    CompressionType.LZO,
    CompressionType.NONE,
    CompressionType.SNAPPY,
]


class FileFormatOptions(ABC):
    """Abstract base class for file format options.

    This class serves as the base for all file format-specific option classes,
    ensuring they implement the required SQL generation method.
    """

    @abstractmethod
    def to_sql(self) -> str:
        """Converts the options to a SQL string."""
        pass


@dataclass
class FileFormat:
    """Represents a Snowflake file format definition.

    This class encapsulates a file format name and its associated options,
    providing methods to generate the corresponding SQL statements.

    Attributes:
        name (str): Name of the file format
        comment (Optional[str]): Comment for the format
        create_if_not_exists (bool): Whether to create if not exists
        create_or_replace (bool): Whether to create or replace the format
        options (Optional[FileFormatOptions]): Format-specific options
        temporary (bool): Whether the format is temporary
        volatile (bool): Whether the format is volatile

    Example:
        >>> format = FileFormat.builder("my_csv_format")\\
        ...     .with_options(CsvOptions.builder()
        ...         .with_compression(CompressionType.GZIP)
        ...         .build())\\
        ...     .build()
    """

    name: str
    comment: Optional[str] = None
    create_if_not_exists: bool = False
    create_or_replace: bool = False
    options: Optional[FileFormatOptions] = None
    temporary: bool = False
    volatile: bool = False

    @classmethod
    def builder(cls, name: str) -> FileFormatBuilder:
        """Creates a builder for constructing FileFormat instances."""
        return FileFormatBuilder(name)

    def to_sql(self) -> str:
        """Converts the FileFormat instance to a SQL string."""
        parts = []

        if self.create_or_replace:
            parts.append("CREATE OR REPLACE")
        else:
            parts.append("CREATE")

        if self.temporary:
            parts.append("TEMPORARY")
        elif self.volatile:
            parts.append("VOLATILE")

        parts.append("FILE FORMAT")

        if self.create_if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.name)

        if self.options:
            parts.append(self.options.to_sql())

        if self.comment:
            parts.append(f"COMMENT = {sql_quote_comment(self.comment)}")

        return " ".join(parts)


class FileFormatBuilder:
    """Builder class for constructing FileFormat instances.

    This builder provides a fluent interface for configuring file formats
    with various options and properties.

    Example:
        >>> builder = FileFormatBuilder("my_format")\\
        ...     .with_create_or_replace()\\
        ...     .with_temporary()\\
        ...     .with_options(csv_options)
    """

    def __init__(self, name: str):
        """Initialize the builder with a format name."""
        self._comment: Optional[str] = None
        self._create_if_not_exists = False
        self._create_or_replace = False
        self._name = name
        self._options: Optional[FileFormatOptions] = None
        self._temporary = False
        self._volatile = False

    def build(self) -> FileFormat:
        """Builds the FileFormat instance."""
        return FileFormat(
            comment=self._comment,
            create_if_not_exists=self._create_if_not_exists,
            create_or_replace=self._create_or_replace,
            name=self._name,
            options=self._options,
            temporary=self._temporary,
            volatile=self._volatile,
        )

    def with_comment(self, comment: str) -> FileFormatBuilder:
        """Adds a comment to the format definition."""
        self._comment = comment
        return self

    def with_create_or_replace(self) -> FileFormatBuilder:
        """Adds CREATE OR REPLACE clause to the format definition."""
        self._create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> FileFormatBuilder:
        """Adds CREATE IF NOT EXISTS clause to the format definition."""
        self._create_if_not_exists = True
        return self

    def with_options(self, options: FileFormatOptions) -> FileFormatBuilder:
        """Adds format-specific options to the format definition."""
        self._options = options
        return self

    def with_temporary(self) -> FileFormatBuilder:
        """Adds TEMPORARY clause to the format definition."""
        self._temporary = True
        return self

    def with_volatile(self) -> FileFormatBuilder:
        """Adds VOLATILE clause to the format definition."""
        self._volatile = True
        return self


class FileFormatSpecification:
    """Represents a file format specification that can be either named or inline.

    This class handles both references to existing named formats and inline format
    definitions (create on the fly) for use in operations like COPY INTO.

    Attributes:
        type (str): Either "named" or "inline" indicating the specification type
        value (Union[str, FileFormat]): The format name or FileFormat instance
    """

    def __init__(self, spec: Union[str, FileFormat]):
        """Initialize a file format specification."""
        if isinstance(spec, str):
            self.type = "named"
            self.value = spec
        else:
            self.type = "inline"
            self.value = spec

    @classmethod
    def inline(cls, file_format: FileFormat) -> 'FileFormatSpecification':
        """Creates a specification with an inline file format definition."""
        return cls(file_format)

    @classmethod
    def named(cls, name: str) -> 'FileFormatSpecification':
        """Creates a specification referencing an existing file format."""
        return cls(name)

    def to_sql(self) -> str:
        """Converts the FileFormatSpecification instance to a SQL string."""
        if self.type == "named":
            return f"FORMAT_NAME = {sql_quote_string(str(self.value))}"
        if isinstance(self.value, FileFormat):
            return self.value.options.to_sql() if self.value.options else ""
        return ""


@dataclass
class AvroOptions(FileFormatOptions):
    """Options for configuring Avro file formats in Snowflake.

    Attributes:
        compression (Optional[StandardCompressionType]): Compression algorithm
        null_if (Optional[List[str]]): Strings to interpret as NULL values
        replace_invalid_characters (Optional[bool]): Replace invalid UTF-8 characters
        trim_space (Optional[bool]): Whether to trim whitespace from string fields

    Example:
        >>> options = AvroOptions.builder()\\
        ...     .with_compression(CompressionType.GZIP)\\
        ...     .with_trim_space(True)\\
        ...     .build()
    """

    compression: Optional[StandardCompressionType] = None
    null_if: Optional[List[str]] = None
    replace_invalid_characters: Optional[bool] = None
    trim_space: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'AvroOptionsBuilder':
        """Creates a builder for constructing AvroOptions instances."""
        return AvroOptionsBuilder()

    def to_sql(self) -> str:
        """Converts the AvroOptions instance to a SQL string."""
        parts = ["TYPE = AVRO"]

        if self.compression:
            parts.append(f"COMPRESSION = {str(self.compression)}")
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {sql_format_boolean(self.trim_space)}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {sql_format_boolean(self.replace_invalid_characters)}"
            )
        if self.null_if:
            parts.append(f"NULL_IF = {sql_format_list(self.null_if)}")

        return " ".join(parts)


class AvroOptionsBuilder:
    """Builder for constructing AvroOptions instances.

    Provides a fluent interface for setting Avro format options.

    Example:
        >>> builder = AvroOptionsBuilder()\\
        ...     .with_compression(CompressionType.GZIP)\\
        ...     .with_trim_space(True)
    """

    def __init__(self):
        """Initializes an AvroOptionsBuilder instance."""
        self.compression: Optional[StandardCompressionType] = None
        self.null_if: Optional[List[str]] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.trim_space: Optional[bool] = None

    def build(self) -> AvroOptions:
        """Builds the AvroOptions instance."""
        return AvroOptions(
            compression=self.compression,
            null_if=self.null_if,
            replace_invalid_characters=self.replace_invalid_characters,
            trim_space=self.trim_space,
        )

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'AvroOptionsBuilder':
        """Sets the compression algorithm."""
        self.compression = compression
        return self

    def with_null_if(self, null_if: List[str]) -> 'AvroOptionsBuilder':
        """Sets the strings to interpret as NULL values."""
        self.null_if = null_if
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'AvroOptionsBuilder':
        """Sets whether to replace invalid UTF-8 characters."""
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_trim_space(self, trim_space: bool) -> 'AvroOptionsBuilder':
        """Sets whether to trim whitespace from string fields."""
        self.trim_space = trim_space
        return self


@dataclass
class ParquetOptions(FileFormatOptions):
    """Options for configuring Parquet file formats in Snowflake.

    Attributes:
        binary_as_text (Optional[bool]): Treat binary data as text
        compression (Optional[ParquetCompressionType]): Parquet-specific compression
        null_if (Optional[List[str]]): Strings to interpret as NULL values
        replace_invalid_characters (Optional[bool]): Replace invalid UTF-8 characters
        trim_space (Optional[bool]): Trim whitespace from string fields
        use_logical_type (Optional[bool]): Use Parquet logical type definitions
        use_vectorized_scanner (Optional[bool]): Use vectorized Parquet scanning

    Example:
        >>> options = ParquetOptions.builder()\\
        ...     .with_compression(CompressionType.SNAPPY)\\
        ...     .with_use_logical_type(True)\\
        ...     .build()
    """

    binary_as_text: Optional[bool] = None
    compression: Optional[ParquetCompressionType] = None
    null_if: Optional[List[str]] = None
    replace_invalid_characters: Optional[bool] = None
    trim_space: Optional[bool] = None
    use_logical_type: Optional[bool] = None
    use_vectorized_scanner: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'ParquetOptionsBuilder':
        """Creates a builder for constructing ParquetOptions instances."""
        return ParquetOptionsBuilder()

    def to_sql(self) -> str:
        """Converts the ParquetOptions instance to a SQL string."""
        parts = ["TYPE = PARQUET"]

        if self.compression:
            parts.append(f"COMPRESSION = {str(self.compression)}")
        if self.binary_as_text is not None:
            parts.append(f"BINARY_AS_TEXT = {sql_format_boolean(self.binary_as_text)}")
        if self.use_logical_type is not None:
            parts.append(
                f"USE_LOGICAL_TYPE = {sql_format_boolean(self.use_logical_type)}"
            )
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {sql_format_boolean(self.trim_space)}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {sql_format_boolean(self.replace_invalid_characters)}"
            )
        if self.null_if:
            parts.append(f"NULL_IF = {sql_format_list(self.null_if)}")
        if self.use_vectorized_scanner is not None:
            parts.append(
                f"USE_VECTORIZED_SCANNER = {sql_format_boolean(self.use_vectorized_scanner)}"
            )

        return " ".join(parts)


class ParquetOptionsBuilder:
    def __init__(self):
        """Initializes a ParquetOptionsBuilder instance."""
        self.binary_as_text: Optional[bool] = None
        self.compression: Optional[ParquetCompressionType] = None
        self.null_if: Optional[List[str]] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.trim_space: Optional[bool] = None
        self.use_logical_type: Optional[bool] = None
        self.use_vectorized_scanner: Optional[bool] = None

    def build(self) -> ParquetOptions:
        """Builds the ParquetOptions instance."""
        return ParquetOptions(
            binary_as_text=self.binary_as_text,
            compression=self.compression,
            null_if=self.null_if,
            replace_invalid_characters=self.replace_invalid_characters,
            trim_space=self.trim_space,
            use_logical_type=self.use_logical_type,
            use_vectorized_scanner=self.use_vectorized_scanner,
        )

    def with_binary_as_text(self, binary_as_text: bool) -> 'ParquetOptionsBuilder':
        """Sets whether to treat binary data as text."""
        self.binary_as_text = binary_as_text
        return self

    def with_compression(
        self, compression: ParquetCompressionType
    ) -> 'ParquetOptionsBuilder':
        """Sets the compression algorithm."""
        self.compression = compression
        return self

    def with_null_if(self, null_if: List[str]) -> 'ParquetOptionsBuilder':
        """Sets the strings to interpret as NULL values."""
        self.null_if = null_if
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'ParquetOptionsBuilder':
        """Sets whether to replace invalid UTF-8 characters."""
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_trim_space(self, trim_space: bool) -> 'ParquetOptionsBuilder':
        """Sets whether to trim whitespace from string fields."""
        self.trim_space = trim_space
        return self

    def with_use_logical_type(self, use_logical_type: bool) -> 'ParquetOptionsBuilder':
        """Sets whether to use Parquet logical type definitions."""
        self.use_logical_type = use_logical_type
        return self

    def with_use_vectorized_scanner(
        self, use_vectorized_scanner: bool
    ) -> 'ParquetOptionsBuilder':
        """Sets whether to use vectorized Parquet scanning."""
        self.use_vectorized_scanner = use_vectorized_scanner
        return self


@dataclass
class JsonOptions(FileFormatOptions):
    """Options for configuring JSON file formats in Snowflake.

    Attributes:
        allow_duplicate (Optional[bool]): Allow duplicate object keys
        binary_format (Optional[BinaryFormat]): How to interpret binary data
        compression (Optional[StandardCompressionType]): Compression algorithm
        date_format (Optional[str]): Format string for date values
        enable_octal (Optional[bool]): Enable octal number parsing
        file_extension (Optional[str]): Expected file extension
        ignore_utf8_errors (Optional[bool]): Ignore UTF-8 encoding errors
        null_if (Optional[List[str]]): Strings to interpret as NULL values
        replace_invalid_characters (Optional[bool]): Replace invalid UTF-8 characters
        skip_byte_order_mark (Optional[bool]): Skip UTF-8 BOM if present
        strip_null_values (Optional[bool]): Remove null value entries
        strip_outer_array (Optional[bool]): Remove outer array from JSON
        time_format (Optional[str]): Format string for time values
        timestamp_format (Optional[str]): Format string for timestamp values
        trim_space (Optional[bool]): Trim whitespace from string fields

    Example:
        >>> options = JsonOptions.builder()\\
        ...     .with_compression(CompressionType.GZIP)\\
        ...     .with_strip_outer_array(True)\\
        ...     .build()
    """

    allow_duplicate: Optional[bool] = None
    binary_format: Optional[BinaryFormat] = None
    compression: Optional[StandardCompressionType] = None
    date_format: Optional[str] = None
    enable_octal: Optional[bool] = None
    file_extension: Optional[str] = None
    ignore_utf8_errors: Optional[bool] = None
    null_if: Optional[List[str]] = None
    replace_invalid_characters: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None
    strip_null_values: Optional[bool] = None
    strip_outer_array: Optional[bool] = None
    time_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    trim_space: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'JsonOptionsBuilder':
        """Creates a builder for constructing JsonOptions instances."""
        return JsonOptionsBuilder()

    def to_sql(self) -> str:
        """Converts the JsonOptions instance to a SQL string."""
        parts = ["TYPE = JSON"]

        if self.compression:
            parts.append(f"COMPRESSION = {str(self.compression)}")
        if self.date_format:
            parts.append(f"DATE_FORMAT = {sql_quote_string(self.date_format)}")
        if self.time_format:
            parts.append(f"TIME_FORMAT = {sql_quote_string(self.time_format)}")
        if self.timestamp_format:
            parts.append(
                f"TIMESTAMP_FORMAT = {sql_quote_string(self.timestamp_format)}"
            )
        if self.binary_format:
            parts.append(f"BINARY_FORMAT = {str(self.binary_format)}")
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {sql_format_boolean(self.trim_space)}")
        if self.enable_octal is not None:
            parts.append(f"ENABLE_OCTAL = {sql_format_boolean(self.enable_octal)}")
        if self.allow_duplicate is not None:
            parts.append(
                f"ALLOW_DUPLICATE = {sql_format_boolean(self.allow_duplicate)}"
            )
        if self.strip_outer_array is not None:
            parts.append(
                f"STRIP_OUTER_ARRAY = {sql_format_boolean(self.strip_outer_array)}"
            )
        if self.strip_null_values is not None:
            parts.append(
                f"STRIP_NULL_VALUES = {sql_format_boolean(self.strip_null_values)}"
            )
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {sql_format_boolean(self.replace_invalid_characters)}"
            )
        if self.ignore_utf8_errors is not None:
            parts.append(
                f"IGNORE_UTF8_ERRORS = {sql_format_boolean(self.ignore_utf8_errors)}"
            )
        if self.skip_byte_order_mark is not None:
            parts.append(
                f"SKIP_BYTE_ORDER_MARK = {sql_format_boolean(self.skip_byte_order_mark)}"
            )
        if self.file_extension:
            parts.append(f"FILE_EXTENSION = {sql_quote_string(self.file_extension)}")
        if self.null_if:
            parts.append(f"NULL_IF = {sql_format_list(self.null_if)}")

        return " ".join(parts)


class JsonOptionsBuilder:
    def __init__(self):
        """Initializes a JsonOptionsBuilder instance."""
        self.allow_duplicate: Optional[bool] = None
        self.binary_format: Optional[BinaryFormat] = None
        self.compression: Optional[StandardCompressionType] = None
        self.date_format: Optional[str] = None
        self.enable_octal: Optional[bool] = None
        self.file_extension: Optional[str] = None
        self.ignore_utf8_errors: Optional[bool] = None
        self.null_if: Optional[List[str]] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.skip_byte_order_mark: Optional[bool] = None
        self.strip_null_values: Optional[bool] = None
        self.strip_outer_array: Optional[bool] = None
        self.time_format: Optional[str] = None
        self.timestamp_format: Optional[str] = None
        self.trim_space: Optional[bool] = None

    def build(self) -> JsonOptions:
        """Builds the JsonOptions instance."""
        return JsonOptions(
            allow_duplicate=self.allow_duplicate,
            binary_format=self.binary_format,
            compression=self.compression,
            date_format=self.date_format,
            enable_octal=self.enable_octal,
            file_extension=self.file_extension,
            ignore_utf8_errors=self.ignore_utf8_errors,
            null_if=self.null_if,
            replace_invalid_characters=self.replace_invalid_characters,
            skip_byte_order_mark=self.skip_byte_order_mark,
            strip_null_values=self.strip_null_values,
            strip_outer_array=self.strip_outer_array,
            time_format=self.time_format,
            timestamp_format=self.timestamp_format,
            trim_space=self.trim_space,
        )

    def with_allow_duplicate(self, allow_duplicate: bool) -> 'JsonOptionsBuilder':
        """Sets whether to allow duplicate object keys."""
        self.allow_duplicate = allow_duplicate
        return self

    def with_binary_format(self, binary_format: BinaryFormat) -> 'JsonOptionsBuilder':
        """Sets how to interpret binary data."""
        self.binary_format = binary_format
        return self

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'JsonOptionsBuilder':
        """Sets the compression algorithm."""
        self.compression = compression
        return self

    def with_date_format(self, date_format: str) -> 'JsonOptionsBuilder':
        """Sets the format string for date values."""
        self.date_format = date_format
        return self

    def with_enable_octal(self, enable_octal: bool) -> 'JsonOptionsBuilder':
        """Sets whether to enable octal number parsing."""
        self.enable_octal = enable_octal
        return self

    def with_file_extension(self, file_extension: str) -> 'JsonOptionsBuilder':
        """Sets the expected file extension."""
        self.file_extension = file_extension
        return self

    def with_ignore_utf8_errors(self, ignore_utf8_errors: bool) -> 'JsonOptionsBuilder':
        """Sets whether to ignore UTF-8 encoding errors."""
        self.ignore_utf8_errors = ignore_utf8_errors
        return self

    def with_null_if(self, null_if: List[str]) -> 'JsonOptionsBuilder':
        """Sets the strings to interpret as NULL values."""
        self.null_if = null_if
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'JsonOptionsBuilder':
        """Sets whether to replace invalid UTF-8 characters."""
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_skip_byte_order_mark(
        self, skip_byte_order_mark: bool
    ) -> 'JsonOptionsBuilder':
        """Sets whether to skip UTF-8 BOM if present."""
        self.skip_byte_order_mark = skip_byte_order_mark
        return self

    def with_strip_null_values(self, strip_null_values: bool) -> 'JsonOptionsBuilder':
        """Sets whether to remove null value entries."""
        self.strip_null_values = strip_null_values
        return self

    def with_strip_outer_array(self, strip_outer_array: bool) -> 'JsonOptionsBuilder':
        """Sets whether to remove outer array from JSON."""
        self.strip_outer_array = strip_outer_array
        return self

    def with_time_format(self, time_format: str) -> 'JsonOptionsBuilder':
        """Sets the format string for time values."""
        self.time_format = time_format
        return self

    def with_timestamp_format(self, timestamp_format: str) -> 'JsonOptionsBuilder':
        """Sets the format string for timestamp values."""
        self.timestamp_format = timestamp_format
        return self

    def with_trim_space(self, trim_space: bool) -> 'JsonOptionsBuilder':
        """Sets whether to trim whitespace from string fields."""
        self.trim_space = trim_space
        return self


@dataclass
class CsvOptions(FileFormatOptions):
    """Options for configuring CSV file formats in Snowflake.

    Attributes:
        binary_format (Optional[BinaryFormat]): How to interpret binary data
        compression (Optional[StandardCompressionType]): Compression algorithm
        date_format (Optional[str]): Format string for date values
        empty_field_as_null (Optional[bool]): Treat empty fields as NULL
        encoding (Optional[str]): Character encoding of the file
        error_on_column_count_mismatch (Optional[bool]): Error on mismatched columns
        escape (Optional[str]): Escape character for field delimiters
        escape_unenclosed_field (Optional[str]): Escape for unenclosed fields
        field_delimiter (Optional[str]): Character(s) separating fields
        field_optionally_enclosed_by (Optional[str]): Optional field enclosure char
        file_extension (Optional[str]): Expected file extension
        null_if (Optional[List[str]]): Strings to interpret as NULL values
        parse_header (Optional[bool]): Whether to parse header row
        record_delimiter (Optional[str]): Character(s) separating records
        replace_invalid_characters (Optional[bool]): Replace invalid UTF-8 characters
        skip_blank_lines (Optional[bool]): Whether to skip empty lines
        skip_byte_order_mark (Optional[bool]): Skip UTF-8 BOM if present
        skip_header (Optional[int]): Number of header rows to skip
        time_format (Optional[str]): Format string for time values
        timestamp_format (Optional[str]): Format string for timestamp values
        trim_space (Optional[bool]): Trim whitespace from fields

    Example:
        >>> options = CsvOptions.builder()\\
        ...     .with_field_delimiter(",")\\
        ...     .with_parse_header(True)\\
        ...     .with_skip_blank_lines(True)\\
        ...     .build()
    """

    binary_format: Optional[BinaryFormat] = None
    compression: Optional[StandardCompressionType] = None
    date_format: Optional[str] = None
    empty_field_as_null: Optional[bool] = None
    encoding: Optional[str] = None
    error_on_column_count_mismatch: Optional[bool] = None
    escape_unenclosed_field: Optional[str] = None
    escape: Optional[str] = None
    field_delimiter: Optional[str] = None
    field_optionally_enclosed_by: Optional[str] = None
    file_extension: Optional[str] = None
    null_if: Optional[List[str]] = None
    parse_header: Optional[bool] = None
    record_delimiter: Optional[str] = None
    replace_invalid_characters: Optional[bool] = None
    skip_blank_lines: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None
    skip_header: Optional[int] = None
    time_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    trim_space: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'CsvOptionsBuilder':
        """Creates a builder for constructing CsvOptions instances."""
        return CsvOptionsBuilder()

    def to_sql(self) -> str:
        """Converts the CsvOptions instance to a SQL string."""
        parts = ["TYPE = CSV"]

        if self.compression:
            parts.append(f"COMPRESSION = {str(self.compression)}")
        if self.record_delimiter:
            parts.append(
                f"RECORD_DELIMITER = {sql_quote_string(self.record_delimiter)}"
            )
        if self.field_delimiter:
            parts.append(f"FIELD_DELIMITER = {sql_quote_string(self.field_delimiter)}")
        if self.file_extension:
            parts.append(f"FILE_EXTENSION = {sql_quote_string(self.file_extension)}")
        if self.parse_header is not None:
            parts.append(f"PARSE_HEADER = {sql_format_boolean(self.parse_header)}")
        if self.skip_header is not None:
            parts.append(f"SKIP_HEADER = {self.skip_header}")
        if self.skip_blank_lines is not None:
            parts.append(
                f"SKIP_BLANK_LINES = {sql_format_boolean(self.skip_blank_lines)}"
            )
        if self.date_format:
            parts.append(f"DATE_FORMAT = {sql_quote_string(self.date_format)}")
        if self.time_format:
            parts.append(f"TIME_FORMAT = {sql_quote_string(self.time_format)}")
        if self.timestamp_format:
            parts.append(
                f"TIMESTAMP_FORMAT = {sql_quote_string(self.timestamp_format)}"
            )
        if self.binary_format:
            parts.append(f"BINARY_FORMAT = {str(self.binary_format)}")
        if self.escape:
            parts.append(f"ESCAPE = {sql_quote_string(self.escape)}")
        if self.escape_unenclosed_field:
            parts.append(
                f"ESCAPE_UNENCLOSED_FIELD = {sql_quote_string(sql_escape_string(self.escape_unenclosed_field))}"
            )
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {sql_format_boolean(self.trim_space)}")
        if self.field_optionally_enclosed_by:
            parts.append(
                f"FIELD_OPTIONALLY_ENCLOSED_BY = '{self.field_optionally_enclosed_by}'"
            )
        if self.null_if:
            parts.append(f"NULL_IF = {sql_format_list(self.null_if)}")
        if self.error_on_column_count_mismatch is not None:
            parts.append(
                f"ERROR_ON_COLUMN_COUNT_MISMATCH = {sql_format_boolean(self.error_on_column_count_mismatch)}"
            )
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {sql_format_boolean(self.replace_invalid_characters)}"
            )
        if self.empty_field_as_null is not None:
            parts.append(
                f"EMPTY_FIELD_AS_NULL = {sql_format_boolean(self.empty_field_as_null)}"
            )
        if self.skip_byte_order_mark is not None:
            parts.append(
                f"SKIP_BYTE_ORDER_MARK = {sql_format_boolean(self.skip_byte_order_mark)}"
            )
        if self.encoding:
            parts.append(f"ENCODING = {sql_quote_string(self.encoding)}")

        return " ".join(parts)


class CsvOptionsBuilder:
    def __init__(self):
        """Initializes a CsvOptionsBuilder instance."""
        self.binary_format: Optional[BinaryFormat] = None
        self.compression: Optional[StandardCompressionType] = None
        self.date_format: Optional[str] = None
        self.empty_field_as_null: Optional[bool] = None
        self.encoding: Optional[str] = None
        self.error_on_column_count_mismatch: Optional[bool] = None
        self.escape_unenclosed_field: Optional[str] = None
        self.escape: Optional[str] = None
        self.field_delimiter: Optional[str] = None
        self.field_optionally_enclosed_by: Optional[str] = None
        self.file_extension: Optional[str] = None
        self.null_if: Optional[List[str]] = None
        self.parse_header: Optional[bool] = None
        self.record_delimiter: Optional[str] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.skip_blank_lines: Optional[bool] = None
        self.skip_byte_order_mark: Optional[bool] = None
        self.skip_header: Optional[int] = None
        self.time_format: Optional[str] = None
        self.timestamp_format: Optional[str] = None
        self.trim_space: Optional[bool] = None

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'CsvOptionsBuilder':
        """Sets the compression algorithm."""
        self.compression = compression
        return self

    def with_record_delimiter(self, record_delimiter: str) -> 'CsvOptionsBuilder':
        """Sets the character(s) separating records."""
        self.record_delimiter = record_delimiter
        return self

    def with_field_delimiter(self, field_delimiter: str) -> 'CsvOptionsBuilder':
        """Sets the character(s) separating fields."""
        self.field_delimiter = field_delimiter
        return self

    def with_file_extension(self, file_extension: str) -> 'CsvOptionsBuilder':
        """Sets the expected file extension."""
        self.file_extension = file_extension
        return self

    def with_parse_header(self, parse_header: bool) -> 'CsvOptionsBuilder':
        """Sets whether to parse the header row."""
        self.parse_header = parse_header
        return self

    def with_skip_header(self, skip_header: int) -> 'CsvOptionsBuilder':
        """Sets the number of header rows to skip."""
        self.skip_header = skip_header
        return self

    def with_skip_blank_lines(self, skip_blank_lines: bool) -> 'CsvOptionsBuilder':
        """Sets whether to skip empty lines."""
        self.skip_blank_lines = skip_blank_lines
        return self

    def with_date_format(self, date_format: str) -> 'CsvOptionsBuilder':
        """Sets the format string for date values."""
        self.date_format = date_format
        return self

    def with_time_format(self, time_format: str) -> 'CsvOptionsBuilder':
        """Sets the format string for time values."""
        self.time_format = time_format
        return self

    def with_timestamp_format(self, timestamp_format: str) -> 'CsvOptionsBuilder':
        """Sets the format string for timestamp values."""
        self.timestamp_format = timestamp_format
        return self

    def with_binary_format(self, binary_format: BinaryFormat) -> 'CsvOptionsBuilder':
        """Sets how to interpret binary data."""
        self.binary_format = binary_format
        return self

    def with_escape(self, escape: str) -> 'CsvOptionsBuilder':
        """Sets the escape character for field delimiters."""
        self.escape = escape
        return self

    def with_escape_unenclosed_field(
        self, escape_unenclosed_field: str
    ) -> 'CsvOptionsBuilder':
        """Sets the escape character for unenclosed fields."""
        self.escape_unenclosed_field = escape_unenclosed_field
        return self

    def with_trim_space(self, trim_space: bool) -> 'CsvOptionsBuilder':
        """Sets whether to trim whitespace from fields."""
        self.trim_space = trim_space
        return self

    def with_field_optionally_enclosed_by(
        self, field_optionally_enclosed_by: str
    ) -> 'CsvOptionsBuilder':
        """Sets the optional field enclosure character."""
        self.field_optionally_enclosed_by = field_optionally_enclosed_by
        return self

    def with_null_if(self, null_if: List[str]) -> 'CsvOptionsBuilder':
        """Sets the strings to interpret as NULL values."""
        self.null_if = null_if
        return self

    def with_error_on_column_count_mismatch(
        self, error_on_column_count_mismatch: bool
    ) -> 'CsvOptionsBuilder':
        """Sets whether to error on mismatched columns."""
        self.error_on_column_count_mismatch = error_on_column_count_mismatch
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'CsvOptionsBuilder':
        """Sets whether to replace invalid UTF-8 characters."""
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_empty_field_as_null(
        self, empty_field_as_null: bool
    ) -> 'CsvOptionsBuilder':
        """Sets whether to treat empty fields as NULL."""
        self.empty_field_as_null = empty_field_as_null
        return self

    def with_skip_byte_order_mark(
        self, skip_byte_order_mark: bool
    ) -> 'CsvOptionsBuilder':
        """Sets whether to skip UTF-8 BOM if present."""
        self.skip_byte_order_mark = skip_byte_order_mark
        return self

    def with_encoding(self, encoding: str) -> 'CsvOptionsBuilder':
        """Sets the character encoding of the file."""
        self.encoding = encoding
        return self

    def build(self) -> CsvOptions:
        """Builds the CsvOptions instance."""
        return CsvOptions(
            binary_format=self.binary_format,
            compression=self.compression,
            date_format=self.date_format,
            empty_field_as_null=self.empty_field_as_null,
            encoding=self.encoding,
            error_on_column_count_mismatch=self.error_on_column_count_mismatch,
            escape_unenclosed_field=self.escape_unenclosed_field,
            escape=self.escape,
            field_delimiter=self.field_delimiter,
            field_optionally_enclosed_by=self.field_optionally_enclosed_by,
            file_extension=self.file_extension,
            null_if=self.null_if,
            parse_header=self.parse_header,
            record_delimiter=self.record_delimiter,
            replace_invalid_characters=self.replace_invalid_characters,
            skip_blank_lines=self.skip_blank_lines,
            skip_byte_order_mark=self.skip_byte_order_mark,
            skip_header=self.skip_header,
            time_format=self.time_format,
            timestamp_format=self.timestamp_format,
            trim_space=self.trim_space,
        )


@dataclass
class XmlOptions(FileFormatOptions):
    """Options for configuring XML file formats in Snowflake.

    Attributes:
        compression (Optional[StandardCompressionType]): Compression algorithm
        disable_auto_convert (Optional[bool]): Disable automatic type conversion
        disable_snowflake_data (Optional[bool]): Disable Snowflake data type inference
        ignore_utf8_errors (Optional[bool]): Ignore UTF-8 encoding errors
        preserve_space (Optional[bool]): Preserve whitespace in XML elements
        replace_invalid_characters (Optional[bool]): Replace invalid UTF-8 characters
        skip_byte_order_mark (Optional[bool]): Skip UTF-8 BOM if present
        strip_outer_element (Optional[bool]): Remove outer XML element

    Example:
        >>> options = XmlOptions.builder()\\
        ...     .with_compression(CompressionType.GZIP)\\
        ...     .with_preserve_space(True)\\
        ...     .build()
    """

    compression: Optional[StandardCompressionType] = None
    disable_auto_convert: Optional[bool] = None
    disable_snowflake_data: Optional[bool] = None
    ignore_utf8_errors: Optional[bool] = None
    preserve_space: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None
    strip_outer_element: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'XmlOptionsBuilder':
        """Creates a builder for constructing XmlOptions instances."""
        return XmlOptionsBuilder()

    def to_sql(self) -> str:
        """Converts the XmlOptions instance to a SQL string."""
        parts = ["TYPE = XML"]

        if self.compression:
            parts.append(f"COMPRESSION = {str(self.compression)}")
        if self.ignore_utf8_errors is not None:
            parts.append(
                f"IGNORE_UTF8_ERRORS = {sql_format_boolean(self.ignore_utf8_errors)}"
            )
        if self.preserve_space is not None:
            parts.append(f"PRESERVE_SPACE = {sql_format_boolean(self.preserve_space)}")
        if self.strip_outer_element is not None:
            parts.append(
                f"STRIP_OUTER_ELEMENT = {sql_format_boolean(self.strip_outer_element)}"
            )
        if self.disable_snowflake_data is not None:
            parts.append(
                f"DISABLE_SNOWFLAKE_DATA = {sql_format_boolean(self.disable_snowflake_data)}"
            )
        if self.disable_auto_convert is not None:
            parts.append(
                f"DISABLE_AUTO_CONVERT = {sql_format_boolean(self.disable_auto_convert)}"
            )
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {sql_format_boolean(self.replace_invalid_characters)}"
            )
        if self.skip_byte_order_mark is not None:
            parts.append(
                f"SKIP_BYTE_ORDER_MARK = {sql_format_boolean(self.skip_byte_order_mark)}"
            )

        return " ".join(parts)


class XmlOptionsBuilder:
    def __init__(self):
        """Initializes an XmlOptionsBuilder instance."""
        self.compression: Optional[StandardCompressionType] = None
        self.disable_auto_convert: Optional[bool] = None
        self.disable_snowflake_data: Optional[bool] = None
        self.ignore_utf8_errors: Optional[bool] = None
        self.preserve_space: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.skip_byte_order_mark: Optional[bool] = None
        self.strip_outer_element: Optional[bool] = None

    def build(self) -> XmlOptions:
        """Builds the XmlOptions instance."""
        return XmlOptions(
            compression=self.compression,
            disable_auto_convert=self.disable_auto_convert,
            disable_snowflake_data=self.disable_snowflake_data,
            ignore_utf8_errors=self.ignore_utf8_errors,
            preserve_space=self.preserve_space,
            replace_invalid_characters=self.replace_invalid_characters,
            skip_byte_order_mark=self.skip_byte_order_mark,
            strip_outer_element=self.strip_outer_element,
        )

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'XmlOptionsBuilder':
        """Sets the compression algorithm."""
        self.compression = compression
        return self

    def with_disable_auto_convert(
        self, disable_auto_convert: bool
    ) -> 'XmlOptionsBuilder':
        """Sets whether to disable automatic type conversion."""
        self.disable_auto_convert = disable_auto_convert
        return self

    def with_disable_snowflake_data(
        self, disable_snowflake_data: bool
    ) -> 'XmlOptionsBuilder':
        """Sets whether to disable Snowflake data type inference."""
        self.disable_snowflake_data = disable_snowflake_data
        return self

    def with_ignore_utf8_errors(self, ignore_utf8_errors: bool) -> 'XmlOptionsBuilder':
        """Sets whether to ignore UTF-8 encoding errors."""
        self.ignore_utf8_errors = ignore_utf8_errors
        return self

    def with_preserve_space(self, preserve_space: bool) -> 'XmlOptionsBuilder':
        """Sets whether to preserve whitespace in XML elements."""
        self.preserve_space = preserve_space
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'XmlOptionsBuilder':
        """Sets whether to replace invalid UTF-8 characters."""
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_skip_byte_order_mark(
        self, skip_byte_order_mark: bool
    ) -> 'XmlOptionsBuilder':
        """Sets whether to skip UTF-8 BOM if present."""
        self.skip_byte_order_mark = skip_byte_order_mark
        return self

    def with_strip_outer_element(
        self, strip_outer_element: bool
    ) -> 'XmlOptionsBuilder':
        """Sets whether to remove the outer XML element."""
        self.strip_outer_element = strip_outer_element
        return self


@dataclass
class OrcOptions(FileFormatOptions):
    """Options for configuring ORC file formats in Snowflake.

    Attributes:
        null_if (Optional[List[str]]): Strings to interpret as NULL values
        replace_invalid_characters (Optional[bool]): Replace invalid UTF-8 characters
        trim_space (Optional[bool]): Trim whitespace from string fields

    Example:
        >>> options = OrcOptions.builder()\\
        ...     .with_trim_space(True)\\
        ...     .with_null_if(["NULL", "\\N"])\\
        ...     .build()
    """

    null_if: Optional[List[str]] = None
    replace_invalid_characters: Optional[bool] = None
    trim_space: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'OrcOptionsBuilder':
        """Creates a builder for constructing OrcOptions instances."""
        return OrcOptionsBuilder()

    def to_sql(self) -> str:
        """Converts the OrcOptions instance to a SQL string."""
        parts = ["TYPE = ORC"]

        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {sql_format_boolean(self.trim_space)}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {sql_format_boolean(self.replace_invalid_characters)}"
            )
        if self.null_if:
            parts.append(f"NULL_IF = {sql_format_list(self.null_if)}")

        return " ".join(parts)


class OrcOptionsBuilder:
    def __init__(self):
        """Initializes an OrcOptionsBuilder instance."""
        self.null_if: Optional[List[str]] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.trim_space: Optional[bool] = None

    def with_null_if(self, null_if: List[str]) -> 'OrcOptionsBuilder':
        """Sets the strings to interpret as NULL values."""
        self.null_if = null_if
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'OrcOptionsBuilder':
        """Sets whether to replace invalid UTF-8 characters."""
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_trim_space(self, trim_space: bool) -> 'OrcOptionsBuilder':
        """Sets whether to trim whitespace from string fields."""
        self.trim_space = trim_space
        return self

    def build(self) -> OrcOptions:
        """Builds the OrcOptions instance."""
        return OrcOptions(
            null_if=self.null_if,
            replace_invalid_characters=self.replace_invalid_characters,
            trim_space=self.trim_space,
        )
