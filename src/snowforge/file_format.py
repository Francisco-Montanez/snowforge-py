from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, Flag, auto
from typing import List, Literal, Optional, Union


class BinaryFormat(Enum):
    HEX = "HEX"
    BASE64 = "BASE64"
    UTF8 = "UTF8"

    def __str__(self) -> str:
        return self.value


class CompressionType(str, Enum):
    """Base compression options available across all formats"""

    AUTO = "AUTO"
    NONE = "NONE"
    GZIP = "GZIP"
    BROTLI = "BROTLI"
    ZSTD = "ZSTD"
    DEFLATE = "DEFLATE"
    RAW_DEFLATE = "RAWDEFLATE"
    BZ2 = "BZ2"
    LZO = "LZO"
    SNAPPY = "SNAPPY"

    def __str__(self) -> str:
        return self.value


# Define valid compression types for each format using TypeVar and Literal
StandardCompressionType = Literal[
    CompressionType.AUTO,
    CompressionType.NONE,
    CompressionType.GZIP,
    CompressionType.BROTLI,
    CompressionType.ZSTD,
    CompressionType.DEFLATE,
    CompressionType.RAW_DEFLATE,
    CompressionType.BZ2,
]

ParquetCompressionType = Literal[
    CompressionType.AUTO,
    CompressionType.NONE,
    CompressionType.LZO,
    CompressionType.SNAPPY,
]


class FileFormatOptions(ABC):
    @abstractmethod
    def to_sql(self) -> str:
        """Convert the options to a SQL string."""
        pass


@dataclass
class FileFormat:
    name: str
    options: Optional[FileFormatOptions] = None
    sql_statement: str = ""

    @classmethod
    def builder(cls, name: str) -> FileFormatBuilder:
        return FileFormatBuilder(name)


class FileFormatBuilder:
    def __init__(self, name: str):
        self._name = name
        self._temporary = False
        self._volatile = False
        self._options: Optional[FileFormatOptions] = None
        self._create_or_replace = False
        self._create_if_not_exists = False
        self._comment: Optional[str] = None

    def with_create_or_replace(self) -> FileFormatBuilder:
        self._create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> FileFormatBuilder:
        self._create_if_not_exists = True
        return self

    def with_temporary(self) -> FileFormatBuilder:
        self._temporary = True
        return self

    def with_volatile(self) -> FileFormatBuilder:
        self._volatile = True
        return self

    def with_options(self, options: FileFormatOptions) -> FileFormatBuilder:
        self._options = options
        return self

    def with_comment(self, comment: str) -> FileFormatBuilder:
        self._comment = comment
        return self

    def _to_sql(self) -> str:
        parts = []

        if self._create_or_replace:
            parts.append("CREATE OR REPLACE")
        else:
            parts.append("CREATE")

        if self._temporary:
            parts.append("TEMPORARY")
        elif self._volatile:
            parts.append("VOLATILE")

        parts.append("FILE FORMAT")

        if self._create_if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self._name)

        if self._options:
            parts.append(self._options.to_sql())

        if self._comment:
            parts.append(f"COMMENT = '{self._comment.replace(chr(39), chr(39)*2)}'")

        return " ".join(parts)

    def build(self) -> FileFormat:
        return FileFormat(
            name=self._name, options=self._options, sql_statement=self._to_sql()
        )


class FileFormatSpecification:
    def __init__(self, spec: Union[str, FileFormat]):
        if isinstance(spec, str):
            self.type = "named"
            self.value = spec
        else:
            self.type = "inline"
            self.value = spec

    @classmethod
    def named(cls, name: str) -> 'FileFormatSpecification':
        return cls(name)

    @classmethod
    def inline(cls, file_format: FileFormat) -> 'FileFormatSpecification':
        return cls(file_format)

    def to_sql(self) -> str:
        if self.type == "named":
            return f"FORMAT_NAME = '{self.value}'"
        if isinstance(self.value, FileFormat):
            return self.value.options.to_sql() if self.value.options else ""
        return ""


@dataclass
class AvroOptions(FileFormatOptions):
    compression: Optional[StandardCompressionType] = None
    trim_space: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    null_if: Optional[List[str]] = None

    @classmethod
    def builder(cls) -> 'AvroOptionsBuilder':
        return AvroOptionsBuilder()

    def to_sql(self) -> str:
        parts = ["TYPE = AVRO"]

        if self.compression:
            parts.append(f"COMPRESSION = {self.compression}")
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {str(self.trim_space).upper()}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {str(self.replace_invalid_characters).upper()}"
            )
        if self.null_if:
            null_if_str = ", ".join(f"'{val}'" for val in self.null_if)
            parts.append(f"NULL_IF = ({null_if_str})")

        return " ".join(parts)


class AvroOptionsBuilder:
    def __init__(self):
        self.compression: Optional[StandardCompressionType] = None
        self.trim_space: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.null_if: Optional[List[str]] = None

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'AvroOptionsBuilder':
        self.compression = compression
        return self

    def with_trim_space(self, trim_space: bool) -> 'AvroOptionsBuilder':
        self.trim_space = trim_space
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'AvroOptionsBuilder':
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_null_if(self, null_if: List[str]) -> 'AvroOptionsBuilder':
        self.null_if = null_if
        return self

    def build(self) -> AvroOptions:
        return AvroOptions(
            compression=self.compression,
            trim_space=self.trim_space,
            replace_invalid_characters=self.replace_invalid_characters,
            null_if=self.null_if,
        )


@dataclass
class ParquetOptions(FileFormatOptions):
    compression: Optional[ParquetCompressionType] = None
    binary_as_text: Optional[bool] = None
    use_logical_type: Optional[bool] = None
    trim_space: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    null_if: Optional[List[str]] = None
    use_vectorized_scanner: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'ParquetOptionsBuilder':
        return ParquetOptionsBuilder()

    def to_sql(self) -> str:
        parts = ["TYPE = PARQUET"]

        if self.compression:
            parts.append(f"COMPRESSION = {self.compression}")
        if self.binary_as_text is not None:
            parts.append(f"BINARY_AS_TEXT = {str(self.binary_as_text).upper()}")
        if self.use_logical_type is not None:
            parts.append(f"USE_LOGICAL_TYPE = {str(self.use_logical_type).upper()}")
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {str(self.trim_space).upper()}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {str(self.replace_invalid_characters).upper()}"
            )
        if self.null_if:
            null_if_str = ", ".join(f"'{val}'" for val in self.null_if)
            parts.append(f"NULL_IF = ({null_if_str})")
        if self.use_vectorized_scanner is not None:
            parts.append(
                f"USE_VECTORIZED_SCANNER = {str(self.use_vectorized_scanner).upper()}"
            )

        return " ".join(parts)


class ParquetOptionsBuilder:
    def __init__(self):
        self.compression: Optional[ParquetCompressionType] = None
        self.binary_as_text: Optional[bool] = None
        self.use_logical_type: Optional[bool] = None
        self.trim_space: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.null_if: Optional[List[str]] = None
        self.use_vectorized_scanner: Optional[bool] = None

    def with_compression(
        self, compression: ParquetCompressionType
    ) -> 'ParquetOptionsBuilder':
        self.compression = compression
        return self

    def with_binary_as_text(self, binary_as_text: bool) -> 'ParquetOptionsBuilder':
        self.binary_as_text = binary_as_text
        return self

    def with_use_logical_type(self, use_logical_type: bool) -> 'ParquetOptionsBuilder':
        self.use_logical_type = use_logical_type
        return self

    def with_trim_space(self, trim_space: bool) -> 'ParquetOptionsBuilder':
        self.trim_space = trim_space
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'ParquetOptionsBuilder':
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_null_if(self, null_if: List[str]) -> 'ParquetOptionsBuilder':
        self.null_if = null_if
        return self

    def with_use_vectorized_scanner(
        self, use_vectorized_scanner: bool
    ) -> 'ParquetOptionsBuilder':
        self.use_vectorized_scanner = use_vectorized_scanner
        return self

    def build(self) -> ParquetOptions:
        return ParquetOptions(
            compression=self.compression,
            binary_as_text=self.binary_as_text,
            use_logical_type=self.use_logical_type,
            trim_space=self.trim_space,
            replace_invalid_characters=self.replace_invalid_characters,
            null_if=self.null_if,
            use_vectorized_scanner=self.use_vectorized_scanner,
        )


@dataclass
class JsonOptions(FileFormatOptions):
    compression: Optional[StandardCompressionType] = None
    date_format: Optional[str] = None
    time_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    binary_format: Optional[BinaryFormat] = None
    trim_space: Optional[bool] = None
    null_if: Optional[List[str]] = None
    file_extension: Optional[str] = None
    enable_octal: Optional[bool] = None
    allow_duplicate: Optional[bool] = None
    strip_outer_array: Optional[bool] = None
    strip_null_values: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    ignore_utf8_errors: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'JsonOptionsBuilder':
        return JsonOptionsBuilder()

    def to_sql(self) -> str:
        parts = ["TYPE = JSON"]

        if self.compression:
            parts.append(f"COMPRESSION = {self.compression}")
        if self.date_format:
            parts.append(f"DATE_FORMAT = '{self.date_format}'")
        if self.time_format:
            parts.append(f"TIME_FORMAT = '{self.time_format}'")
        if self.timestamp_format:
            parts.append(f"TIMESTAMP_FORMAT = '{self.timestamp_format}'")
        if self.binary_format:
            parts.append(f"BINARY_FORMAT = {self.binary_format}")
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {str(self.trim_space).upper()}")
        if self.enable_octal is not None:
            parts.append(f"ENABLE_OCTAL = {str(self.enable_octal).upper()}")
        if self.allow_duplicate is not None:
            parts.append(f"ALLOW_DUPLICATE = {str(self.allow_duplicate).upper()}")
        if self.strip_outer_array is not None:
            parts.append(f"STRIP_OUTER_ARRAY = {str(self.strip_outer_array).upper()}")
        if self.strip_null_values is not None:
            parts.append(f"STRIP_NULL_VALUES = {str(self.strip_null_values).upper()}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {str(self.replace_invalid_characters).upper()}"
            )
        if self.ignore_utf8_errors is not None:
            parts.append(f"IGNORE_UTF8_ERRORS = {str(self.ignore_utf8_errors).upper()}")
        if self.skip_byte_order_mark is not None:
            parts.append(
                f"SKIP_BYTE_ORDER_MARK = {str(self.skip_byte_order_mark).upper()}"
            )
        if self.file_extension:
            parts.append(f"FILE_EXTENSION = '{self.file_extension}'")
        if self.null_if:
            null_if_str = ", ".join(f"'{val}'" for val in self.null_if)
            parts.append(f"NULL_IF = ({null_if_str})")

        return " ".join(parts)


class JsonOptionsBuilder:
    def __init__(self):
        self.compression: Optional[StandardCompressionType] = None
        self.date_format: Optional[str] = None
        self.time_format: Optional[str] = None
        self.timestamp_format: Optional[str] = None
        self.binary_format: Optional[BinaryFormat] = None
        self.trim_space: Optional[bool] = None
        self.null_if: Optional[List[str]] = None
        self.file_extension: Optional[str] = None
        self.enable_octal: Optional[bool] = None
        self.allow_duplicate: Optional[bool] = None
        self.strip_outer_array: Optional[bool] = None
        self.strip_null_values: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.ignore_utf8_errors: Optional[bool] = None
        self.skip_byte_order_mark: Optional[bool] = None

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'JsonOptionsBuilder':
        self.compression = compression
        return self

    def with_date_format(self, date_format: str) -> 'JsonOptionsBuilder':
        self.date_format = date_format
        return self

    def with_time_format(self, time_format: str) -> 'JsonOptionsBuilder':
        self.time_format = time_format
        return self

    def with_timestamp_format(self, timestamp_format: str) -> 'JsonOptionsBuilder':
        self.timestamp_format = timestamp_format
        return self

    def with_binary_format(self, binary_format: BinaryFormat) -> 'JsonOptionsBuilder':
        self.binary_format = binary_format
        return self

    def with_trim_space(self, trim_space: bool) -> 'JsonOptionsBuilder':
        self.trim_space = trim_space
        return self

    def with_null_if(self, null_if: List[str]) -> 'JsonOptionsBuilder':
        self.null_if = null_if
        return self

    def with_file_extension(self, file_extension: str) -> 'JsonOptionsBuilder':
        self.file_extension = file_extension
        return self

    def with_enable_octal(self, enable_octal: bool) -> 'JsonOptionsBuilder':
        self.enable_octal = enable_octal
        return self

    def with_allow_duplicate(self, allow_duplicate: bool) -> 'JsonOptionsBuilder':
        self.allow_duplicate = allow_duplicate
        return self

    def with_strip_outer_array(self, strip_outer_array: bool) -> 'JsonOptionsBuilder':
        self.strip_outer_array = strip_outer_array
        return self

    def with_strip_null_values(self, strip_null_values: bool) -> 'JsonOptionsBuilder':
        self.strip_null_values = strip_null_values
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'JsonOptionsBuilder':
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_ignore_utf8_errors(self, ignore_utf8_errors: bool) -> 'JsonOptionsBuilder':
        self.ignore_utf8_errors = ignore_utf8_errors
        return self

    def with_skip_byte_order_mark(
        self, skip_byte_order_mark: bool
    ) -> 'JsonOptionsBuilder':
        self.skip_byte_order_mark = skip_byte_order_mark
        return self

    def build(self) -> JsonOptions:
        return JsonOptions(
            compression=self.compression,
            date_format=self.date_format,
            time_format=self.time_format,
            timestamp_format=self.timestamp_format,
            binary_format=self.binary_format,
            trim_space=self.trim_space,
            null_if=self.null_if,
            file_extension=self.file_extension,
            enable_octal=self.enable_octal,
            allow_duplicate=self.allow_duplicate,
            strip_outer_array=self.strip_outer_array,
            strip_null_values=self.strip_null_values,
            replace_invalid_characters=self.replace_invalid_characters,
            ignore_utf8_errors=self.ignore_utf8_errors,
            skip_byte_order_mark=self.skip_byte_order_mark,
        )


@dataclass
class CsvOptions(FileFormatOptions):
    compression: Optional[StandardCompressionType] = None
    record_delimiter: Optional[str] = None
    field_delimiter: Optional[str] = None
    file_extension: Optional[str] = None
    parse_header: Optional[bool] = None
    skip_header: Optional[int] = None
    skip_blank_lines: Optional[bool] = None
    date_format: Optional[str] = None
    time_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    binary_format: Optional[BinaryFormat] = None
    escape: Optional[str] = None
    escape_unenclosed_field: Optional[str] = None
    trim_space: Optional[bool] = None
    field_optionally_enclosed_by: Optional[str] = None
    null_if: Optional[List[str]] = None
    error_on_column_count_mismatch: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    empty_field_as_null: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None
    encoding: Optional[str] = None

    @classmethod
    def builder(cls) -> 'CsvOptionsBuilder':
        return CsvOptionsBuilder()

    def to_sql(self) -> str:
        parts = ["TYPE = CSV"]

        if self.compression:
            parts.append(f"COMPRESSION = {self.compression}")
        if self.record_delimiter:
            parts.append(f"RECORD_DELIMITER = '{self.record_delimiter}'")
        if self.field_delimiter:
            parts.append(f"FIELD_DELIMITER = '{self.field_delimiter}'")
        if self.file_extension:
            parts.append(f"FILE_EXTENSION = '{self.file_extension}'")
        if self.parse_header is not None:
            parts.append(f"PARSE_HEADER = {str(self.parse_header).upper()}")
        if self.skip_header is not None:
            parts.append(f"SKIP_HEADER = {self.skip_header}")
        if self.skip_blank_lines is not None:
            parts.append(f"SKIP_BLANK_LINES = {str(self.skip_blank_lines).upper()}")
        if self.date_format:
            parts.append(f"DATE_FORMAT = '{self.date_format}'")
        if self.time_format:
            parts.append(f"TIME_FORMAT = '{self.time_format}'")
        if self.timestamp_format:
            parts.append(f"TIMESTAMP_FORMAT = '{self.timestamp_format}'")
        if self.binary_format:
            parts.append(f"BINARY_FORMAT = {self.binary_format}")
        if self.escape:
            parts.append(f"ESCAPE = '{self.escape}'")
        if self.escape_unenclosed_field:
            parts.append(f"ESCAPE_UNENCLOSED_FIELD = '{self.escape_unenclosed_field}'")
        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {str(self.trim_space).upper()}")
        if self.field_optionally_enclosed_by:
            parts.append(
                f"FIELD_OPTIONALLY_ENCLOSED_BY = '{self.field_optionally_enclosed_by}'"
            )
        if self.null_if:
            null_if_str = ", ".join(f"'{val}'" for val in self.null_if)
            parts.append(f"NULL_IF = ({null_if_str})")
        if self.error_on_column_count_mismatch is not None:
            parts.append(
                f"ERROR_ON_COLUMN_COUNT_MISMATCH = {str(self.error_on_column_count_mismatch).upper()}"
            )
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {str(self.replace_invalid_characters).upper()}"
            )
        if self.empty_field_as_null is not None:
            parts.append(
                f"EMPTY_FIELD_AS_NULL = {str(self.empty_field_as_null).upper()}"
            )
        if self.skip_byte_order_mark is not None:
            parts.append(
                f"SKIP_BYTE_ORDER_MARK = {str(self.skip_byte_order_mark).upper()}"
            )
        if self.encoding:
            parts.append(f"ENCODING = '{self.encoding}'")

        return " ".join(parts)


class CsvOptionsBuilder:
    def __init__(self):
        self.compression: Optional[StandardCompressionType] = None
        self.record_delimiter: Optional[str] = None
        self.field_delimiter: Optional[str] = None
        self.file_extension: Optional[str] = None
        self.parse_header: Optional[bool] = None
        self.skip_header: Optional[int] = None
        self.skip_blank_lines: Optional[bool] = None
        self.date_format: Optional[str] = None
        self.time_format: Optional[str] = None
        self.timestamp_format: Optional[str] = None
        self.binary_format: Optional[BinaryFormat] = None
        self.escape: Optional[str] = None
        self.escape_unenclosed_field: Optional[str] = None
        self.trim_space: Optional[bool] = None
        self.field_optionally_enclosed_by: Optional[str] = None
        self.null_if: Optional[List[str]] = None
        self.error_on_column_count_mismatch: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.empty_field_as_null: Optional[bool] = None
        self.skip_byte_order_mark: Optional[bool] = None
        self.encoding: Optional[str] = None

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'CsvOptionsBuilder':
        self.compression = compression
        return self

    def with_record_delimiter(self, record_delimiter: str) -> 'CsvOptionsBuilder':
        self.record_delimiter = record_delimiter
        return self

    def with_field_delimiter(self, field_delimiter: str) -> 'CsvOptionsBuilder':
        self.field_delimiter = field_delimiter
        return self

    def with_file_extension(self, file_extension: str) -> 'CsvOptionsBuilder':
        self.file_extension = file_extension
        return self

    def with_parse_header(self, parse_header: bool) -> 'CsvOptionsBuilder':
        self.parse_header = parse_header
        return self

    def with_skip_header(self, skip_header: int) -> 'CsvOptionsBuilder':
        self.skip_header = skip_header
        return self

    def with_skip_blank_lines(self, skip_blank_lines: bool) -> 'CsvOptionsBuilder':
        self.skip_blank_lines = skip_blank_lines
        return self

    def with_date_format(self, date_format: str) -> 'CsvOptionsBuilder':
        self.date_format = date_format
        return self

    def with_time_format(self, time_format: str) -> 'CsvOptionsBuilder':
        self.time_format = time_format
        return self

    def with_timestamp_format(self, timestamp_format: str) -> 'CsvOptionsBuilder':
        self.timestamp_format = timestamp_format
        return self

    def with_binary_format(self, binary_format: BinaryFormat) -> 'CsvOptionsBuilder':
        self.binary_format = binary_format
        return self

    def with_escape(self, escape: str) -> 'CsvOptionsBuilder':
        self.escape = escape
        return self

    def with_escape_unenclosed_field(
        self, escape_unenclosed_field: str
    ) -> 'CsvOptionsBuilder':
        self.escape_unenclosed_field = escape_unenclosed_field
        return self

    def with_trim_space(self, trim_space: bool) -> 'CsvOptionsBuilder':
        self.trim_space = trim_space
        return self

    def with_field_optionally_enclosed_by(
        self, field_optionally_enclosed_by: str
    ) -> 'CsvOptionsBuilder':
        self.field_optionally_enclosed_by = field_optionally_enclosed_by
        return self

    def with_null_if(self, null_if: List[str]) -> 'CsvOptionsBuilder':
        self.null_if = null_if
        return self

    def with_error_on_column_count_mismatch(
        self, error_on_column_count_mismatch: bool
    ) -> 'CsvOptionsBuilder':
        self.error_on_column_count_mismatch = error_on_column_count_mismatch
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'CsvOptionsBuilder':
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_empty_field_as_null(
        self, empty_field_as_null: bool
    ) -> 'CsvOptionsBuilder':
        self.empty_field_as_null = empty_field_as_null
        return self

    def with_skip_byte_order_mark(
        self, skip_byte_order_mark: bool
    ) -> 'CsvOptionsBuilder':
        self.skip_byte_order_mark = skip_byte_order_mark
        return self

    def with_encoding(self, encoding: str) -> 'CsvOptionsBuilder':
        self.encoding = encoding
        return self

    def build(self) -> CsvOptions:
        return CsvOptions(
            compression=self.compression,
            record_delimiter=self.record_delimiter,
            field_delimiter=self.field_delimiter,
            file_extension=self.file_extension,
            parse_header=self.parse_header,
            skip_header=self.skip_header,
            skip_blank_lines=self.skip_blank_lines,
            date_format=self.date_format,
            time_format=self.time_format,
            timestamp_format=self.timestamp_format,
            binary_format=self.binary_format,
            escape=self.escape,
            escape_unenclosed_field=self.escape_unenclosed_field,
            trim_space=self.trim_space,
            field_optionally_enclosed_by=self.field_optionally_enclosed_by,
            null_if=self.null_if,
            error_on_column_count_mismatch=self.error_on_column_count_mismatch,
            replace_invalid_characters=self.replace_invalid_characters,
            empty_field_as_null=self.empty_field_as_null,
            skip_byte_order_mark=self.skip_byte_order_mark,
            encoding=self.encoding,
        )


@dataclass
class XmlOptions(FileFormatOptions):
    compression: Optional[StandardCompressionType] = None
    ignore_utf8_errors: Optional[bool] = None
    preserve_space: Optional[bool] = None
    strip_outer_element: Optional[bool] = None
    disable_snowflake_data: Optional[bool] = None
    disable_auto_convert: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    skip_byte_order_mark: Optional[bool] = None

    @classmethod
    def builder(cls) -> 'XmlOptionsBuilder':
        return XmlOptionsBuilder()

    def to_sql(self) -> str:
        parts = ["TYPE = XML"]

        if self.compression:
            parts.append(f"COMPRESSION = {self.compression}")
        if self.ignore_utf8_errors is not None:
            parts.append(f"IGNORE_UTF8_ERRORS = {str(self.ignore_utf8_errors).upper()}")
        if self.preserve_space is not None:
            parts.append(f"PRESERVE_SPACE = {str(self.preserve_space).upper()}")
        if self.strip_outer_element is not None:
            parts.append(
                f"STRIP_OUTER_ELEMENT = {str(self.strip_outer_element).upper()}"
            )
        if self.disable_snowflake_data is not None:
            parts.append(
                f"DISABLE_SNOWFLAKE_DATA = {str(self.disable_snowflake_data).upper()}"
            )
        if self.disable_auto_convert is not None:
            parts.append(
                f"DISABLE_AUTO_CONVERT = {str(self.disable_auto_convert).upper()}"
            )
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {str(self.replace_invalid_characters).upper()}"
            )
        if self.skip_byte_order_mark is not None:
            parts.append(
                f"SKIP_BYTE_ORDER_MARK = {str(self.skip_byte_order_mark).upper()}"
            )

        return " ".join(parts)


class XmlOptionsBuilder:
    def __init__(self):
        self.compression: Optional[StandardCompressionType] = None
        self.ignore_utf8_errors: Optional[bool] = None
        self.preserve_space: Optional[bool] = None
        self.strip_outer_element: Optional[bool] = None
        self.disable_snowflake_data: Optional[bool] = None
        self.disable_auto_convert: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.skip_byte_order_mark: Optional[bool] = None

    def with_compression(
        self, compression: StandardCompressionType
    ) -> 'XmlOptionsBuilder':
        self.compression = compression
        return self

    def with_ignore_utf8_errors(self, ignore_utf8_errors: bool) -> 'XmlOptionsBuilder':
        self.ignore_utf8_errors = ignore_utf8_errors
        return self

    def with_preserve_space(self, preserve_space: bool) -> 'XmlOptionsBuilder':
        self.preserve_space = preserve_space
        return self

    def with_strip_outer_element(
        self, strip_outer_element: bool
    ) -> 'XmlOptionsBuilder':
        self.strip_outer_element = strip_outer_element
        return self

    def with_disable_snowflake_data(
        self, disable_snowflake_data: bool
    ) -> 'XmlOptionsBuilder':
        self.disable_snowflake_data = disable_snowflake_data
        return self

    def with_disable_auto_convert(
        self, disable_auto_convert: bool
    ) -> 'XmlOptionsBuilder':
        self.disable_auto_convert = disable_auto_convert
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'XmlOptionsBuilder':
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_skip_byte_order_mark(
        self, skip_byte_order_mark: bool
    ) -> 'XmlOptionsBuilder':
        self.skip_byte_order_mark = skip_byte_order_mark
        return self

    def build(self) -> XmlOptions:
        return XmlOptions(
            compression=self.compression,
            ignore_utf8_errors=self.ignore_utf8_errors,
            preserve_space=self.preserve_space,
            strip_outer_element=self.strip_outer_element,
            disable_snowflake_data=self.disable_snowflake_data,
            disable_auto_convert=self.disable_auto_convert,
            replace_invalid_characters=self.replace_invalid_characters,
            skip_byte_order_mark=self.skip_byte_order_mark,
        )


@dataclass
class OrcOptions(FileFormatOptions):
    trim_space: Optional[bool] = None
    replace_invalid_characters: Optional[bool] = None
    null_if: Optional[List[str]] = None

    @classmethod
    def builder(cls) -> 'OrcOptionsBuilder':
        return OrcOptionsBuilder()

    def to_sql(self) -> str:
        parts = ["TYPE = ORC"]

        if self.trim_space is not None:
            parts.append(f"TRIM_SPACE = {str(self.trim_space).upper()}")
        if self.replace_invalid_characters is not None:
            parts.append(
                f"REPLACE_INVALID_CHARACTERS = {str(self.replace_invalid_characters).upper()}"
            )
        if self.null_if:
            null_if_str = ", ".join(f"'{val}'" for val in self.null_if)
            parts.append(f"NULL_IF = ({null_if_str})")

        return " ".join(parts)


class OrcOptionsBuilder:
    def __init__(self):
        self.trim_space: Optional[bool] = None
        self.replace_invalid_characters: Optional[bool] = None
        self.null_if: Optional[List[str]] = None

    def with_trim_space(self, trim_space: bool) -> 'OrcOptionsBuilder':
        self.trim_space = trim_space
        return self

    def with_replace_invalid_characters(
        self, replace_invalid_characters: bool
    ) -> 'OrcOptionsBuilder':
        self.replace_invalid_characters = replace_invalid_characters
        return self

    def with_null_if(self, null_if: List[str]) -> 'OrcOptionsBuilder':
        self.null_if = null_if
        return self

    def build(self) -> OrcOptions:
        return OrcOptions(
            trim_space=self.trim_space,
            replace_invalid_characters=self.replace_invalid_characters,
            null_if=self.null_if,
        )
