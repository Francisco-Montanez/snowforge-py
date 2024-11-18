import pytest

from snowforge.file_format import (
    BinaryFormat,
    CompressionType,
    CsvOptions,
    FileFormat,
    FileFormatSpecification,
    JsonOptions,
    ParquetOptions,
)


def test_binary_format_str():
    assert str(BinaryFormat.BASE64) == "BASE64"
    assert str(BinaryFormat.HEX) == "HEX"
    assert str(BinaryFormat.UTF8) == "UTF8"


def test_compression_type_str():
    assert str(CompressionType.AUTO) == "AUTO"
    assert str(CompressionType.GZIP) == "GZIP"
    assert str(CompressionType.NONE) == "NONE"
    assert str(CompressionType.SNAPPY) == "SNAPPY"


def test_file_format_builder_basic():
    format = (
        FileFormat.builder("test_format")
        .with_create_or_replace()
        .with_temporary()
        .build()
    )

    assert format.name == "test_format"
    assert format.create_or_replace is True
    assert format.temporary is True
    assert format.volatile is False


def test_file_format_builder_with_options():
    csv_options = (
        CsvOptions.builder()
        .with_compression(CompressionType.GZIP)
        .with_field_delimiter(",")
        .with_skip_header(1)
        .build()
    )

    format = (
        FileFormat.builder("test_format")
        .with_options(csv_options)
        .with_comment("Test comment")
        .build()
    )

    assert format.options == csv_options
    assert format.comment == "Test comment"


def test_file_format_sql_generation():
    format = (
        FileFormat.builder("test_format")
        .with_create_or_replace()
        .with_temporary()
        .with_comment("Test format")
        .build()
    )

    expected_sql = (
        "CREATE OR REPLACE TEMPORARY FILE FORMAT test_format COMMENT = 'Test format'"
    )
    assert format.to_sql() == expected_sql


def test_file_format_specification():
    # Test named format
    named_spec = FileFormatSpecification.named("existing_format")
    assert named_spec.type == "named"
    assert named_spec.to_sql() == "FORMAT_NAME = 'existing_format'"

    # Test inline format
    csv_options = CsvOptions(field_delimiter=",", skip_header=1)
    format = FileFormat("inline_format", options=csv_options)
    inline_spec = FileFormatSpecification.inline(format)
    assert inline_spec.type == "inline"
    assert "FIELD_DELIMITER = ','" in inline_spec.to_sql()


def test_csv_options_builder():
    options = (
        CsvOptions.builder()
        .with_compression(CompressionType.GZIP)
        .with_field_delimiter(",")
        .with_skip_header(1)
        .with_parse_header(True)
        .with_trim_space(True)
        .with_null_if(["NULL", ""])
        .build()
    )

    assert options.compression == CompressionType.GZIP
    assert options.field_delimiter == ","
    assert options.skip_header == 1
    assert options.parse_header is True
    assert options.trim_space is True
    assert options.null_if == ["NULL", ""]


def test_csv_options_sql_generation():
    options = CsvOptions(
        compression=CompressionType.GZIP,
        field_delimiter=",",
        skip_header=1,
        parse_header=True,
        binary_format=BinaryFormat.BASE64,
    )

    sql = options.to_sql()
    assert "TYPE = CSV" in sql
    assert "COMPRESSION = GZIP" in sql
    assert "FIELD_DELIMITER = ','" in sql
    assert "SKIP_HEADER = 1" in sql
    assert "PARSE_HEADER = TRUE" in sql
    assert "BINARY_FORMAT = BASE64" in sql


def test_csv_options_special_characters():
    options = CsvOptions(
        field_delimiter="\t", escape="\\", field_optionally_enclosed_by='"'
    )

    sql = options.to_sql()
    assert "FIELD_DELIMITER = '\t'" in sql
    assert "ESCAPE = '\\\\'" in sql
    assert "FIELD_OPTIONALLY_ENCLOSED_BY = '\"'" in sql


def test_json_options_builder():
    options = (
        JsonOptions.builder()
        .with_compression(CompressionType.AUTO)
        .with_file_extension(".json")
        .with_strip_outer_array(True)
        .with_allow_duplicate(False)
        .with_strip_null_values(True)
        .build()
    )

    assert options.compression == CompressionType.AUTO
    assert options.file_extension == ".json"
    assert options.strip_outer_array is True
    assert options.allow_duplicate is False
    assert options.strip_null_values is True


def test_json_options_sql_generation():
    options = JsonOptions(
        compression=CompressionType.GZIP,
        file_extension=".json",
        strip_outer_array=True,
        allow_duplicate=False,
    )

    sql = options.to_sql()
    assert "TYPE = JSON" in sql
    assert "COMPRESSION = GZIP" in sql
    assert "FILE_EXTENSION = '.json'" in sql
    assert "STRIP_OUTER_ARRAY = TRUE" in sql
    assert "ALLOW_DUPLICATE = FALSE" in sql


def test_multiple_format_creation():
    formats = [
        FileFormat.builder("csv_format")
        .with_create_or_replace()
        .with_options(
            CsvOptions.builder()
            .with_compression(CompressionType.GZIP)
            .with_field_delimiter(",")
            .build()
        )
        .build(),
        FileFormat.builder("json_format")
        .with_create_or_replace()
        .with_options(
            JsonOptions.builder()
            .with_compression(CompressionType.AUTO)
            .with_file_extension(".json")
            .build()
        )
        .build(),
        FileFormat.builder("parquet_format")
        .with_create_or_replace()
        .with_options(
            ParquetOptions.builder().with_compression(CompressionType.SNAPPY).build()
        )
        .build(),
    ]

    for format in formats:
        sql = format.to_sql()
        assert sql.startswith("CREATE OR REPLACE FILE FORMAT")
        assert format.name in sql
