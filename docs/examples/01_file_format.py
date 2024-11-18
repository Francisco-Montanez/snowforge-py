from snowforge.file_format import (
    AvroOptions,
    CompressionType,
    CsvOptions,
    FileFormat,
    JsonOptions,
    OrcOptions,
    ParquetOptions,
    XmlOptions,
)
from snowforge.forge import Forge, SnowflakeConfig

avro_format = (
    FileFormat.builder("avro_format")
    .with_create_or_replace()
    .with_options(
        AvroOptions(
            compression=CompressionType.AUTO,
            trim_space=True,
            null_if=["NULL", ""],
            replace_invalid_characters=True,
        )
    )
    .build()
)

# 1.2 Create CSV File Format
csv_format = (
    FileFormat.builder("csv_format")
    .with_create_or_replace()
    .with_options(
        CsvOptions(
            compression=CompressionType.NONE,
            trim_space=True,
            field_delimiter=",",
            file_extension=".csv",
            parse_header=True,
            error_on_column_count_mismatch=False,
            null_if=["NULL", ""],
        )
    )
    .build()
)

csv_format_with_escape = (
    FileFormat.builder("csv_format_with_escape")
    .with_create_or_replace()
    .with_options(
        CsvOptions(
            compression=CompressionType.NONE,
            trim_space=True,
            field_delimiter="\t",
            file_extension=".csv",
            parse_header=True,
            error_on_column_count_mismatch=False,
            null_if=["NULL", ""],
            escape="\\",
            escape_unenclosed_field="\\",
            field_optionally_enclosed_by='"',
        )
    )
    .build()
)

# 1.3 Create JSON File Format
json_format = (
    FileFormat.builder("json_format")
    .with_create_or_replace()
    .with_options(
        JsonOptions(
            compression=CompressionType.AUTO,
            file_extension=".json",
            allow_duplicate=False,
            replace_invalid_characters=True,
            null_if=["NULL", ""],
        )
    )
    .build()
)

# 1.4 Create ORC File Format
orc_format = (
    FileFormat.builder("orc_format")
    .with_create_or_replace()
    .with_options(
        OrcOptions(
            trim_space=True,
            replace_invalid_characters=True,
            null_if=["NULL", ""],
        )
    )
    .build()
)

# 1.5 Create Parquet File Format
parquet_format = (
    FileFormat.builder("parquet_format")
    .with_create_or_replace()
    .with_options(
        ParquetOptions(
            compression=CompressionType.SNAPPY,
            binary_as_text=False,
            trim_space=True,
            null_if=["NULL", ""],
        )
    )
    .with_comment("Parquet file format for data ingestion")
    .build()
)

# 1.6 Create XML File Format
xml_format = (
    FileFormat.builder("xml_format")
    .with_create_or_replace()
    .with_options(
        XmlOptions(
            compression=CompressionType.AUTO,
            replace_invalid_characters=True,
        )
    )
    .build()
)

with Forge(SnowflakeConfig.from_env()) as forge:
    forge.workflow().use_database(
        "OFFICIAL_TEST_DB", create_if_not_exists=True
    ).use_schema("OFFICIAL_TEST_SCHEMA", create_if_not_exists=True).add_file_formats(
        [
            avro_format,
            csv_format,
            csv_format_with_escape,
            json_format,
            orc_format,
            parquet_format,
            xml_format,
        ]
    ).execute()
