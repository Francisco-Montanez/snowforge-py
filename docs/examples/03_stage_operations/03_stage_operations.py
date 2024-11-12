from snowforge import Forge, SnowflakeConfig
from snowforge.file_format import CsvOptions, FileFormat, FileFormatSpecification
from snowforge.stage import InternalStageParams, S3ExternalStageParams, Stage

# Create internal stage
internal_stage = (
    Stage.builder()
    .with_name("my_internal_stage")
    .with_stage_params(InternalStageParams(encryption={"TYPE": "SNOWFLAKE_SSE"}))
    .with_file_format(
        FileFormatSpecification.inline(
            FileFormat.builder("my_csv_format")
            .with_options(
                CsvOptions(field_delimiter=",", skip_header=1, null_if=["NULL", ""])
            )
            .build()
        )
    )
    .create_if_not_exists()
    .build()
)

# Create S3 external stage
s3_stage = (
    Stage.builder()
    .with_name("my_s3_stage")
    .with_stage_params(
        S3ExternalStageParams(
            url="s3://my-bucket/path/", storage_integration="my_storage_integration"
        )
    )
    .temporary()
    .build()
)

# Basic connection setup
config = SnowflakeConfig.from_env()
# Execute using Forge
with Forge(config) as forge:
    forge.workflow().create_stage(internal_stage).execute()
