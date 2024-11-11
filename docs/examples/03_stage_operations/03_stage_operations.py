from src.file_format import CsvOptions, FileFormat, FileFormatSpecification
from src.forge import Forge, SnowflakeConfig
from src.stage import InternalStageParams, S3ExternalStageParams, Stage

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
config = SnowflakeConfig(
    account="your_account",
    user="your_user",
    password="your_password",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    role="your_role",
    session_parameters={"QUERY_TAG": "example_workflow", "TIMEZONE": "UTC"},
)
# Execute using Forge
forge = Forge(config)
forge.workflow().create_stage(internal_stage).create_stage(s3_stage).execute()
