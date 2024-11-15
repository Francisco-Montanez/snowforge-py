from snowforge import Forge, SnowflakeConfig
from snowforge.file_format import FileFormatSpecification
from snowforge.stage import (
    InternalDirectoryTableParams,
    InternalStageEncryptionType,
    InternalStageParams,
    Stage,
)

internal_stage = (
    Stage.builder("TEST_INTERNAL_STAGE")
    .with_create_if_not_exists()
    .with_stage_params(InternalStageParams(encryption=InternalStageEncryptionType.SSE))
    .with_directory_table_params(
        InternalDirectoryTableParams(enable=True, refresh_on_create=True)
    )
    .with_file_format(
        FileFormatSpecification.named(
            "CSV_FORMAT",
        )
    )
    .with_comment("Internal stage for testing")
    .build()
)

with Forge(SnowflakeConfig.from_env()) as forge:
    forge.workflow().use_database(
        "OFFICIAL_TEST_DB", create_if_not_exists=True
    ).use_schema("OFFICIAL_TEST_SCHEMA", create_if_not_exists=True).add_stage(
        internal_stage
    ).execute()
