from snowforge.copy_into import (
    CopyInto,
    CopyIntoOptions,
    CopyIntoSource,
    CopyIntoTarget,
)
from snowforge.forge import Forge, SnowflakeConfig

# Create COPY INTO for users table
users_copy = (
    CopyInto.builder()
    .with_source(CopyIntoSource.stage("USER_DATA_STAGE"))
    .with_target(CopyIntoTarget.table("USERS"))
    .with_options(
        CopyIntoOptions.builder()
        .with_pattern(".*users[.]csv")
        .with_file_format("CSV_FORMAT")
        .with_match_by_column_name("CASE_INSENSITIVE")
        .with_on_error("SKIP_FILE")
        .with_purge(True)
        .build()
    )
    .build()
)

# Create COPY INTO for products table
products_copy = (
    CopyInto.builder()
    .with_source(CopyIntoSource.stage("PRODUCT_DATA_STAGE"))
    .with_target(CopyIntoTarget.table("PRODUCTS"))
    .with_options(
        CopyIntoOptions.builder()
        .with_pattern(".*products[.]csv")
        .with_file_format("CSV_FORMAT")
        .with_match_by_column_name("CASE_INSENSITIVE")
        .with_on_error("SKIP_FILE")
        .with_purge(True)
        .build()
    )
    .build()
)

# Execute the workflow
with Forge(SnowflakeConfig.from_env()) as forge:
    forge.workflow().use_database("OFFICIAL_TEST_DB").use_schema(
        "OFFICIAL_TEST_SCHEMA"
    ).copy_into(users_copy).copy_into(products_copy).execute()
