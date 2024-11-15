from snowforge.forge import Forge, SnowflakeConfig
from snowforge.stream import Stream, StreamMode, StreamType

# Create a stream to track changes in the users table
users_stream = (
    Stream.builder("USERS_CHANGES_STREAM")
    .with_create_or_replace()
    .with_source("OFFICIAL_TEST_DB.OFFICIAL_TEST_SCHEMA.USERS")
    .with_append_only(False)
    .with_show_initial_rows(False)
    .with_comment("Stream to capture changes in the users table")
    .with_tags({"department": "user_management", "data_sensitivity": "high"})
    .build()
)

# Create a stream to track changes in the products table
products_stream = (
    Stream.builder("PRODUCTS_CHANGES_STREAM")
    .with_create_or_replace()
    .with_source("OFFICIAL_TEST_DB.OFFICIAL_TEST_SCHEMA.PRODUCTS")
    .with_append_only(False)
    .with_show_initial_rows(True)
    .with_comment("Stream to capture changes in the products table")
    .with_tags({"department": "product_management", "data_sensitivity": "medium"})
    .build()
)

# Execute the workflow to create the streams
with Forge(SnowflakeConfig.from_env()) as forge:
    forge.workflow().use_database("OFFICIAL_TEST_DB").use_schema(
        "OFFICIAL_TEST_SCHEMA"
    ).add_tag(
        "department",
        allowed_values=["user_management", "product_management"],
        comment="Department responsible for the data",
        replace=True,
    ).add_tag(
        "data_sensitivity",
        allowed_values=["high", "medium", "low"],
        comment="Data sensitivity level",
        replace=True,
    ).add_streams(
        [users_stream, products_stream]
    ).execute()
