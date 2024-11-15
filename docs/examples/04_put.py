import csv
import os
from datetime import datetime, timezone

from snowforge.file_format import CompressionType, FileFormatSpecification
from snowforge.forge import Forge, SnowflakeConfig
from snowforge.put import InternalStage, Put
from snowforge.stage import (
    InternalDirectoryTableParams,
    InternalStageEncryptionType,
    InternalStageParams,
    Stage,
)

# Create sample data directory if it doesn't exist
sample_data_dir = "sample_data"
os.makedirs(sample_data_dir, exist_ok=True)

# Generate sample users data
users_data = [
    ["email", "password_hash", "first_name", "last_name", "created_at", "updated_at"],
    [
        "john.doe@example.com",
        "e10adc3949ba59abbe56e057f20f883e",
        "John",
        "Doe",
        datetime.now(timezone.utc).isoformat(),
        datetime.now(timezone.utc).isoformat(),
    ],
    [
        "jane.smith@example.com",
        "f30aa7a662c728b7407c54ae6bfd27d1",
        "Jane",
        "Smith",
        datetime.now(timezone.utc).isoformat(),
        datetime.now(timezone.utc).isoformat(),
    ],
]

# Generate sample products data
products_data = [
    ["sku", "name", "description", "price", "category", "created_at"],
    [
        "TECH-001",
        "Laptop Pro X",
        "High-performance laptop with 16GB RAM",
        "999.99",
        "Electronics",
        datetime.now(timezone.utc).isoformat(),
    ],
    [
        "TECH-002",
        "Wireless Mouse",
        "Ergonomic wireless mouse",
        "29.99",
        "Electronics",
        datetime.now(timezone.utc).isoformat(),
    ],
    [
        "HOME-001",
        "Coffee Maker",
        "12-cup programmable coffee maker",
        "79.99",
        "Home Appliances",
        datetime.now(timezone.utc).isoformat(),
    ],
]

# Write sample data to CSV files
users_file = os.path.join(sample_data_dir, "users.csv")
products_file = os.path.join(sample_data_dir, "products.csv")

# with open(users_file, "w", newline="") as f:
#     writer = csv.writer(f)
#     writer.writerows(users_data)

# with open(products_file, "w", newline="") as f:
#     writer = csv.writer(f)
#     writer.writerows(products_data)


product_data_stage = (
    Stage.builder("PRODUCT_DATA_STAGE")
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
    .with_comment("Product data stage")
    .build()
)

user_data_stage = (
    Stage.builder("USER_DATA_STAGE")
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
    .with_comment("User data stage")
    .build()
)


# Modified example usage
with Forge(SnowflakeConfig.from_env()) as forge:
    # First execute the database and schema operations
    forge.workflow().use_database("OFFICIAL_TEST_DB").use_schema(
        "OFFICIAL_TEST_SCHEMA"
    ).add_stages([product_data_stage, user_data_stage]).execute()

    # Then execute the PUT operations separately
    forge.put_file(
        Put.builder()
        .with_file_path(users_file)
        .with_auto_compress(False)
        .with_overwrite(True)
        .with_source_compression(CompressionType.NONE)
        .with_stage(InternalStage("named", "USER_DATA_STAGE"))
        .build()
    )

    forge.put_file(
        Put.builder()
        .with_file_path(products_file)
        .with_auto_compress(False)
        .with_overwrite(True)
        .with_source_compression(CompressionType.NONE)
        .with_stage(InternalStage("named", "PRODUCT_DATA_STAGE"))
        .build()
    )
