from snowforge.copy_into import CopyInto, CopyIntoSource, CopyIntoTarget
from snowforge.file_format import FileFormat, ParquetOptions
from snowforge.forge import Forge


def demonstrate_copy_operations(forge: Forge):
    """Demonstrates various COPY INTO operations."""

    # Basic COPY from stage to table
    basic_copy = (
        CopyInto.builder()
        .with_target(CopyIntoTarget.table("customers"))
        .with_source(CopyIntoSource.stage("customer_stage"))
        .build()
    )

    # COPY with column mapping
    mapped_copy = (
        CopyInto.builder()
        .with_target(CopyIntoTarget.table("orders"))
        .with_source(CopyIntoSource.stage("order_stage"))
        # .with_columns(["order_id", "customer_id", "amount"])
        # .with_file_format(
        #     FileFormat.builder("parquet_format")
        #     # .with_type("PARQUET")
        #     .with_options(ParquetOptions(binary_as_text=True)).build()
        # )
        .build()
    )

    # COPY with transformations
    transform_copy = (
        CopyInto.builder()
        .with_target(CopyIntoTarget.table("transactions"))
        .with_source(CopyIntoSource.stage("transaction_stage"))
        # .with_transformations(
        #     [
        #         "TO_TIMESTAMP($1, 'YYYY-MM-DD HH24:MI:SS')",
        #         "$2",
        #         "CAST($3 AS NUMBER(10,2))",
        #     ]
        # )
        .build()
    )

    # Execute all COPY operations
    forge.workflow().copy_into(basic_copy).copy_into(mapped_copy).copy_into(
        transform_copy
    ).execute()
