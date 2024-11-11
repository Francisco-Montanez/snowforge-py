from pathlib import Path

from src.file_format import Compression
from src.forge import Forge, SnowflakeConfig
from src.put import InternalStage, Put


def demonstrate_put_operations(forge: Forge):
    """Demonstrates various PUT command operations."""

    # Basic PUT operation
    basic_put = (
        Put.builder()
        .with_file_path(Path("./data/simple.csv"))
        .with_stage(InternalStage.named("my_stage"))
        .build()
    )

    # PUT with parallel loading and compression
    parallel_put = (
        Put.builder()
        .with_file_path(Path("./data/large_file.csv"))
        .with_stage(InternalStage.named("my_stage"))
        .with_parallel(4)
        .with_auto_compress(True)
        .with_source_compression(Compression.GZIP)
        .build()
    )

    # PUT to table stage
    table_put = (
        Put.builder()
        .with_file_path(Path("./data/table_data.csv"))
        .with_stage(InternalStage.table("my_table"))
        .with_overwrite(True)
        .build()
    )

    # PUT to user stage
    user_put = (
        Put.builder()
        .with_file_path(Path("./data/user_data.csv"))
        .with_stage(InternalStage.user("my_files"))
        .build()
    )

    # Execute all PUT operations in a single workflow
    forge.workflow().put_file(basic_put).put_file(parallel_put).put_file(
        table_put
    ).put_file(user_put).execute()
