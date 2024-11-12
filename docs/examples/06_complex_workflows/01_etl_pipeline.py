from pathlib import Path

from snowforge.copy_into import CopyInto, CopyIntoSource, CopyIntoTarget
from snowforge.file_format import FileFormat, FileFormatSpecification, ParquetOptions
from snowforge.forge import Forge, SnowflakeConfig
from snowforge.stage import S3ExternalStageParams, Stage
from snowforge.table import Column, ColumnType, Table
from snowforge.task import Schedule, Task, TaskType, WarehouseSize


def create_etl_pipeline(forge: Forge):
    """
    Creates a complete ETL pipeline that:
    1. Sets up landing and transformed tables
    2. Creates external stage for data ingestion
    3. Configures file format for Parquet files
    4. Sets up tasks for data processing
    """

    # 1. Create Landing Table
    landing_table = (
        Table.builder()
        .with_name("raw_events")
        .add_column(Column("event_id", ColumnType.STRING))
        .add_column(Column("event_timestamp", ColumnType.TIMESTAMP))
        .add_column(Column("event_type", ColumnType.STRING))
        .add_column(Column("payload", ColumnType.VARIANT))
        .add_column(
            Column(
                "ingestion_timestamp",
                ColumnType.TIMESTAMP,
                default="CURRENT_TIMESTAMP()",
            )
        )
        .with_comment("Raw events landing table")
        .build()
    )

    # 2. Create Processed Table
    processed_table = (
        Table.builder()
        .with_name("processed_events")
        .add_column(Column("event_id", ColumnType.STRING))
        .add_column(Column("event_timestamp", ColumnType.TIMESTAMP))
        .add_column(Column("event_type", ColumnType.STRING))
        .add_column(Column("processed_payload", ColumnType.OBJECT))
        .add_column(
            Column(
                "processing_timestamp",
                ColumnType.TIMESTAMP,
                default="CURRENT_TIMESTAMP()",
            )
        )
        .with_cluster_by(["event_type", "event_timestamp"])
        .build()
    )

    # 3. Configure External Stage
    stage = (
        Stage.builder()
        .with_name("event_intake")
        .with_stage_params(
            S3ExternalStageParams(
                url="s3://my-event-bucket/incoming/",
                storage_integration="event_s3_integration",
                encryption={"TYPE": "AWS_SSE_KMS"},
            )
        )
        .with_file_format(
            FileFormatSpecification.inline(
                FileFormat.builder("parquet_format")
                .with_options(ParquetOptions(binary_as_text=False, trim_space=True))
                .build()
            )
        )
        .build()
    )

    # 4. Create Data Loading Task
    load_task = (
        Task.builder()
        .with_name("load_raw_events")
        .with_warehouse("LOAD_WH")
        .with_warehouse_size(WarehouseSize.LARGE)
        .with_schedule(Schedule(cron_expr="*/10 * * * *", timezone="America/New_York"))
        .with_sql_statement(
            """
            COPY INTO raw_events
            FROM @event_intake
            FILE_FORMAT = parquet_format
        """
        )
        .build()
    )

    # 5. Create Copy Into Task
    copy_into_task = (
        CopyInto.builder()
        .with_source(
            CopyIntoSource.table(processed_table.name)
            # .with_file_format(
            #     FileFormatSpecification.inline(
            #         FileFormat.builder("parquet_format")
            #         .with_options(ParquetOptions(binary_as_text=False, trim_space=True))
            #         .build()
            #     )
            # )
            # .build()
        )
        .with_target(CopyIntoTarget.table(processed_table.name))
        .build()
    )

    # 6. Create Task Sequence
    task_sequence = (
        Task.builder()
        .with_name("etl_pipeline")
        .with_task_type(TaskType.SQL)
        # .add_task(load_task)
        # .add_task(copy_into_task)
        .build()
    )

    # 7. Add Task Sequence to Forge
    forge.create_task(task_sequence)
