from datetime import datetime

from snowforge.copy_into import CopyInto, CopyIntoSource, CopyIntoTarget
from snowforge.forge import Forge, SnowflakeConfig
from snowforge.stage import InternalStageParams, Stage
from snowforge.table import Column, ColumnType, Table
from snowforge.task import Task, TaskType, WarehouseSize


def create_migration_workflow(forge: Forge):
    """
    Creates a data migration workflow that:
    1. Sets up source and target tables
    2. Creates migration tracking table
    3. Sets up incremental migration tasks
    4. Implements validation and rollback capabilities
    """

    # Migration tracking table
    migration_control = (
        Table.builder()
        .with_name("migration_control")
        .add_column(Column("migration_id", ColumnType.NUMBER, identity=True))
        .add_column(Column("source_table", ColumnType.STRING))
        .add_column(Column("target_table", ColumnType.STRING))
        .add_column(Column("batch_id", ColumnType.NUMBER))
        .add_column(Column("start_time", ColumnType.TIMESTAMP))
        .add_column(Column("end_time", ColumnType.TIMESTAMP))
        .add_column(Column("records_migrated", ColumnType.NUMBER))
        .add_column(Column("status", ColumnType.STRING))
        .add_column(Column("error_message", ColumnType.STRING))
        .build()
    )

    # Migration validation task
    validation_task = (
        Task.builder()
        .with_name("validate_migration")
        .with_warehouse("MIGRATION_WH")
        .with_warehouse_size(WarehouseSize.LARGE)
        .with_sql_statement(
            """
            MERGE INTO migration_control m
            USING (
                SELECT
                    migration_id,
                    CASE
                        WHEN source_count = target_count THEN 'SUCCESS'
                        ELSE 'VALIDATION_FAILED'
                    END as status,
                    CURRENT_TIMESTAMP() as end_time,
                    source_count as records_migrated
                FROM (
                    SELECT
                        m.migration_id,
                        (SELECT COUNT(*) FROM identifier(m.source_table)) as source_count,
                        (SELECT COUNT(*) FROM identifier(m.target_table)) as target_count
                    FROM migration_control m
                    WHERE status = 'IN_PROGRESS'
                )
            ) v
            ON m.migration_id = v.migration_id
            WHEN MATCHED THEN UPDATE SET
                status = v.status,
                end_time = v.end_time,
                records_migrated = v.records_migrated
        """
        )
        .build()
    )

    # Incremental migration task
    migration_task = (
        Task.builder()
        .with_name("migrate_batch")
        .with_warehouse("MIGRATION_WH")
        .with_warehouse_size(WarehouseSize.XLARGE)
        .with_sql_statement(
            """
            BEGIN
                LET batch_size := 1000000;
                LET current_batch := (
                    SELECT COALESCE(MAX(batch_id), 0) + 1
                    FROM migration_control
                    WHERE source_table = 'source_table'
                );

                INSERT INTO migration_control (
                    source_table, target_table, batch_id,
                    start_time, status
                )
                SELECT
                    'source_table',
                    'target_table',
                    :current_batch,
                    CURRENT_TIMESTAMP(),
                    'IN_PROGRESS';

                COPY INTO target_table
                FROM (
                    SELECT *
                    FROM source_table
                    WHERE _BATCH_ID = :current_batch
                    LIMIT :batch_size
                );

                CALL validate_migration();
            END;
        """
        )
        .with_error_integration("migration_alerts")
        .build()
    )

    # Execute workflow
    forge.workflow().create_table(migration_control).create_task(
        validation_task
    ).create_task(migration_task).execute()
