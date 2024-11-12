from datetime import datetime

from snowforge import Forge
from snowforge.task import Schedule, Task, WarehouseSize


def create_data_processing_tasks(forge: Forge):
    """Creates a set of data processing tasks."""

    # Simple SQL task
    cleanup_task = (
        Task.builder()
        .with_name("cleanup_staging")
        .with_warehouse("COMPUTE_WH")
        .with_warehouse_size(WarehouseSize.XSMALL)
        .with_schedule(Schedule(cron_expr="0 0 * * *", timezone="America/New_York"))
        .with_sql_statement(
            """
            DELETE FROM staging.events
            WHERE event_date < DATEADD(days, -7, CURRENT_DATE())
        """
        )
        .build()
    )

    # Stored procedure task
    etl_task = (
        Task.builder()
        .with_name("run_daily_etl")
        .with_warehouse("TRANSFORM_WH")
        .with_warehouse_size(WarehouseSize.LARGE)
        .with_schedule(Schedule(cron_expr="0 2 * * *", timezone="America/New_York"))
        # .with_stored_procedure("CALL etl.process_daily_data()")
        .with_sql_statement("CALL etl.process_daily_data()")
        .with_session_parameters(
            {"TIMEZONE": "America/New_York", "QUERY_TAG": "daily_etl"}
        )
        .build()
    )

    # Multi-statement task
    reporting_task = (
        Task.builder()
        .with_name("generate_reports")
        .with_warehouse("REPORT_WH")
        # .with_multi_statement(
        #     [
        #         "CALL reporting.refresh_marts()",
        #         "CALL reporting.generate_dashboards()",
        #         "CALL notification.send_completion_alert()",
        #     ]
        # )
        .with_error_integration("error_notification")
        .with_comment("Daily reporting refresh task")
        .build()
    )

    # Execute task creation
    forge.workflow().create_task(cleanup_task).create_task(etl_task).create_task(
        reporting_task
    ).execute()
