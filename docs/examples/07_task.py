from snowforge.forge import Forge, SnowflakeConfig
from snowforge.table import Column, ColumnType, Table
from snowforge.task import Schedule, Task, TaskType, WarehouseSize

# Task to process changes from users stream
users_changes_task = (
    Task.builder("PROCESS_USERS_CHANGES")
    .with_create_or_replace()
    .with_warehouse_size(WarehouseSize.XSMALL)
    .with_schedule(Schedule(cron_expr="0 */1 * * *", timezone="UTC"))  # Run every hour
    .with_task_type(TaskType.SQL)
    .with_sql_statement(
        """
        MERGE INTO OFFICIAL_TEST_DB.OFFICIAL_TEST_SCHEMA.USERS_HISTORY T
        USING (
            SELECT
                user_id,
                email,
                first_name,
                last_name,
                created_at,
                updated_at,
                METADATA$ACTION as operation,
                METADATA$ISUPDATE as is_update,
                METADATA$ROW_ID as row_id
            FROM OFFICIAL_TEST_DB.OFFICIAL_TEST_SCHEMA.USERS_CHANGES_STREAM
        ) S
        ON T.row_id = S.row_id
        WHEN MATCHED AND S.operation = 'DELETE' THEN DELETE
        WHEN MATCHED AND S.is_update THEN UPDATE SET
            T.email = S.email,
            T.first_name = S.first_name,
            T.last_name = S.last_name,
            T.updated_at = S.updated_at
        WHEN NOT MATCHED AND S.operation = 'INSERT' THEN INSERT (
            user_id, email, first_name, last_name, created_at, updated_at, row_id
        ) VALUES (
            S.user_id, S.email, S.first_name, S.last_name, S.created_at, S.updated_at, S.row_id
        )
    """
    )
    .with_comment("Task to process changes from users stream into history table")
    .with_tags({"department": "user_management", "data_sensitivity": "high"})
    .build()
)

# Task to process changes from products stream
products_changes_task = (
    Task.builder("PROCESS_PRODUCTS_CHANGES")
    .with_create_or_replace()
    .with_warehouse_size(WarehouseSize.XSMALL)
    .with_schedule(Schedule(cron_expr="*/30 * * * *", timezone="UTC"))
    .with_task_type(TaskType.SQL)
    .with_sql_statement(
        """
        MERGE INTO OFFICIAL_TEST_DB.OFFICIAL_TEST_SCHEMA.PRODUCTS_HISTORY T
        USING (
            SELECT
                product_id,
                sku,
                name,
                description,
                price,
                category,
                created_at,
                METADATA$ACTION as operation,
                METADATA$ISUPDATE as is_update,
                METADATA$ROW_ID as row_id
            FROM OFFICIAL_TEST_DB.OFFICIAL_TEST_SCHEMA.PRODUCTS_CHANGES_STREAM
        ) S
        ON T.row_id = S.row_id
        WHEN MATCHED AND S.operation = 'DELETE' THEN DELETE
        WHEN MATCHED AND S.is_update THEN UPDATE SET
            T.sku = S.sku,
            T.name = S.name,
            T.description = S.description,
            T.price = S.price,
            T.category = S.category
        WHEN NOT MATCHED AND S.operation = 'INSERT' THEN INSERT (
            product_id, sku, name, description, price, category, created_at, row_id
        ) VALUES (
            S.product_id, S.sku, S.name, S.description, S.price, S.category, S.created_at, S.row_id
        )
    """
    )
    .with_comment("Task to process changes from products stream into history table")
    .with_tags({"department": "product_management", "data_sensitivity": "medium"})
    .build()
)

# First create the history tables
users_history_table = (
    Table.builder("USERS_HISTORY")
    .with_create_if_not_exists()
    .with_column(Column("user_id", ColumnType.NUMBER))
    .with_column(Column("email", ColumnType.STRING(255)))
    .with_column(Column("first_name", ColumnType.STRING(50)))
    .with_column(Column("last_name", ColumnType.STRING(50)))
    .with_column(Column("created_at", ColumnType.TIMESTAMP))
    .with_column(Column("updated_at", ColumnType.TIMESTAMP))
    .with_column(Column("row_id", ColumnType.STRING))
    .with_comment("Historical tracking of user changes")
    .with_tag("department", "user_management")
    .with_tag("data_sensitivity", "high")
    .build()
)

products_history_table = (
    Table.builder("PRODUCTS_HISTORY")
    .with_create_if_not_exists()
    .with_column(Column("product_id", ColumnType.NUMBER))
    .with_column(Column("sku", ColumnType.STRING(50)))
    .with_column(Column("name", ColumnType.STRING(200)))
    .with_column(Column("description", ColumnType.TEXT))
    .with_column(Column("price", ColumnType.NUMBER(10, 2)))
    .with_column(Column("category", ColumnType.STRING(50)))
    .with_column(Column("created_at", ColumnType.TIMESTAMP))
    .with_column(Column("row_id", ColumnType.STRING))
    .with_comment("Historical tracking of product changes")
    .with_tag("department", "product_management")
    .with_tag("data_sensitivity", "medium")
    .build()
)

# Execute the workflow
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
    ).add_tables(
        [users_history_table, products_history_table]
    ).add_tasks(
        [users_changes_task, products_changes_task]
    ).execute()
