from src.forge import Forge, SnowflakeConfig
from src.table import Column, ColumnType, Table, TableType

# Create table definition
table = (
    Table.builder()
    .with_name("customers")
    .add_column(Column("id", ColumnType.NUMBER, nullable=False, identity=True))
    .add_column(Column("name", ColumnType.STRING, nullable=False))
    .add_column(Column("email", ColumnType.STRING, unique=True))
    .add_column(
        Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
    )
    .with_table_type(TableType.TRANSIENT)
    .with_comment("Customer information table")
    .with_tag("department", "sales")
    .with_tag("owner", "data_team")
    .build()
)

# Basic connection setup
config = SnowflakeConfig(
    account="your_account",
    user="your_user",
    password="your_password",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    role="your_role",
    session_parameters={"QUERY_TAG": "example_workflow", "TIMEZONE": "UTC"},
)

# Create Forge instance and execute
forge = Forge(config)
forge.create_table(table)

# Create a table with clustering
orders_table = (
    Table.builder()
    .with_name("orders")
    .add_column(Column("order_id", ColumnType.NUMBER, nullable=False, identity=True))
    .add_column(Column("customer_id", ColumnType.NUMBER, foreign_key="customers(id)"))
    .add_column(Column("order_date", ColumnType.DATE))
    .add_column(Column("amount", ColumnType.NUMBER))
    .with_cluster_by(["order_date"])
    .with_data_retention_time_in_days(90)
    .build()
)

forge.create_table(orders_table)
