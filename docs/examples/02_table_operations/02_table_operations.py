from snowforge import Forge, SnowflakeConfig
from snowforge.table import Column, ColumnType, Table, TableType

# Create table definition
customers_table = (
    Table.builder()
    .with_name("customers")
    .create_if_not_exists()
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
config = SnowflakeConfig.from_env()

# Create Forge instance and execute
forge = Forge(config)
forge.execute_sql("CREATE TAG IF NOT EXISTS DEPARTMENT")
forge.execute_sql("CREATE TAG IF NOT EXISTS OWNER")
forge.create_table(customers_table)

# Create a table with clustering
orders_table = (
    Table.builder()
    .with_name("orders")
    .create_if_not_exists()
    .add_column(Column("order_id", ColumnType.NUMBER, nullable=False, identity=True))
    .add_column(Column("customer_id", ColumnType.NUMBER, foreign_key="customers(id)"))
    .add_column(Column("order_date", ColumnType.DATE))
    .add_column(Column("amount", ColumnType.NUMBER))
    .with_cluster_by(["order_date"])
    .with_data_retention_time_in_days(90)
    .build()
)

forge.create_table(orders_table)
