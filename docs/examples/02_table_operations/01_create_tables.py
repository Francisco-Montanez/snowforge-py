from snowforge import Forge, SnowflakeConfig
from snowforge.table import Column, ColumnType, Table, TableType


# Create a complete data model for an e-commerce system
def create_ecommerce_schema(forge: Forge):
    # 1. Create Users table
    users_table = (
        Table.builder()
        .with_name("users")
        .add_column(
            Column(
                "user_id",
                ColumnType.NUMBER,
                nullable=False,
                identity=True,
                primary_key=True,
            )
        )
        .add_column(
            Column("email", ColumnType.STRING(255), nullable=False, unique=True)
        )
        .add_column(Column("password_hash", ColumnType.STRING(64), nullable=False))
        .add_column(Column("first_name", ColumnType.STRING(50)))
        .add_column(Column("last_name", ColumnType.STRING(50)))
        .add_column(
            Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
        )
        .add_column(
            Column("updated_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
        )
        .with_comment("User accounts and authentication information")
        .with_tag("department", "user_management")
        .with_tag("security_level", "high")
        .build()
    )

    # 2. Create Products table with clustering
    products_table = (
        Table.builder()
        .with_name("products")
        .add_column(
            Column("product_id", ColumnType.NUMBER, nullable=False, identity=True)
        )
        .add_column(Column("sku", ColumnType.STRING(50), nullable=False))
        .add_column(Column("name", ColumnType.STRING(200), nullable=False))
        .add_column(Column("description", ColumnType.TEXT))
        .add_column(Column("price", ColumnType.NUMBER(10, 2), nullable=False))
        .add_column(Column("category", ColumnType.STRING(50)))
        .add_column(
            Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
        )
        .with_cluster_by(["category", "created_at"])
        .with_table_type(TableType.TRANSIENT)
        .build()
    )

    # Execute as a single transaction
    forge.workflow().create_table(users_table).create_table(products_table).execute()
