from snowforge.forge import Forge, SnowflakeConfig
from snowforge.table import Column, ColumnType, Table, TableType

users_table = (
    Table.builder("users")
    .with_create_or_replace()
    .with_column(
        Column(
            "user_id",
            ColumnType.NUMBER,
            nullable=False,
            identity=True,
            primary_key=True,
        )
    )
    .with_column(Column("email", ColumnType.STRING(255), nullable=False, unique=True))
    .with_column(Column("password_hash", ColumnType.STRING(64), nullable=False))
    .with_column(Column("first_name", ColumnType.STRING(50)))
    .with_column(Column("last_name", ColumnType.STRING(50)))
    .with_column(
        Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
    )
    .with_column(
        Column("updated_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
    )
    .with_comment("User accounts and authentication information")
    .with_tag("department", "user_management")
    .with_tag("security_level", "high")
    .build()
)

# 2. Create Products table with clustering
products_table = (
    Table.builder("products")
    .with_create_or_replace()
    .with_column(Column("product_id", ColumnType.NUMBER, nullable=False, identity=True))
    .with_column(Column("sku", ColumnType.STRING(50), nullable=False))
    .with_column(Column("name", ColumnType.STRING(200), nullable=False))
    .with_column(Column("description", ColumnType.TEXT))
    .with_column(Column("price", ColumnType.NUMBER(10, 2), nullable=False))
    .with_column(Column("category", ColumnType.STRING(50)))
    .with_column(
        Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
    )
    .with_cluster_by(["category", "created_at"])
    .with_table_type(TableType.TRANSIENT)
    .build()
)


with Forge(SnowflakeConfig.from_env()) as forge:
    forge.workflow().use_database("OFFICIAL_TEST_DB").use_schema(
        "OFFICIAL_TEST_SCHEMA"
    ).add_tag(
        "department",
        allowed_values=["user_management"],
        comment="Department of the user",
    ).add_tag(
        "security_level",
        allowed_values=["high", "medium", "low"],
        comment="Security level of the user",
    ).add_tables(
        [users_table, products_table]
    ).execute()
