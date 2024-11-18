import pytest

from snowforge.table import (
    AggregationPolicy,
    Column,
    ColumnType,
    RowAccessPolicy,
    Table,
    TableType,
)


@pytest.fixture
def basic_column():
    return Column("test_column", ColumnType.STRING(255))


@pytest.fixture
def complex_column():
    return Column(
        "user_id",
        ColumnType.NUMBER,
        nullable=False,
        identity=True,
        primary_key=True,
        comment="Primary key for users",
    )


@pytest.fixture
def basic_table(basic_column):
    return Table.builder("TEST_TABLE").with_column(basic_column).build()


@pytest.fixture
def complex_table():
    return (
        Table.builder("USERS")
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
        .with_column(
            Column("email", ColumnType.STRING(255), nullable=False, unique=True)
        )
        .with_column(
            Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
        )
        .with_comment("User accounts table")
        .with_tag("department", "hr")
        .with_tag("security_level", "high")
        .with_table_type(TableType.TRANSIENT)
        .with_cluster_by(["email"])
        .build()
    )


def test_column_type_basic():
    """Test basic column type creation."""
    assert str(ColumnType.STRING) == "STRING"
    assert ColumnType.STRING(255) == "STRING(255)"
    assert ColumnType.NUMBER(10, 2) == "NUMBER(10,2)"


def test_column_creation_basic(basic_column):
    """Test basic column creation."""
    assert basic_column.name == "test_column"
    assert basic_column.data_type == "STRING(255)"
    assert basic_column.nullable is True
    expected_sql = "test_column STRING(255)"
    assert basic_column.to_sql() == expected_sql


def test_column_creation_complex(complex_column):
    """Test complex column creation with all options."""
    expected_sql = (
        "user_id NUMBER NOT NULL IDENTITY PRIMARY KEY "
        "COMMENT 'Primary key for users'"
    )
    assert complex_column.to_sql() == expected_sql


def test_column_with_foreign_key():
    """Test column with foreign key reference."""
    column = Column(
        "department_id",
        ColumnType.NUMBER,
        nullable=False,
        foreign_key="departments(id)",
    )
    expected = "department_id NUMBER NOT NULL REFERENCES departments(id)"
    assert column.to_sql() == expected


def test_table_creation_basic(basic_table):
    """Test basic table creation."""
    expected = "CREATE TABLE TEST_TABLE (test_column STRING(255))"
    assert basic_table.to_sql() == expected


def test_table_creation_complex(complex_table):
    """Test complex table creation with all options."""
    expected = (
        "CREATE OR REPLACE TRANSIENT TABLE USERS ("
        "user_id NUMBER NOT NULL IDENTITY PRIMARY KEY, "
        "email STRING(255) NOT NULL UNIQUE, "
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()"
        ") "
        "COMMENT = 'User accounts table' "
        "CLUSTER BY (email) "
        "WITH TAG (department = 'hr', security_level = 'high')"
    )
    assert complex_table.to_sql() == expected


def test_table_builder_validation():
    """Test table builder validation."""
    # Test missing name
    with pytest.raises(ValueError, match="Table name must be set"):
        Table.builder("").build()

    # Test missing columns
    with pytest.raises(ValueError, match="Table must have at least one column"):
        Table.builder("TEST_TABLE").build()


def test_table_with_policies():
    """Test table creation with row access and aggregation policies."""
    table = (
        Table.builder("SALES")
        .with_column(Column("id", ColumnType.NUMBER))
        .with_row_access_policy(RowAccessPolicy("sales_access", ["department_id"]))
        .with_aggregation_policy(AggregationPolicy("sales_agg", ["region_id"]))
        .build()
    )
    expected = (
        "CREATE TABLE SALES (id NUMBER) "
        "WITH ROW ACCESS POLICY sales_access ON ('department_id') "
        "WITH AGGREGATION POLICY sales_agg ON ('region_id')"
    )
    assert table.to_sql() == expected


def test_table_with_data_retention():
    """Test table creation with data retention settings."""
    table = (
        Table.builder("LOGS")
        .with_column(Column("id", ColumnType.NUMBER))
        .with_data_retention_time_in_days(90)
        .with_max_data_extension_time_in_days(180)
        .build()
    )
    expected = (
        "CREATE TABLE LOGS (id NUMBER) "
        "DATA_RETENTION_TIME_IN_DAYS = 90 "
        "MAX_DATA_EXTENSION_TIME_IN_DAYS = 180"
    )
    assert table.to_sql() == expected


def test_table_with_special_characters():
    """Test table creation with special characters in comments and collation."""
    table = (
        Table.builder("TEST_TABLE")
        .with_column(
            Column(
                "name",
                ColumnType.STRING(50),
                comment="User's name with \"quotes\"",
                collate="utf8_unicode_ci",
            )
        )
        .with_comment("Table's comment with \"quotes\"")
        .build()
    )
    expected = (
        "CREATE TABLE TEST_TABLE ("
        "name STRING(50) COMMENT 'User\\'s name with \"quotes\"' "
        "COLLATE 'utf8_unicode_ci'"
        ") COMMENT = 'Table\\'s comment with \"quotes\"'"
    )
    assert table.to_sql() == expected


def test_table_type_enum():
    """Test TableType enum values."""
    assert str(TableType.PERMANENT) == "PERMANENT"
    assert str(TableType.TEMPORARY) == "TEMPORARY"
    assert str(TableType.TRANSIENT) == "TRANSIENT"
    assert str(TableType.VOLATILE) == "VOLATILE"
