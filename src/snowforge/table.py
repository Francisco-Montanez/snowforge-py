from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union


class TableType(str, Enum):
    """Represents different types of tables."""

    PERMANENT = "PERMANENT"
    TEMPORARY = "TEMPORARY"
    TRANSIENT = "TRANSIENT"
    VOLATILE = "VOLATILE"


class ColumnType(str, Enum):
    """Represents different column data types."""

    NUMBER = "NUMBER"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    VARIANT = "VARIANT"
    ARRAY = "ARRAY"
    OBJECT = "OBJECT"
    TEXT = "TEXT"

    def __call__(self, *args: Union[int, str]) -> str:
        """
        Allows parameterized column types like STRING(255) or NUMBER(10,2).

        Args:
            *args: Variable length arguments for type parameters
                  (e.g., length for STRING, precision and scale for NUMBER)

        Returns:
            str: Formatted column type with parameters
        """
        if not args:
            return str(self.value)

        params = ",".join(str(arg) for arg in args)
        return f"{self.value}({params})"

    def __str__(self) -> str:
        return self.value


@dataclass
class Column:
    """Represents a table column definition."""

    name: str
    data_type: Union[
        ColumnType, str
    ]  # Can be either ColumnType or parameterized string
    nullable: bool = True
    default: Optional[str] = None
    identity: bool = False
    primary_key: bool = False
    unique: bool = False
    foreign_key: Optional[str] = None
    comment: Optional[str] = None
    collate: Optional[str] = None

    def to_sql(self) -> str:
        parts = [self.name]

        # Handle both plain ColumnType and parameterized types
        if isinstance(self.data_type, ColumnType):
            parts.append(str(self.data_type))
        else:
            parts.append(self.data_type)

        if not self.nullable:
            parts.append("NOT NULL")
        if self.default:
            parts.append(f"DEFAULT {self.default}")
        if self.identity:
            parts.append("IDENTITY")
        if self.primary_key:
            parts.append("PRIMARY KEY")
        if self.unique:
            parts.append("UNIQUE")
        if self.foreign_key:
            parts.append(f"REFERENCES {self.foreign_key}")
        if self.comment:
            parts.append(f"COMMENT '{self.comment.replace(chr(39), chr(39)*2)}'")
        if self.collate:
            parts.append(f"COLLATE '{self.collate}'")

        return " ".join(parts)


@dataclass
class RowAccessPolicy:
    """Represents a row access policy."""

    name: str
    on: List[str]


@dataclass
class AggregationPolicy:
    """Represents an aggregation policy."""

    name: str
    on: List[str]


@dataclass
class Table:
    """
    Represents a Snowflake table configuration.

    A Table encapsulates all the properties and configurations needed to define
    a table structure, including columns, constraints, and various options.
    """

    name: str
    columns: List[Column]
    table_type: TableType = TableType.PERMANENT
    cluster_by: Optional[List[str]] = None
    stage_file_format: Optional[str] = None
    stage_copy_options: Optional[str] = None
    data_retention_time_in_days: Optional[int] = None
    max_data_extension_time_in_days: Optional[int] = None
    change_tracking: Optional[bool] = None
    default_ddl_collation: Optional[str] = None
    copy_grants: bool = False
    comment: Optional[str] = None
    row_access_policy: Optional[RowAccessPolicy] = None
    aggregation_policy: Optional[AggregationPolicy] = None
    tags: Dict[str, str] = field(default_factory=dict)
    is_create_or_replace: bool = False
    is_create_if_not_exists: bool = False

    @classmethod
    def builder(cls, name: str) -> TableBuilder:
        """Creates a new TableBuilder instance."""
        return TableBuilder(name=name)

    def to_sql(self) -> str:
        """Generates the SQL statement for the table."""
        parts = []

        if self.is_create_or_replace:
            parts.append("CREATE OR REPLACE")
        else:
            parts.append("CREATE")

        if self.table_type != TableType.PERMANENT:
            parts.append(self.table_type.value)

        parts.append("TABLE")

        if self.is_create_if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.name)

        # Add columns
        column_definitions = [col.to_sql() for col in self.columns]
        parts.append(f"({', '.join(column_definitions)})")

        if self.comment:
            parts.append(f"COMMENT = '{self.comment.replace(chr(39), chr(39)*2)}'")

        if self.data_retention_time_in_days is not None:
            parts.append(
                f"DATA_RETENTION_TIME_IN_DAYS = {self.data_retention_time_in_days}"
            )

        if self.max_data_extension_time_in_days is not None:
            parts.append(
                f"MAX_DATA_EXTENSION_TIME_IN_DAYS = {self.max_data_extension_time_in_days}"
            )

        if self.change_tracking is not None:
            parts.append(f"CHANGE_TRACKING = {str(self.change_tracking).upper()}")

        if self.default_ddl_collation:
            parts.append(f"DEFAULT_DDL_COLLATION = '{self.default_ddl_collation}'")

        if self.copy_grants:
            parts.append("COPY GRANTS")

        if self.cluster_by:
            parts.append(f"CLUSTER BY ({', '.join(self.cluster_by)})")

        if self.row_access_policy:
            policy = self.row_access_policy
            parts.append(
                f"WITH ROW ACCESS POLICY {policy.name} ON ({', '.join(policy.on)})"
            )

        if self.aggregation_policy:
            policy = self.aggregation_policy
            parts.append(
                f"WITH AGGREGATION POLICY {policy.name} ON ({', '.join(policy.on)})"
            )

        if self.tags:
            tag_parts = [f"TAG ({key} = '{value}')" for key, value in self.tags.items()]
            parts.append("WITH " + " ".join(tag_parts))

        return " ".join(parts)


@dataclass
class TableBuilder:
    """Builder for Table instances."""

    name: Optional[str] = None
    columns: List[Column] = field(default_factory=list)
    table_type: TableType = TableType.PERMANENT
    cluster_by: Optional[List[str]] = None
    stage_file_format: Optional[str] = None
    stage_copy_options: Optional[str] = None
    data_retention_time_in_days: Optional[int] = None
    max_data_extension_time_in_days: Optional[int] = None
    change_tracking: Optional[bool] = None
    default_ddl_collation: Optional[str] = None
    copy_grants: bool = False
    comment: Optional[str] = None
    row_access_policy: Optional[RowAccessPolicy] = None
    aggregation_policy: Optional[AggregationPolicy] = None
    tags: Dict[str, str] = field(default_factory=dict)
    is_create_or_replace: bool = False
    is_create_if_not_exists: bool = False

    def with_column(self, column: Column) -> TableBuilder:
        """Adds a column to the table."""
        self.columns.append(column)
        return self

    def with_table_type(self, table_type: TableType) -> TableBuilder:
        """Sets the table type."""
        self.table_type = table_type
        return self

    def with_cluster_by(self, columns: List[str]) -> TableBuilder:
        """Sets the clustering columns."""
        self.cluster_by = columns
        return self

    def with_stage_file_format(self, format: str) -> TableBuilder:
        """Sets the stage file format."""
        self.stage_file_format = format
        return self

    def with_stage_copy_options(self, options: str) -> TableBuilder:
        """Sets the stage copy options."""
        self.stage_copy_options = options
        return self

    def with_data_retention_time_in_days(self, days: int) -> TableBuilder:
        """Sets the data retention time in days."""
        self.data_retention_time_in_days = days
        return self

    def with_max_data_extension_time_in_days(self, days: int) -> TableBuilder:
        """Sets the maximum data extension time in days."""
        self.max_data_extension_time_in_days = days
        return self

    def with_change_tracking(self, enabled: bool) -> TableBuilder:
        """Sets whether change tracking is enabled."""
        self.change_tracking = enabled
        return self

    def with_default_ddl_collation(self, collation: str) -> TableBuilder:
        """Sets the default DDL collation."""
        self.default_ddl_collation = collation
        return self

    def with_copy_grants(self, copy_grants: bool) -> TableBuilder:
        """Sets whether to copy grants."""
        self.copy_grants = copy_grants
        return self

    def with_comment(self, comment: str) -> TableBuilder:
        """Sets the table comment."""
        self.comment = comment
        return self

    def with_row_access_policy(self, policy: RowAccessPolicy) -> TableBuilder:
        """Sets the row access policy."""
        self.row_access_policy = policy
        return self

    def with_aggregation_policy(self, policy: AggregationPolicy) -> TableBuilder:
        """Sets the aggregation policy."""
        self.aggregation_policy = policy
        return self

    def with_tag(self, key: str, value: str) -> TableBuilder:
        """Adds a tag to the table."""
        self.tags[key] = value
        return self

    def with_create_or_replace(self) -> TableBuilder:
        """Sets the table to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> TableBuilder:
        """Sets the table to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def build(self) -> Table:
        """Builds and returns a new Table instance."""
        if not self.name:
            raise ValueError("Table name must be set")
        if not self.columns:
            raise ValueError("Table must have at least one column")

        return Table(
            name=self.name,
            columns=self.columns,
            table_type=self.table_type,
            cluster_by=self.cluster_by,
            stage_file_format=self.stage_file_format,
            stage_copy_options=self.stage_copy_options,
            data_retention_time_in_days=self.data_retention_time_in_days,
            max_data_extension_time_in_days=self.max_data_extension_time_in_days,
            change_tracking=self.change_tracking,
            default_ddl_collation=self.default_ddl_collation,
            copy_grants=self.copy_grants,
            comment=self.comment,
            row_access_policy=self.row_access_policy,
            aggregation_policy=self.aggregation_policy,
            tags=self.tags,
            is_create_or_replace=self.is_create_or_replace,
            is_create_if_not_exists=self.is_create_if_not_exists,
        )
