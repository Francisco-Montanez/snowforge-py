# snowforge-py

Simplify the creation and management of Snowflake objects and data pipeline processes.

**Note: This is an alpha release. The API is subject to change.**

## Features

- **Database Operations**
  - Table creation and management with comprehensive column types
  - Internal and external stage management
  - File format handling (CSV, JSON, Avro, Parquet, ORC, XML)
  - Stream creation and monitoring
  - Task scheduling and management

- **Data Loading**
  - PUT operations with compression support
  - COPY INTO operations with extensive options
  - Pattern matching and file format specifications

- **Workflow Orchestration**
  - Transaction management
  - Multi-step workflow support
  - Database and schema management
  - Tag management and application


## Installation


```bash
pip install snowforge
```


## Quick Start

```python
from snowforge import (
    Forge, SnowflakeConfig,
    FileFormat, Stage, Table, Stream, Task,
    CopyInto, CopyIntoSource, CopyIntoTarget,
    TaskType, WarehouseSize
)
from snowforge.copy_into import OnError
from snowforge.file_format import CsvOptions, CompressionType, FileFormatSpecification
from snowforge.stage import InternalStageParams, InternalStageEncryptionType
from snowforge.table import Column, ColumnType
from snowforge.task import Schedule

# 1. Create file format for CSV files
csv_format = (
    FileFormat.builder("SALES_CSV_FORMAT")
    .with_create_or_replace()
    .with_options(
        CsvOptions(
            compression=CompressionType.AUTO,
            field_delimiter=",",
            skip_header=1,
            null_if=["NULL", ""],
        )
    )
    .with_comment("CSV format for sales data")
    .build()
)

# 2. Create internal stage for data loading
sales_stage = (
    Stage.builder("SALES_STAGE")
    .with_create_or_replace()
    .with_stage_params(
        InternalStageParams(encryption=InternalStageEncryptionType.SSE)
    )
    .with_file_format(FileFormatSpecification.named("SALES_CSV_FORMAT"))
    .with_comment("Stage for sales data files")
    .build()
)

# 3. Create sales table
sales_table = (
    Table.builder("SALES")
    .with_create_or_replace()
    .with_column(Column("sale_id", ColumnType.NUMBER, nullable=False, identity=True))
    .with_column(Column("product_id", ColumnType.NUMBER, nullable=False))
    .with_column(Column("customer_id", ColumnType.NUMBER, nullable=False))
    .with_column(Column("quantity", ColumnType.NUMBER))
    .with_column(Column("amount", ColumnType.NUMBER(15, 2)))
    .with_column(Column("sale_date", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()"))
    .with_cluster_by(["sale_date"])
    .with_comment("Sales transactions table")
    .build()
)

# 4. Create stream to track changes
sales_stream = (
    Stream.builder("SALES_CHANGES")
    .with_create_or_replace()
    .with_source("SALES")
    .with_append_only(False)
    .with_comment("Stream to capture changes in sales table")
    .build()
)

# 5. Create task to process changes
sales_analytics_task = (
    Task.builder("PROCESS_SALES_CHANGES")
    .with_create_or_replace()
    .with_warehouse_size(WarehouseSize.XSMALL)
    .with_schedule(Schedule(cron_expr="0 */1 * * *", timezone="UTC"))
    .with_task_type(TaskType.SQL)
    .with_sql_statement("""
        MERGE INTO SALES_ANALYTICS T USING (
            SELECT
                DATE_TRUNC('day', sale_date) as sale_day,
                SUM(quantity) as total_quantity,
                SUM(amount) as total_amount,
                COUNT(*) as transaction_count
            FROM SALES_CHANGES
            GROUP BY DATE_TRUNC('day', sale_date)
        ) S
        ON T.sale_day = S.sale_day
        WHEN MATCHED THEN UPDATE SET
            T.total_quantity = T.total_quantity + S.total_quantity,
            T.total_amount = T.total_amount + S.total_amount,
            T.transaction_count = T.transaction_count + S.transaction_count
        WHEN NOT MATCHED THEN INSERT (
            sale_day, total_quantity, total_amount, transaction_count
        ) VALUES (
            S.sale_day, S.total_quantity, S.total_amount, S.transaction_count
        )
    """)
    .with_comment("Task to aggregate sales data hourly")
    .build()
)

# 6. Execute the complete workflow
with Forge(SnowflakeConfig.from_env()) as forge:
    (forge.workflow()
        .use_database("SALES_DB", create_if_not_exists=True)
        .use_schema("SALES_SCHEMA", create_if_not_exists=True)
        .add_file_format(csv_format)
        .add_stage(sales_stage)
        .add_table(sales_table)
        .add_stream(sales_stream)
        .add_task(sales_analytics_task)
        .execute()
    )

# 7. Load data using COPY INTO
copy_sales = (
    CopyInto.builder()
    .with_source(CopyIntoSource.stage("SALES_STAGE"))
    .with_target(CopyIntoTarget.table("SALES"))
    .with_pattern(".*sales[.]csv")
    .with_file_format(FileFormatSpecification.named("SALES_CSV_FORMAT"))
    .with_on_error(OnError.CONTINUE)
    .build()
)

forge.workflow().copy_into(copy_sales).execute()
```


## Documentation

For detailed documentation and examples, see the [docs](docs/) directory:

1. [Snowflake Connection](docs/examples/00_connection.py)
2. [File Formats](docs/examples/01_file_formats.py)
3. [Stages](docs/examples/02_stages.py)
4. [Tables](docs/examples/03_tables.py)
5. [Put](docs/examples/04_put.py)
6. [Copy Into](docs/examples/05_copy_into.py)
7. [Streams](docs/examples/06_streams.py)
8. [Tasks](docs/examples/07_tasks.py)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the GPL-3.0 License - see the [LICENSE](LICENSE) file for details.