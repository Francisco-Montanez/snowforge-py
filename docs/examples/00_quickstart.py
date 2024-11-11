from pathlib import Path

from src.copy_into import CopyInto, CopyIntoSource, CopyIntoTarget
from src.file_format import CsvOptions, FileFormat, FileFormatSpecification
from src.forge import Forge, SnowflakeConfig
from src.put import InternalStage, Put
from src.stage import InternalStageParams, Stage
from src.table import Column, ColumnType, Table

# 1. Configure Snowflake connection
config = SnowflakeConfig(
    account="your_account",
    user="your_user",
    password="your_password",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    role="your_role",
    session_parameters={"QUERY_TAG": "quickstart_example"},
)

# 2. Create Forge instance
forge = Forge(config)

# 3. Define table structure
customers_table = (
    Table.builder()
    .with_name("customers")
    .add_column(Column("id", ColumnType.NUMBER, nullable=False, identity=True))
    .add_column(Column("name", ColumnType.STRING))
    .add_column(Column("email", ColumnType.STRING))
    .add_column(
        Column("created_at", ColumnType.TIMESTAMP, default="CURRENT_TIMESTAMP()")
    )
    .build()
)

# 4. Create stage with file format
stage = (
    Stage.builder()
    .with_name("customer_stage")
    .with_stage_params(InternalStageParams())
    .with_file_format(
        FileFormatSpecification.inline(
            FileFormat.builder("csv_format")
            .with_options(
                CsvOptions(field_delimiter=",", skip_header=1, null_if=["", "NULL"])
            )
            .build()
        )
    )
    .build()
)

# 5. Define data loading operation
put_command = (
    Put.builder()
    .with_file_path(Path("./data/customers.csv"))
    .with_stage(InternalStage.named("customer_stage"))
    .with_auto_compress(True)
    .build()
)

copy_command = (
    CopyInto.builder()
    .with_target(CopyIntoTarget.table("customers"))
    .with_source(CopyIntoSource.stage("customer_stage"))
    .build()
)

# 6. Execute complete workflow
forge.workflow().create_table(customers_table).create_stage(stage).put_file(
    put_command
).copy_into(copy_command).execute()
