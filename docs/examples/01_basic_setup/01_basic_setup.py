from snowforge import Forge, SnowflakeConfig

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

config = SnowflakeConfig.from_env()

# Create Forge instance
forge = Forge(config)

# Simple SQL execution
results = forge.execute_sql(
    "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
)

print("Current session details:", results)

# Using transaction context
with forge.transaction() as conn:
    # Multiple operations in a single transaction
    forge.execute_sql("CREATE SCHEMA IF NOT EXISTS example_schema")
    forge.execute_sql("USE SCHEMA example_schema")
