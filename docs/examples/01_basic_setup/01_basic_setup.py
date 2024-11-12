from snowforge import Forge, SnowflakeConfig

# Load configuration
config = SnowflakeConfig.from_env()


with Forge(config) as forge:
    try:
        # Simple SQL execution
        results = forge.execute_sql(
            "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        print("Current session details:", results)

        # Using transaction context
        with forge.transaction() as conn:
            forge.execute_sql("CREATE SCHEMA IF NOT EXISTS example_schema")
            forge.execute_sql("USE SCHEMA example_schema")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
