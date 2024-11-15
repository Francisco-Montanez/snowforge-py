from snowforge.forge import Forge, SnowflakeConfig

# use manual config
manual_config = SnowflakeConfig(
    account="account",
    user="user",
    password="password",
)

# or use environment variables
env_config = SnowflakeConfig.from_env()

# use the config to create a connection
with Forge(env_config) as forge:
    forge.workflow().use_database(
        "OFFICIAL_TEST_DB", create_if_not_exists=True
    ).use_schema("OFFICIAL_TEST_SCHEMA", create_if_not_exists=True)
