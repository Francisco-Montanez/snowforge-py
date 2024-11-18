from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

from snowforge.utilities import sql_quote_comment

from .copy_into import CopyInto
from .file_format import FileFormat
from .put import Put
from .stage import Stage
from .stream import Stream
from .table import Table
from .task import Task

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConfig:
    """
    Configuration for Snowflake connection.

    Required Parameters:
        account: Snowflake account identifier
        user: Snowflake username
        password: Snowflake password

    Optional Parameters:
        warehouse: Snowflake warehouse name
        database: Snowflake database name
        schema: Snowflake schema name
        role: Snowflake role name
        session_parameters: Additional session parameters
    """

    account: str
    user: str
    password: str
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    role: Optional[str] = None
    session_parameters: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        env_path: Optional[Union[str, Path]] = None,
        env_prefix: str = "SNOWFLAKE_",
        raise_if_missing: bool = True,
    ) -> SnowflakeConfig:
        """
        Create a SnowflakeConfig instance from environment variables.

        Args:
            env_path: Optional path to .env file. If None, looks for .env in current directory
            env_prefix: Prefix for environment variables (default: "SNOWFLAKE_")
            raise_if_missing: Whether to raise an error if required variables are missing

        Environment variables:
            Required:
                SNOWFLAKE_ACCOUNT: Snowflake account identifier
                SNOWFLAKE_USER: Snowflake username
                SNOWFLAKE_PASSWORD: Snowflake password

            Optional:
                SNOWFLAKE_WAREHOUSE: Snowflake warehouse name
                SNOWFLAKE_DATABASE: Snowflake database name
                SNOWFLAKE_SCHEMA: Snowflake schema name
                SNOWFLAKE_ROLE: Snowflake role name
                SNOWFLAKE_SESSION_PARAMETERS: JSON string of session parameters

        Returns:
            SnowflakeConfig: Configuration instance

        Raises:
            ValueError: If required environment variables are missing and raise_if_missing is True
        """
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()

        required_vars = ["ACCOUNT", "USER", "PASSWORD"]
        optional_vars = ["WAREHOUSE", "DATABASE", "SCHEMA", "ROLE"]

        config_dict = {}
        missing_vars = []

        for var in required_vars:
            env_var = f"{env_prefix}{var}"
            value = os.getenv(env_var)
            if value is None:
                missing_vars.append(env_var)
            config_dict[var.lower()] = value

        if missing_vars and raise_if_missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        # Add optional variables
        for var in optional_vars:
            env_var = f"{env_prefix}{var}"
            value = os.getenv(env_var)
            if value is not None:
                config_dict[var.lower()] = value

        # Session parameters (optional JSON string)
        session_params = os.getenv(f"{env_prefix}SESSION_PARAMETERS", "{}")
        if session_params:
            try:
                import json

                config_dict["session_parameters"] = json.loads(session_params)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Failed to parse {env_prefix}SESSION_PARAMETERS as JSON: {e}"
                )
                config_dict["session_parameters"] = {}

        return cls(**config_dict)


class TransactionManager:
    """Manages Snowflake transaction boundaries and retries."""

    def __init__(self, connection: SnowflakeConnection, max_retries: int = 3):
        self.connection = connection
        self.max_retries = max_retries
        self._transaction_level = 0

    @contextmanager
    def transaction(
        self, readonly: bool = False
    ) -> Generator[SnowflakeConnection, None, None]:
        """Manage transaction boundaries with retry logic."""
        retry_count = 0
        while True:
            try:
                if self._transaction_level == 0:
                    self.connection.cursor().execute("BEGIN TRANSACTION")
                self._transaction_level += 1

                yield self.connection

                if self._transaction_level == 1:
                    self.connection.cursor().execute("COMMIT")
                self._transaction_level -= 1
                break

            except snowflake.connector.errors.ProgrammingError as e:
                if self._transaction_level > 0:
                    self._transaction_level -= 1
                if retry_count < self.max_retries and self._is_retryable(e):
                    retry_count += 1
                    continue
                raise
            except Exception:
                if self._transaction_level > 0:
                    try:
                        self.connection.cursor().execute("ROLLBACK")
                    except Exception:
                        pass
                    self._transaction_level = 0
                raise

    def _is_retryable(self, error: Exception) -> bool:
        """Determine if an error is retryable."""
        retryable_codes = {
            250001,  # Connection reset
            250002,  # Connection closed
            90100,  # Network error
        }
        return (
            isinstance(error, snowflake.connector.errors.ProgrammingError)
            and error.errno in retryable_codes
        )


class Forge:
    """Snowflake workflow orchestrator with proper session management."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn: Optional[SnowflakeConnection] = None
        self._session_id: Optional[int] = None

    @contextmanager
    def get_connection(self) -> Generator[SnowflakeConnection, None, None]:
        """Get or create a Snowflake connection."""
        if not self._conn:
            self._conn = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
                role=self.config.role,
                session_parameters=self.config.session_parameters,
            )
            self._session_id = self._conn.session_id

        try:
            yield self._conn
        except Exception:
            self._cleanup()
            raise

    def _cleanup(self) -> None:
        """Properly cleanup Snowflake session and connection."""
        if self._conn and self._session_id is not None:
            try:
                cursor = self._conn.cursor()
                cursor.execute(
                    "SELECT SYSTEM$ABORT_SESSION(%s)", (str(self._session_id),)
                )
                cursor.close()
            except Exception:
                logger.warning("Failed to abort session", exc_info=True)
            finally:
                try:
                    self._conn.close()
                except Exception:
                    logger.warning("Failed to close connection", exc_info=True)
                self._conn = None
                self._session_id = None

    def __enter__(self) -> 'Forge':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._cleanup()

    @contextmanager
    def transaction(self) -> Generator[SnowflakeConnection, None, None]:
        """Execute operations in a transaction."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN")
                yield conn
                cursor.execute("COMMIT")
            except Exception:
                try:
                    cursor.execute("ROLLBACK")
                except Exception:
                    logger.error("Failed to rollback transaction", exc_info=True)
                raise
            finally:
                cursor.close()

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Executes a SQL statement with proper resource management."""
        cursor = None
        try:
            with self.transaction() as conn:
                cursor = conn.cursor(snowflake.connector.DictCursor)
                cursor.execute(sql)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except SnowflakeError as e:
            logger.error(f"Snowflake error executing SQL: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing SQL: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    logger.error("Failed to close cursor", exc_info=True)

    def create_table(self, table: Table) -> None:
        """Creates a table in Snowflake."""
        sql = table.to_sql()
        logger.info(f"Creating table: {table.name}")
        self.execute_sql(sql)

    def create_stage(self, stage: Stage) -> None:
        """Creates a stage in Snowflake."""
        sql = stage.to_sql()
        logger.info(f"Creating stage: {stage.name}")
        self.execute_sql(sql)

    def create_file_format(self, file_format: FileFormat) -> None:
        """Creates a file format in Snowflake."""
        sql = file_format.to_sql()
        logger.info(f"Creating file format: {file_format.name}")
        self.execute_sql(sql)

    def create_stream(self, stream: Stream) -> None:
        """Creates a stream in Snowflake."""
        sql = stream.to_sql()
        logger.info(f"Creating stream: {stream.name}")
        self.execute_sql(sql)

    def create_task(self, task: Task) -> None:
        """Creates a task in Snowflake."""
        sql = task.to_sql()
        logger.info(f"Creating task: {task.name}")
        self.execute_sql(sql)

    def put_file(self, put: Put) -> None:
        """Executes a PUT command to stage files."""
        sql = put.to_sql()
        logger.info(f"Putting file: {put.file_path}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()

    def copy_into(self, copy: CopyInto) -> None:
        """Executes a COPY INTO command."""
        sql = copy.to_sql()
        logger.info(f"Copying data from {copy.source.name} to {copy.target.name}")
        self.execute_sql(sql)

    def workflow(self) -> WorkflowBuilder:
        """Creates a new workflow builder."""
        return WorkflowBuilder(self)

    def create_database(self, sql: str) -> None:
        """Executes a CREATE DATABASE statement."""
        logger.info(f"Executing: {sql}")
        self.execute_sql(sql)

    def use_database(self, sql: str) -> None:
        """Executes a USE DATABASE statement."""
        logger.info(f"Executing: {sql}")
        self.execute_sql(sql)

    def create_schema(self, sql: str) -> None:
        """Executes a CREATE SCHEMA statement."""
        logger.info(f"Executing: {sql}")
        self.execute_sql(sql)

    def use_schema(self, sql: str) -> None:
        """Executes a USE SCHEMA statement."""
        logger.info(f"Executing: {sql}")
        self.execute_sql(sql)

    def add_tag(self, sql: str) -> None:
        """Executes tag-related SQL statements."""
        logger.info(f"Executing: {sql}")
        for statement in sql.split(';'):
            if statement.strip():
                self.execute_sql(statement.strip() + ';')

    def custom_sql(self, sql: str) -> None:
        """Executes a custom SQL statement."""
        logger.info(f"Executing SQL: {sql}")
        self.execute_sql(sql)


class WorkflowBuilder:
    """Builds and executes Snowflake workflows."""

    def __init__(self, forge: Forge):
        self.forge = forge
        self.steps: List[WorkflowStep] = []  # Single list for all steps
        self._current_transaction_steps: List[WorkflowStep] = []

    def _execute_transaction_steps(self) -> None:
        """Execute accumulated transaction steps."""
        if self._current_transaction_steps:
            with self.forge.transaction() as conn:
                for step in self._current_transaction_steps:
                    logger.info(f"Executing workflow step: {step.step_type}")
                    method = getattr(self.forge, step.step_type)
                    method(step.object)
            self._current_transaction_steps.clear()

    def use_database(
        self,
        database: str,
        create_if_not_exists: bool = False,
        create_or_replace: bool = False,
    ) -> WorkflowBuilder:
        """Sets the database for the workflow."""
        if create_if_not_exists:
            self._add_transaction_step(
                WorkflowStep(
                    "create_database", f"CREATE DATABASE IF NOT EXISTS {database};"
                )
            )
        elif create_or_replace:
            self._add_transaction_step(
                WorkflowStep(
                    "create_database", f"CREATE OR REPLACE DATABASE {database};"
                )
            )
        self._add_transaction_step(
            WorkflowStep("use_database", f"USE DATABASE {database};")
        )
        return self

    def use_schema(
        self,
        schema: str,
        create_if_not_exists: bool = False,
        create_or_replace: bool = False,
    ) -> WorkflowBuilder:
        """Sets the schema for the workflow."""
        if create_if_not_exists:
            self._add_transaction_step(
                WorkflowStep("create_schema", f"CREATE SCHEMA IF NOT EXISTS {schema};")
            )
        elif create_or_replace:
            self._add_transaction_step(
                WorkflowStep("create_schema", f"CREATE OR REPLACE SCHEMA {schema};")
            )
        self._add_transaction_step(WorkflowStep("use_schema", f"USE SCHEMA {schema};"))
        return self

    def _add_transaction_step(self, step: WorkflowStep) -> None:
        """Add a step that should be executed within a transaction."""
        self._current_transaction_steps.append(step)
        self.steps.append(step)

    def add_table(self, table: Table) -> WorkflowBuilder:
        """Adds a table creation step."""
        self._add_transaction_step(WorkflowStep("create_table", table))
        return self

    def add_tables(self, tables: List[Table]) -> WorkflowBuilder:
        """Adds a list of table creation steps."""
        for table in tables:
            self.add_table(table)
        return self

    def add_stage(self, stage: Stage) -> WorkflowBuilder:
        """Adds a stage creation step."""
        self._add_transaction_step(WorkflowStep("create_stage", stage))
        return self

    def add_stages(self, stages: List[Stage]) -> WorkflowBuilder:
        """Adds a list of stage creation steps."""
        for stage in stages:
            self.add_stage(stage)
        return self

    def add_file_format(self, file_format: FileFormat) -> WorkflowBuilder:
        """Adds a file format creation step."""
        self._add_transaction_step(WorkflowStep("create_file_format", file_format))
        return self

    def add_file_formats(self, file_formats: List[FileFormat]) -> WorkflowBuilder:
        """Adds a list of file format creation steps."""
        for file_format in file_formats:
            self.add_file_format(file_format)
        return self

    def add_stream(self, stream: Stream) -> WorkflowBuilder:
        """Adds a stream creation step."""
        self._add_transaction_step(WorkflowStep("create_stream", stream))
        return self

    def add_streams(self, streams: List[Stream]) -> WorkflowBuilder:
        """Adds a list of stream creation steps."""
        for stream in streams:
            self.add_stream(stream)
        return self

    def add_task(self, task: Task) -> WorkflowBuilder:
        """Adds a task creation step."""
        self._add_transaction_step(WorkflowStep("create_task", task))
        return self

    def add_tasks(self, tasks: List[Task]) -> WorkflowBuilder:
        """Adds a list of task creation steps."""
        for task in tasks:
            self.add_task(task)
        return self

    def put_file(self, put: Put) -> WorkflowBuilder:
        """Adds a PUT command step."""
        # Execute any pending transaction steps before PUT
        self._execute_transaction_steps()

        # Execute PUT operation immediately
        with self.forge.get_connection() as conn:
            cursor = conn.cursor()
            try:
                logger.info(f"Executing PUT operation: {put.file_path}")
                sql = put.to_sql()
                cursor.execute(sql)
            finally:
                cursor.close()

        self.steps.append(WorkflowStep("put_file", put))
        return self

    def copy_into(self, copy: CopyInto) -> WorkflowBuilder:
        """Adds a COPY INTO command step."""
        self._add_transaction_step(WorkflowStep("copy_into", copy))
        return self

    def add_custom_sql(self, sql: str) -> WorkflowBuilder:
        """Adds a custom SQL step."""
        self._add_transaction_step(WorkflowStep("custom_sql", sql))
        return self

    def add_tag(
        self,
        name: str,
        allowed_values: Optional[List[str]] = None,
        comment: Optional[str] = None,
        replace: bool = False,
    ) -> WorkflowBuilder:
        """Creates a tag following Snowflake syntax."""
        if replace:
            sql_parts = ["CREATE OR REPLACE TAG"]
        else:
            sql_parts = ["CREATE TAG IF NOT EXISTS"]

        sql_parts.append(f'{name}')

        if allowed_values:
            values_str = ", ".join(f"'{val}'" for val in allowed_values)
            sql_parts.append(f"ALLOWED_VALUES {values_str}")

        if comment:
            sql_parts.append(f"COMMENT = {sql_quote_comment(comment)}")

        sql = " ".join(sql_parts)
        self._add_transaction_step(WorkflowStep("custom_sql", sql))

        return self

    def execute(self) -> None:
        """Execute any remaining transaction steps."""
        self._execute_transaction_steps()


@dataclass
class WorkflowStep:
    """Represents a single step in a workflow."""

    step_type: str
    object: Union[Table, Stage, FileFormat, Stream, Task, Put, CopyInto, str]
