from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

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
    """Configuration for Snowflake connection."""

    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    session_parameters: Dict[str, Any] = field(default_factory=dict)


class Forge:
    """
    Orchestrates Snowflake workflows by managing connections and executing operations.
    """

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn: Optional[SnowflakeConnection] = None

    @contextmanager
    def connection(self):
        """Creates and manages a Snowflake connection."""
        try:
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
            yield self._conn
        except SnowflakeError as e:
            logger.error(f"Snowflake connection error: {e}")
            raise
        finally:
            if self._conn:
                self._conn.close()
                self._conn = None

    @contextmanager
    def transaction(self):
        """Manages a transaction block."""
        with self.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Transaction failed: {e}")
                raise

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        Executes a SQL statement and returns results.

        Args:
            sql: The SQL statement to execute

        Returns:
            List[Dict[str, Any]]: The query results as a list of dictionaries

        Raises:
            SnowflakeError: If there's an error executing the SQL
        """
        with self.connection() as conn:
            try:
                cursor = conn.cursor(snowflake.connector.DictCursor)
                cursor.execute(sql)
                results = cursor.fetchall()
                return [dict(row) for row in results]
            except SnowflakeError as e:
                logger.error(f"SQL execution error: {e}\nSQL: {sql}")
                raise

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
        sql = file_format.sql_statement
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
        self.execute_sql(sql)

    def copy_into(self, copy: CopyInto) -> None:
        """Executes a COPY INTO command."""
        sql = copy.to_sql()
        logger.info(f"Copying data from {copy.source.name} to {copy.target.name}")
        self.execute_sql(sql)

    def workflow(self) -> WorkflowBuilder:
        """Creates a new workflow builder."""
        return WorkflowBuilder(self)


class WorkflowBuilder:
    """Builds and executes Snowflake workflows."""

    def __init__(self, forge: Forge):
        self.forge = forge
        self.steps: List[WorkflowStep] = []

    def create_table(self, table: Table) -> WorkflowBuilder:
        """Adds a table creation step."""
        self.steps.append(WorkflowStep("create_table", table))
        return self

    def create_stage(self, stage: Stage) -> WorkflowBuilder:
        """Adds a stage creation step."""
        self.steps.append(WorkflowStep("create_stage", stage))
        return self

    def create_file_format(self, file_format: FileFormat) -> WorkflowBuilder:
        """Adds a file format creation step."""
        self.steps.append(WorkflowStep("create_file_format", file_format))
        return self

    def create_stream(self, stream: Stream) -> WorkflowBuilder:
        """Adds a stream creation step."""
        self.steps.append(WorkflowStep("create_stream", stream))
        return self

    def create_task(self, task: Task) -> WorkflowBuilder:
        """Adds a task creation step."""
        self.steps.append(WorkflowStep("create_task", task))
        return self

    def put_file(self, put: Put) -> WorkflowBuilder:
        """Adds a PUT command step."""
        self.steps.append(WorkflowStep("put_file", put))
        return self

    def copy_into(self, copy: CopyInto) -> WorkflowBuilder:
        """Adds a COPY INTO command step."""
        self.steps.append(WorkflowStep("copy_into", copy))
        return self

    def execute(self) -> None:
        """Executes all workflow steps in a transaction."""
        with self.forge.transaction() as conn:
            for step in self.steps:
                logger.info(f"Executing workflow step: {step.step_type}")
                method = getattr(self.forge, step.step_type)
                method(step.object)


@dataclass
class WorkflowStep:
    """Represents a single step in a workflow."""

    step_type: str
    object: Union[Table, Stage, FileFormat, Stream, Task, Put, CopyInto]
