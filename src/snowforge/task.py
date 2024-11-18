from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from snowforge.utilities import (
    sql_format_boolean,
    sql_format_dict,
    sql_format_list,
    sql_quote_comment,
    sql_quote_string,
)


class TaskType(Enum):
    """Represents different types of tasks."""

    SQL = "SQL"
    STORED_PROCEDURE = "STORED_PROCEDURE"
    MULTI_STATEMENT = "MULTI_STATEMENT"
    PROCEDURAL_LOGIC = "PROCEDURAL_LOGIC"

    @classmethod
    def sql(cls, statement: str) -> TaskType:
        """Creates a new SQL task type."""
        return cls.SQL

    @classmethod
    def stored_procedure(cls, proc_call: str) -> TaskType:
        """Creates a new stored procedure task type."""
        return cls.STORED_PROCEDURE

    @classmethod
    def multi_statement(cls, statements: List[str]) -> TaskType:
        """Creates a new multi-statement task type."""
        return cls.MULTI_STATEMENT

    @classmethod
    def procedural_logic(cls, code: str) -> TaskType:
        """Creates a new procedural logic task type."""
        return cls.PROCEDURAL_LOGIC

    def __str__(self) -> str:
        """Returns the string representation of the task type."""
        return self.value


class WarehouseSize(str, Enum):
    """Represents different warehouse sizes."""

    XSMALL = "XSMALL"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"
    XXLARGE = "XXLARGE"
    XXXLARGE = "XXXLARGE"
    X4LARGE = "X4LARGE"
    X5LARGE = "X5LARGE"
    X6LARGE = "X6LARGE"

    def __str__(self) -> str:
        return self.value


@dataclass
class Schedule:
    """Represents task schedule configuration.

    Attributes:
        cron_expr: The cron expression for the schedule
        interval_minutes: The interval in minutes for the schedule
        timezone: The timezone for the schedule
    """

    cron_expr: Optional[str] = None
    interval_minutes: Optional[int] = None
    timezone: Optional[str] = None

    def to_sql(self) -> str:
        if self.interval_minutes is not None:
            return f"'{self.interval_minutes} MINUTE'"
        elif self.cron_expr and self.timezone:
            return f"'USING CRON {self.cron_expr} {self.timezone.strip()}'"
        return ""


@dataclass
class Task:
    """
    Represents a task to be executed in the data warehouse environment.

    A Task encapsulates all the properties and configurations needed to define
    and execute a specific operation.

    Attributes:
        name: The name of the task
        task_type: The type of task
        sql_statement: The SQL statement to be executed
        after: The tasks to run after this task
        allow_overlapping_execution: Whether overlapping execution is allowed
        comment: The comment for the task
        config: The configuration for the task
        error_integration: The error integration for the task
        finalize: The finalize script for the task
        is_create_if_not_exists: Whether to create the task if it doesn't exist
        is_create_or_replace: Whether to create or replace the task
        schedule: The schedule for the task
        session_parameters: The session parameters for the task
        suspend_task_after_num_failures: The number of failures before suspension
        tags: The tags for the task
        task_auto_retry_attempts: The number of auto retry attempts
        user_task_minimum_trigger_interval_in_seconds: The minimum trigger interval in seconds
        user_task_timeout_ms: The task timeout in milliseconds
        warehouse: The warehouse for the task
        warehouse_size: The warehouse size for the task
        when: The condition for task execution
    """

    name: str
    task_type: TaskType
    sql_statement: str
    after: Optional[List[str]] = None
    allow_overlapping_execution: Optional[bool] = None
    comment: Optional[str] = None
    config: Optional[str] = None
    error_integration: Optional[str] = None
    finalize: Optional[str] = None
    is_create_if_not_exists: bool = False
    is_create_or_replace: bool = False
    schedule: Optional[Schedule] = None
    session_parameters: Dict[str, str] = field(default_factory=dict)
    suspend_task_after_num_failures: Optional[int] = None
    tags: Dict[str, str] = field(default_factory=dict)
    task_auto_retry_attempts: Optional[int] = None
    user_task_minimum_trigger_interval_in_seconds: Optional[int] = None
    user_task_timeout_ms: Optional[int] = None
    warehouse_size: Optional[WarehouseSize] = None
    warehouse: Optional[str] = None
    when: Optional[str] = None

    @classmethod
    def builder(cls, name: str) -> TaskBuilder:
        """Creates a new TaskBuilder instance."""
        return TaskBuilder(name=name)

    def to_sql(self) -> str:
        """Generates the SQL statement for the task."""
        parts = []

        create_parts = ["CREATE"]
        if self.is_create_or_replace:
            create_parts.append("OR REPLACE")
        create_parts.extend(["TASK", self.name])
        parts.append(" ".join(create_parts))

        if self.tags:
            parts.append(f"WITH TAG {sql_format_dict(self.tags)}")

        if self.after:
            parts.append(f"AFTER {sql_format_list(self.after)}")

        if self.warehouse_size:
            parts.append(
                f"USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = {self.warehouse_size}"
            )
        if self.warehouse:
            parts.append(f"WAREHOUSE = {sql_quote_string(self.warehouse)}")

        if self.schedule:
            parts.append(f"SCHEDULE = {self.schedule.to_sql()}")

        if self.error_integration:
            parts.append(f"ERROR_INTEGRATION = {self.error_integration}")

        if self.suspend_task_after_num_failures is not None:
            parts.append(
                f"SUSPEND_TASK_AFTER_NUM_FAILURES = {self.suspend_task_after_num_failures}"
            )
        if self.task_auto_retry_attempts is not None:
            parts.append(f"TASK_AUTO_RETRY_ATTEMPTS = {self.task_auto_retry_attempts}")

        if self.allow_overlapping_execution is not None:
            parts.append(
                f"ALLOW_OVERLAPPING_EXECUTION = {sql_format_boolean(self.allow_overlapping_execution)}"
            )

        if self.user_task_timeout_ms is not None:
            parts.append(f"USER_TASK_TIMEOUT_MS = {self.user_task_timeout_ms}")
        if self.user_task_minimum_trigger_interval_in_seconds is not None:
            parts.append(
                f"USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = {self.user_task_minimum_trigger_interval_in_seconds}"
            )

        if self.session_parameters:
            param_pairs = [f"{k} = {v}" for k, v in self.session_parameters.items()]
            parts.append(f"SESSION_PARAMETERS = ({', '.join(param_pairs)})")

        if self.comment:
            parts.append(f"COMMENT = {sql_quote_string(self.comment)}")

        if self.when:
            parts.append(f"WHEN {self.when}")

        parts.append("AS")
        parts.append(self.sql_statement)

        return "\n".join(parts)


@dataclass
class TaskBuilder:
    """Builder for Task instances.

    Attributes:
        after: The tasks to run after this task
        allow_overlapping_execution: Whether overlapping execution is allowed
        comment: The comment for the task
        config: The configuration for the task
        error_integration: The error integration for the task
        finalize: The finalize script for the task
        is_create_if_not_exists: Whether to create the task if it doesn't exist
        is_create_or_replace: Whether to create or replace the task
        name: The name of the task
        schedule: The schedule for the task
        session_parameters: The session parameters for the task
        sql_statement: The SQL statement to be executed
        task_auto_retry_attempts: The number of auto retry attempts
        task_type: The type of task
        user_task_minimum_trigger_interval_in_seconds: The minimum trigger interval in seconds
        user_task_timeout_ms: The task timeout in milliseconds
        warehouse: The warehouse for the task
        warehouse_size: The warehouse size for the task
        when: The condition for task execution
    """

    after: Optional[List[str]] = None
    allow_overlapping_execution: Optional[bool] = None
    comment: Optional[str] = None
    config: Optional[str] = None
    error_integration: Optional[str] = None
    finalize: Optional[str] = None
    is_create_if_not_exists: bool = False
    is_create_or_replace: bool = False
    name: Optional[str] = None
    schedule: Optional[Schedule] = None
    session_parameters: Dict[str, str] = field(default_factory=dict)
    sql_statement: Optional[str] = None
    suspend_task_after_num_failures: Optional[int] = None
    tags: Dict[str, str] = field(default_factory=dict)
    task_auto_retry_attempts: Optional[int] = None
    task_type: Optional[TaskType] = None
    user_task_minimum_trigger_interval_in_seconds: Optional[int] = None
    user_task_timeout_ms: Optional[int] = None
    warehouse_size: Optional[WarehouseSize] = None
    warehouse: Optional[str] = None
    when: Optional[str] = None

    def build(self) -> Task:
        """Builds and returns a new Task instance."""
        if not self.name or self.name.strip() == "":
            raise ValueError("Task name must be set")
        if not self.task_type:
            raise ValueError("Task type must be set")
        if not self.sql_statement:
            raise ValueError("SQL statement must be set")

        return Task(
            after=self.after,
            allow_overlapping_execution=self.allow_overlapping_execution,
            comment=self.comment,
            config=self.config,
            error_integration=self.error_integration,
            finalize=self.finalize,
            is_create_if_not_exists=self.is_create_if_not_exists,
            is_create_or_replace=self.is_create_or_replace,
            name=self.name,
            schedule=self.schedule,
            session_parameters=self.session_parameters,
            sql_statement=self.sql_statement,
            suspend_task_after_num_failures=self.suspend_task_after_num_failures,
            tags=self.tags,
            task_auto_retry_attempts=self.task_auto_retry_attempts,
            task_type=self.task_type,
            user_task_minimum_trigger_interval_in_seconds=self.user_task_minimum_trigger_interval_in_seconds,
            user_task_timeout_ms=self.user_task_timeout_ms,
            warehouse_size=self.warehouse_size,
            warehouse=self.warehouse,
            when=self.when,
        )

    def with_after_tasks(self, tasks: List[str]) -> TaskBuilder:
        """Sets the predecessor tasks."""
        self.after = tasks
        return self

    def with_auto_retry_attempts(self, attempts: int) -> TaskBuilder:
        """Sets the number of auto retry attempts."""
        self.task_auto_retry_attempts = attempts
        return self

    def with_comment(self, comment: str) -> TaskBuilder:
        """Sets the task comment."""
        self.comment = comment
        return self

    def with_config(self, config: str) -> TaskBuilder:
        """Sets the configuration for the task."""
        self.config = config
        return self

    def with_create_or_replace(self) -> TaskBuilder:
        """Sets the task to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> TaskBuilder:
        """Sets the task to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def with_error_integration(self, integration: str) -> TaskBuilder:
        """Sets the error integration for the task."""
        self.error_integration = integration
        return self

    def with_finalize(self, finalize: str) -> TaskBuilder:
        """Sets the finalize script for the task."""
        self.finalize = finalize
        return self

    def with_minimum_trigger_interval(self, interval_seconds: int) -> TaskBuilder:
        """Sets the minimum trigger interval in seconds."""
        self.user_task_minimum_trigger_interval_in_seconds = interval_seconds
        return self

    def with_overlapping_execution(self, allow: bool) -> TaskBuilder:
        """Sets whether overlapping execution is allowed."""
        self.allow_overlapping_execution = allow
        return self

    def with_session_parameters(self, params: Dict[str, str]) -> TaskBuilder:
        """Sets the session parameters for the task."""
        self.session_parameters = params
        return self

    def with_schedule(self, schedule: Schedule) -> TaskBuilder:
        """Sets the schedule for the task."""
        self.schedule = schedule
        return self

    def with_sql_statement(self, sql_statement: str) -> TaskBuilder:
        """Sets the SQL statement for the task."""
        self.sql_statement = sql_statement
        return self

    def with_suspend_after_failures(self, num_failures: int) -> TaskBuilder:
        """Sets the number of failures before suspension."""
        self.suspend_task_after_num_failures = num_failures
        return self

    def with_tags(self, tags: Dict[str, str]) -> TaskBuilder:
        """Sets the tags for the task."""
        self.tags = tags
        return self

    def with_task_type(self, task_type: TaskType) -> TaskBuilder:
        """Sets the task type."""
        self.task_type = task_type
        return self

    def with_timeout(self, timeout_ms: int) -> TaskBuilder:
        """Sets the task timeout in milliseconds."""
        self.user_task_timeout_ms = timeout_ms
        return self

    def with_warehouse(self, warehouse: str) -> TaskBuilder:
        """Sets the warehouse for the task."""
        self.warehouse = warehouse
        return self

    def with_warehouse_size(self, size: WarehouseSize) -> TaskBuilder:
        """Sets the warehouse size for the task."""
        self.warehouse_size = size
        return self

    def with_when_condition(self, condition: str) -> TaskBuilder:
        """Sets the condition for task execution."""
        self.when = condition
        return self
