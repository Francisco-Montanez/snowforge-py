from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union


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


@dataclass
class Schedule:
    """Represents task schedule configuration."""

    cron_expr: Optional[str] = None
    timezone: Optional[str] = None
    interval_minutes: Optional[int] = None

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
    """

    name: str
    task_type: TaskType
    sql_statement: str
    tags: Dict[str, str] = field(default_factory=dict)
    warehouse: Optional[str] = None
    warehouse_size: Optional[WarehouseSize] = None
    schedule: Optional[Schedule] = None
    config: Optional[str] = None
    allow_overlapping_execution: Optional[bool] = None
    session_parameters: Dict[str, str] = field(default_factory=dict)
    user_task_timeout_ms: Optional[int] = None
    suspend_task_after_num_failures: Optional[int] = None
    error_integration: Optional[str] = None
    comment: Optional[str] = None
    finalize: Optional[str] = None
    task_auto_retry_attempts: Optional[int] = None
    user_task_minimum_trigger_interval_in_seconds: Optional[int] = None
    after: Optional[List[str]] = None
    when: Optional[str] = None
    is_create_or_replace: bool = False
    is_create_if_not_exists: bool = False

    @classmethod
    def builder(cls, name: str) -> TaskBuilder:
        """Creates a new TaskBuilder instance."""
        return TaskBuilder(name=name)

    def to_sql(self) -> str:
        """
        Generates the SQL statement for the task based on the Rust implementation from:
        """
        parts = []

        if self.is_create_or_replace:
            parts.append("CREATE OR REPLACE")
        else:
            parts.append("CREATE")
            if self.is_create_if_not_exists:
                parts.append("IF NOT EXISTS")

        parts.append(f"TASK {self.name}")

        if self.tags:
            tag_parts = [f"{k} = '{v}'" for k, v in self.tags.items()]
            parts.append(f"WITH TAG ({', '.join(tag_parts)})")

        if self.warehouse:
            parts.append(f"WAREHOUSE = {self.warehouse}")

        if self.warehouse_size:
            parts.append(
                f"USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = {self.warehouse_size.value}"
            )

        if self.schedule:
            schedule_sql = self.schedule.to_sql()
            if schedule_sql:
                parts.append(f"SCHEDULE = {schedule_sql}")

        if self.config:
            parts.append(f"CONFIG = '{self.config}'")

        if self.allow_overlapping_execution is not None:
            parts.append(
                f"ALLOW_OVERLAPPING_EXECUTION = {str(self.allow_overlapping_execution).upper()}"
            )

        if self.session_parameters:
            param_parts = [f"{k} = '{v}'" for k, v in self.session_parameters.items()]
            parts.append(f"SESSION_PARAMETERS = ({', '.join(param_parts)})")

        if self.user_task_timeout_ms is not None:
            parts.append(f"USER_TASK_TIMEOUT_MS = {self.user_task_timeout_ms}")

        if self.suspend_task_after_num_failures is not None:
            parts.append(
                f"SUSPEND_TASK_AFTER_NUM_FAILURES = {self.suspend_task_after_num_failures}"
            )

        if self.error_integration:
            parts.append(f"ERROR_INTEGRATION = {self.error_integration}")

        if self.comment:
            parts.append(f"COMMENT = '{self.comment.replace(chr(39), chr(39)*2)}'")

        if self.finalize:
            parts.append(f"FINALIZE = '{self.finalize}'")

        if self.task_auto_retry_attempts is not None:
            parts.append(f"TASK_AUTO_RETRY_ATTEMPTS = {self.task_auto_retry_attempts}")

        if self.user_task_minimum_trigger_interval_in_seconds is not None:
            parts.append(
                f"USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = {self.user_task_minimum_trigger_interval_in_seconds}"
            )

        if self.after:
            after_tasks = ", ".join(self.after)
            parts.append(f"AFTER {after_tasks}")

        if self.when:
            parts.append(f"WHEN {self.when}")

        parts.append("AS")
        parts.append(self.sql_statement.strip())

        return "\n".join(parts)


@dataclass
class TaskBuilder:
    """Builder for Task instances."""

    name: Optional[str] = None
    task_type: Optional[TaskType] = None
    sql_statement: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    warehouse: Optional[str] = None
    warehouse_size: Optional[WarehouseSize] = None
    schedule: Optional[Schedule] = None
    config: Optional[str] = None
    allow_overlapping_execution: bool = False
    session_parameters: Dict[str, str] = field(default_factory=dict)
    user_task_timeout_ms: Optional[int] = None
    suspend_task_after_num_failures: Optional[int] = None
    error_integration: Optional[str] = None
    comment: Optional[str] = None
    finalize: Optional[str] = None
    task_auto_retry_attempts: Optional[int] = None
    user_task_minimum_trigger_interval_in_seconds: Optional[int] = None
    after: Optional[List[str]] = None
    when: Optional[str] = None
    is_create_or_replace: bool = False
    is_create_if_not_exists: bool = False

    def with_task_type(self, task_type: TaskType) -> TaskBuilder:
        """Sets the task type."""
        self.task_type = task_type
        return self

    def with_sql_statement(self, sql_statement: str) -> TaskBuilder:
        """Sets the SQL statement for the task."""
        self.sql_statement = sql_statement
        return self

    def with_tags(self, tags: Dict[str, str]) -> TaskBuilder:
        """Sets the tags for the task."""
        self.tags = tags
        return self

    def with_warehouse(self, warehouse: str) -> TaskBuilder:
        """Sets the warehouse for the task."""
        self.warehouse = warehouse
        return self

    def with_warehouse_size(self, size: WarehouseSize) -> TaskBuilder:
        """Sets the warehouse size for the task."""
        self.warehouse_size = size
        return self

    def with_schedule(self, schedule: Schedule) -> TaskBuilder:
        """Sets the schedule for the task."""
        self.schedule = schedule
        return self

    def with_config(self, config: str) -> TaskBuilder:
        """Sets the configuration for the task."""
        self.config = config
        return self

    def with_overlapping_execution(self, allow: bool) -> TaskBuilder:
        """Sets whether overlapping execution is allowed."""
        self.allow_overlapping_execution = allow
        return self

    def with_session_parameters(self, params: Dict[str, str]) -> TaskBuilder:
        """Sets the session parameters for the task."""
        self.session_parameters = params
        return self

    def with_timeout(self, timeout_ms: int) -> TaskBuilder:
        """Sets the task timeout in milliseconds."""
        self.user_task_timeout_ms = timeout_ms
        return self

    def with_suspend_after_failures(self, num_failures: int) -> TaskBuilder:
        """Sets the number of failures before suspension."""
        self.suspend_task_after_num_failures = num_failures
        return self

    def with_error_integration(self, integration: str) -> TaskBuilder:
        """Sets the error integration for the task."""
        self.error_integration = integration
        return self

    def with_comment(self, comment: str) -> TaskBuilder:
        """Sets the task comment."""
        self.comment = comment
        return self

    def with_finalize(self, finalize: str) -> TaskBuilder:
        """Sets the finalize script for the task."""
        self.finalize = finalize
        return self

    def with_auto_retry_attempts(self, attempts: int) -> TaskBuilder:
        """Sets the number of auto retry attempts."""
        self.task_auto_retry_attempts = attempts
        return self

    def with_minimum_trigger_interval(self, interval_seconds: int) -> TaskBuilder:
        """Sets the minimum trigger interval in seconds."""
        self.user_task_minimum_trigger_interval_in_seconds = interval_seconds
        return self

    def with_after_tasks(self, tasks: List[str]) -> TaskBuilder:
        """Sets the predecessor tasks."""
        self.after = tasks
        return self

    def with_when_condition(self, condition: str) -> TaskBuilder:
        """Sets the condition for task execution."""
        self.when = condition
        return self

    def with_create_or_replace(self) -> TaskBuilder:
        """Sets the task to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> TaskBuilder:
        """Sets the task to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def build(self) -> Task:
        """Builds and returns a new Task instance."""
        if not self.name:
            raise ValueError("Task name must be set")
        if not self.task_type:
            raise ValueError("Task type must be set")
        if not self.sql_statement:
            raise ValueError("SQL statement must be set")

        return Task(
            name=self.name,
            task_type=self.task_type,
            sql_statement=self.sql_statement,
            tags=self.tags,
            warehouse=self.warehouse,
            warehouse_size=self.warehouse_size,
            schedule=self.schedule,
            config=self.config,
            allow_overlapping_execution=self.allow_overlapping_execution,
            session_parameters=self.session_parameters,
            user_task_timeout_ms=self.user_task_timeout_ms,
            suspend_task_after_num_failures=self.suspend_task_after_num_failures,
            error_integration=self.error_integration,
            comment=self.comment,
            finalize=self.finalize,
            task_auto_retry_attempts=self.task_auto_retry_attempts,
            user_task_minimum_trigger_interval_in_seconds=self.user_task_minimum_trigger_interval_in_seconds,
            after=self.after,
            when=self.when,
            is_create_or_replace=self.is_create_or_replace,
            is_create_if_not_exists=self.is_create_if_not_exists,
        )
