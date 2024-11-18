import pytest

from snowforge.task import Schedule, Task, TaskType, WarehouseSize


@pytest.fixture
def basic_schedule():
    return Schedule(cron_expr="0 */1 * * *", timezone="UTC")


@pytest.fixture
def interval_schedule():
    return Schedule(interval_minutes=30)


@pytest.fixture
def basic_task():
    return (
        Task.builder("TEST_TASK")
        .with_task_type(TaskType.SQL)
        .with_sql_statement("SELECT 1")
        .build()
    )


@pytest.fixture
def complex_task():
    return (
        Task.builder("COMPLEX_TASK")
        .with_create_or_replace()
        .with_warehouse_size(WarehouseSize.XSMALL)
        .with_schedule(Schedule(cron_expr="0 */1 * * *", timezone="UTC"))
        .with_task_type(TaskType.SQL)
        .with_sql_statement("SELECT * FROM test_table")
        .with_comment("Test task")
        .with_tags({"env": "test", "owner": "data_team"})
        .with_overlapping_execution(True)
        .with_session_parameters({"TIMEZONE": "UTC"})
        .build()
    )


def test_schedule_with_cron(basic_schedule):
    """Test schedule creation with cron expression."""
    expected = "'USING CRON 0 */1 * * * UTC'"
    assert basic_schedule.to_sql() == expected


def test_schedule_with_interval(interval_schedule):
    """Test schedule creation with interval."""
    expected = "'30 MINUTE'"
    assert interval_schedule.to_sql() == expected


def test_empty_schedule():
    """Test schedule with no parameters."""
    schedule = Schedule()
    assert schedule.to_sql() == ""


def test_task_creation_basic(basic_task):
    """Test basic task creation."""
    expected = "CREATE TASK TEST_TASK\nAS\nSELECT 1"
    assert basic_task.to_sql() == expected


def test_task_creation_complex(complex_task):
    """Test complex task creation with all options."""
    expected = (
        "CREATE OR REPLACE TASK COMPLEX_TASK\n"
        "WITH TAG (env = 'test', owner = 'data_team')\n"
        "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = XSMALL\n"
        "SCHEDULE = 'USING CRON 0 */1 * * * UTC'\n"
        "ALLOW_OVERLAPPING_EXECUTION = TRUE\n"
        "SESSION_PARAMETERS = (TIMEZONE = UTC)\n"
        "COMMENT = 'Test task'\n"
        "AS\n"
        "SELECT * FROM test_table"
    )
    assert complex_task.to_sql() == expected


def test_task_builder_validation():
    """Test task builder validation."""
    # Test missing name
    with pytest.raises(ValueError, match="Task name must be set"):
        Task.builder("").build()

    # Test missing task type
    with pytest.raises(ValueError, match="Task type must be set"):
        Task.builder("TEST_TASK").build()

    # Test missing SQL statement
    with pytest.raises(ValueError, match="SQL statement must be set"):
        Task.builder("TEST_TASK").with_task_type(TaskType.SQL).build()


def test_task_with_dependencies():
    """Test task creation with dependencies."""
    task = (
        Task.builder("DEPENDENT_TASK")
        .with_task_type(TaskType.SQL)
        .with_sql_statement("SELECT 1")
        .with_after_tasks(["TASK1", "TASK2"])
        .with_when_condition("SYSTEM$STREAM_HAS_DATA('TEST_STREAM')")
        .build()
    )
    expected = (
        "CREATE TASK DEPENDENT_TASK\n"
        "AFTER ('TASK1', 'TASK2')\n"
        "WHEN SYSTEM$STREAM_HAS_DATA('TEST_STREAM')\n"
        "AS\n"
        "SELECT 1"
    )
    assert task.to_sql() == expected


def test_task_with_error_handling():
    """Test task creation with error handling configuration."""
    task = (
        Task.builder("ERROR_HANDLING_TASK")
        .with_task_type(TaskType.SQL)
        .with_sql_statement("SELECT 1")
        .with_error_integration("ERROR_INT")
        .with_suspend_after_failures(3)
        .with_auto_retry_attempts(2)
        .build()
    )
    expected = (
        "CREATE TASK ERROR_HANDLING_TASK\n"
        "ERROR_INTEGRATION = ERROR_INT\n"
        "SUSPEND_TASK_AFTER_NUM_FAILURES = 3\n"
        "TASK_AUTO_RETRY_ATTEMPTS = 2\n"
        "AS\n"
        "SELECT 1"
    )
    assert task.to_sql() == expected


def test_task_with_timing_config():
    """Test task creation with timing configuration."""
    task = (
        Task.builder("TIMING_TASK")
        .with_task_type(TaskType.SQL)
        .with_sql_statement("SELECT 1")
        .with_timeout(3600000)  # 1 hour
        .with_minimum_trigger_interval(300)  # 5 minutes
        .build()
    )
    expected = (
        "CREATE TASK TIMING_TASK\n"
        "USER_TASK_TIMEOUT_MS = 3600000\n"
        "USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 300\n"
        "AS\n"
        "SELECT 1"
    )
    assert task.to_sql() == expected


def test_task_type_enum():
    """Test TaskType enum values."""
    assert str(TaskType.SQL) == "SQL"
    assert str(TaskType.STORED_PROCEDURE) == "STORED_PROCEDURE"
    assert str(TaskType.MULTI_STATEMENT) == "MULTI_STATEMENT"
    assert str(TaskType.PROCEDURAL_LOGIC) == "PROCEDURAL_LOGIC"


def test_warehouse_size_enum():
    """Test WarehouseSize enum values."""
    assert str(WarehouseSize.XSMALL) == "XSMALL"
    assert str(WarehouseSize.SMALL) == "SMALL"
    assert str(WarehouseSize.MEDIUM) == "MEDIUM"
    assert str(WarehouseSize.LARGE) == "LARGE"
    assert str(WarehouseSize.XLARGE) == "XLARGE"
