import pytest

from snowforge.file_format import FileFormatSpecification
from snowforge.stage import (
    AzureExternalStageParams,
    InternalDirectoryTableParams,
    InternalStageEncryptionType,
    InternalStageParams,
    S3DirectoryTableParams,
    S3ExternalStageParams,
    Stage,
    StorageIntegration,
)


@pytest.fixture
def basic_stage():
    return Stage.builder("TEST_STAGE").build()


@pytest.fixture
def internal_stage_params():
    return InternalStageParams(encryption=InternalStageEncryptionType.SSE)


@pytest.fixture
def file_format():
    return FileFormatSpecification.named("TEST_FORMAT")


def test_basic_stage_creation(basic_stage):
    """Test creation of a basic stage with minimal parameters."""
    assert basic_stage.name == "TEST_STAGE"
    assert basic_stage.to_sql() == "CREATE STAGE TEST_STAGE"


def test_stage_with_create_or_replace():
    """Test stage creation with CREATE OR REPLACE clause."""
    stage = Stage.builder("TEST_STAGE").with_create_or_replace().build()
    assert stage.to_sql() == "CREATE OR REPLACE STAGE TEST_STAGE"


def test_stage_with_if_not_exists():
    """Test stage creation with IF NOT EXISTS clause."""
    stage = Stage.builder("TEST_STAGE").with_create_if_not_exists().build()
    assert stage.to_sql() == "CREATE STAGE IF NOT EXISTS TEST_STAGE"


def test_internal_stage_full_config(internal_stage_params, file_format):
    """Test creation of an internal stage with full configuration."""
    stage = (
        Stage.builder("TEST_INTERNAL")
        .with_stage_params(internal_stage_params)
        .with_directory_table_params(InternalDirectoryTableParams())
        .with_file_format(file_format)
        .with_comment("Test internal stage")
        .with_tag("env", "test")
        .build()
    )

    expected = (
        'CREATE STAGE TEST_INTERNAL '
        "ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') "
        "DIRECTORY = (ENABLE = TRUE REFRESH_ON_CREATE = TRUE) "
        "FILE_FORMAT = (FORMAT_NAME = 'TEST_FORMAT') "
        "COMMENT = 'Test internal stage' "
        "TAG (env = 'test')"
    )
    assert stage.to_sql() == expected


def test_azure_external_stage():
    """Test creation of an Azure external stage."""
    params = AzureExternalStageParams(
        storage_integration="AZURE_INT",
        url="azure://container/path",
        encryption={"TYPE": "AZURE_CSE"},
    )
    stage = Stage.builder("TEST_AZURE").with_stage_params(params).build()
    expected = (
        'CREATE STAGE TEST_AZURE '
        "URL = 'azure://container/path' "
        "STORAGE_INTEGRATION = AZURE_INT "
        "ENCRYPTION = (TYPE = 'AZURE_CSE')"
    )
    assert stage.to_sql() == expected


def test_s3_external_stage():
    """Test creation of an S3 external stage."""
    params = S3ExternalStageParams(
        url="s3://bucket/path",
        storage_integration="S3_INT",
    )
    stage = Stage.builder("TEST_S3").with_stage_params(params).build()
    expected = (
        'CREATE STAGE TEST_S3 '
        "URL = 's3://bucket/path' "
        "STORAGE_INTEGRATION = S3_INT"
    )
    assert stage.to_sql() == expected


def test_stage_builder_without_name():
    """Test that building a stage without a name raises an error."""
    with pytest.raises(ValueError, match="Stage name must be set"):
        Stage.builder("").build()


def test_storage_integration_str():
    """Test string representation of storage integration types."""
    assert str(StorageIntegration.AZURE) == "AZURE"
    assert str(StorageIntegration.GCS) == "GCS"
    assert str(StorageIntegration.S3) == "S3"
    assert str(StorageIntegration.S3_COMPATIBLE) == "S3_COMPATIBLE"


def test_s3_directory_table_params():
    """Test S3 directory table parameters."""
    params = S3DirectoryTableParams(
        aws_role="arn:aws:iam::123456789012:role/test",
        aws_sns_topic="arn:aws:sns:region:123456789012:topic",
        notification_integration="SNS_INT",
    )
    stage = Stage.builder("TEST_S3").with_directory_table_params(params).build()
    expected = (
        'CREATE STAGE TEST_S3 '
        'DIRECTORY = (ENABLE = TRUE REFRESH_ON_CREATE = TRUE '
        "AWS_SNS_TOPIC = 'arn:aws:sns:region:123456789012:topic' "
        "AWS_ROLE = 'arn:aws:iam::123456789012:role/test' "
        'NOTIFICATION_INTEGRATION = SNS_INT)'
    )
    assert stage.to_sql() == expected
