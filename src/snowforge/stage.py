from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Union

from snowforge.utilities import (
    sql_format_dict,
    sql_format_tags,
    sql_quote_comment,
    sql_quote_string,
)

from .file_format import FileFormatSpecification


class StorageIntegration(str, Enum):
    """Represents storage integration types for external stages."""

    AZURE = "AZURE"
    GCS = "GCS"
    S3 = "S3"
    S3_COMPATIBLE = "S3_COMPATIBLE"

    def __str__(self) -> str:
        """Returns the SQL representation of the storage integration."""
        return self.value


class InternalStageEncryptionType(str, Enum):
    """Supported encryption types for internal stages."""

    FULL = "SNOWFLAKE_FULL"
    SSE = "SNOWFLAKE_SSE"

    def __str__(self) -> str:
        """Returns the SQL representation of the encryption type."""
        return self.value


@dataclass
class InternalStageParams:
    """Parameters for internal stages.

    Attributes:
        encryption: The encryption settings for the stage
    """

    encryption: Optional[InternalStageEncryptionType] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the internal stage parameters."""
        parts = []
        if self.encryption:
            parts.append(
                f"ENCRYPTION = (TYPE = {sql_quote_string(str(self.encryption))})"
            )
        return " ".join(parts)


@dataclass
class AzureExternalStageParams:
    """Parameters for Microsoft Azure external stages.

    Attributes:
        storage_integration: The name of the storage integration to use
        url: The URL for the Azure storage service
        encryption: The encryption settings for the stage
    """

    storage_integration: str
    url: str
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the Azure external stage."""
        parts = [
            f"URL = {sql_quote_string(self.url)}",
            f"STORAGE_INTEGRATION = {self.storage_integration}",
        ]
        if self.encryption:
            parts.append(f"ENCRYPTION = {sql_format_dict(self.encryption)}")
        return " ".join(parts)


@dataclass
class GCSExternalStageParams:
    """Parameters for Google Cloud Storage external stages.

    Attributes:
        storage_integration: The name of the storage integration to use
        url: The URL for the GCS storage service
        encryption: The encryption settings for the stage
    """

    storage_integration: str
    url: str
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the GCS external stage."""
        parts = [
            f"URL = {sql_quote_string(self.url)}",
            f"STORAGE_INTEGRATION = {self.storage_integration}",
        ]
        if self.encryption:
            parts.append(f"ENCRYPTION = {sql_format_dict(self.encryption)}")
        return " ".join(parts)


@dataclass
class S3ExternalStageParams:
    """Parameters for Amazon S3 external stages.

    Attributes:
        url: The URL for the S3 storage service
        credentials: The credentials for the S3 storage service
        encryption: The encryption settings for the stage
        storage_integration: The name of the storage integration to use
    """

    url: str
    credentials: Optional[Dict[str, str]] = None
    encryption: Optional[Dict[str, str]] = None
    storage_integration: Optional[str] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the S3 external stage."""
        parts = [f"URL = {sql_quote_string(self.url)}"]
        if self.storage_integration:
            parts.append(f"STORAGE_INTEGRATION = {self.storage_integration}")
        if self.credentials:
            parts.append(f"CREDENTIALS = {sql_format_dict(self.credentials)}")
        if self.encryption:
            parts.append(f"ENCRYPTION = {sql_format_dict(self.encryption)}")
        return " ".join(parts)


@dataclass
class S3CompatibleExternalStageParams:
    """Parameters for S3-compatible external stages.

    Attributes:
        endpoint: The endpoint URL for the S3-compatible storage service
        storage_integration: The name of the storage integration to use
        url: The URL for the S3-compatible storage service
        encryption: The encryption settings for the stage
    """

    endpoint: str
    storage_integration: str
    url: str
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the S3-compatible external stage."""
        parts = [
            f"URL = {sql_quote_string(self.url)}",
            f"STORAGE_INTEGRATION = {self.storage_integration}",
            f"ENDPOINT = {sql_quote_string(self.endpoint)}",
        ]
        if self.encryption:
            parts.append(f"ENCRYPTION = {sql_format_dict(self.encryption)}")
        return " ".join(parts)


@dataclass
class DirectoryTableParams:
    """Base class for directory table parameters.

    Attributes:
        enable: Whether directory tables are enabled (default: True)
        refresh_on_create: Whether to refresh directory tables on creation (default: True)
    """

    enable: bool = True
    refresh_on_create: bool = True

    def to_sql(self) -> str:
        """Generates the SQL statement for the directory table parameters."""
        parts = []
        if self.enable:
            parts.append("ENABLE = TRUE")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = TRUE")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class InternalDirectoryTableParams(DirectoryTableParams):
    """
    Directory table parameters for internal stages.

    Attributes:
        enable: Whether directory tables are enabled (default: True)
        refresh_on_create: Whether to refresh directory tables on creation (default: True)
    """

    def to_sql(self) -> str:
        """Generates the SQL statement for the directory table parameters."""
        parts = []
        if self.enable:
            parts.append("ENABLE = TRUE")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = TRUE")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class AzureDirectoryTableParams(DirectoryTableParams):
    """Directory table parameters for Microsoft Azure external stages.

    Attributes:
        notification_integration: The name of the notification integration to use
    """

    notification_integration: Optional[str] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the Azure directory table parameters."""
        parts = []
        if self.enable:
            parts.append("ENABLE = TRUE")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = TRUE")
        if self.notification_integration:
            parts.append(f"NOTIFICATION_INTEGRATION = {self.notification_integration}")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class GCSDirectoryTableParams(DirectoryTableParams):
    """Directory table parameters for Google Cloud Storage external stages.

    Attributes:
        notification_integration: The name of the notification integration to use
    """

    notification_integration: Optional[str] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the GCS directory table parameters."""
        parts = []
        if self.enable:
            parts.append("ENABLE = TRUE")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = TRUE")
        if self.notification_integration:
            parts.append(f"NOTIFICATION_INTEGRATION = {self.notification_integration}")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class S3DirectoryTableParams(DirectoryTableParams):
    """Directory table parameters for Amazon S3 external stages.

    Attributes:
        aws_role: The AWS role to use for the directory table
        aws_sns_topic: The AWS SNS topic to use for the directory table
        notification_integration: The name of the notification integration to use
    """

    aws_role: Optional[str] = None
    aws_sns_topic: Optional[str] = None
    notification_integration: Optional[str] = None

    def to_sql(self) -> str:
        """Generates the SQL statement for the S3 directory table parameters."""
        parts = []
        if self.enable:
            parts.append("ENABLE = TRUE")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = TRUE")
        if self.aws_sns_topic:
            parts.append(f"AWS_SNS_TOPIC = {sql_quote_string(self.aws_sns_topic)}")
        if self.aws_role:
            parts.append(f"AWS_ROLE = {sql_quote_string(self.aws_role)}")
        if self.notification_integration:
            parts.append(f"NOTIFICATION_INTEGRATION = {self.notification_integration}")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class Stage:
    """
    Represents a Snowflake stage configuration.

    A stage can be either internal or external (S3, GCS, Azure) and includes
    various parameters for configuration.
    """

    def __init__(
        self,
        name: str,
        stage_params: Optional[
            Union[
                InternalStageParams,
                S3ExternalStageParams,
                S3CompatibleExternalStageParams,
                GCSExternalStageParams,
                AzureExternalStageParams,
            ]
        ] = None,
        directory_table_params: Optional[
            Union[
                InternalDirectoryTableParams,
                S3DirectoryTableParams,
                GCSDirectoryTableParams,
                AzureDirectoryTableParams,
            ]
        ] = None,
        file_format: Optional[FileFormatSpecification] = None,
        comment: Optional[str] = None,
        tags: Dict[str, str] = field(default_factory=dict),
        is_create_or_replace: bool = False,
        is_create_if_not_exists: bool = False,
        is_temporary: bool = False,
    ):
        """Initializes a new Stage instance."""
        self.comment = comment
        self.directory_table_params = directory_table_params
        self.file_format = file_format
        self.is_create_if_not_exists = is_create_if_not_exists
        self.is_create_or_replace = is_create_or_replace
        self.is_temporary = is_temporary
        self.name = name
        self.stage_params = stage_params
        self.tags = tags or {}

    @classmethod
    def builder(cls, name: str) -> StageBuilder:
        """Creates a new StageBuilder instance."""
        return StageBuilder(name)

    def to_sql(self) -> str:
        """Generates the SQL statement for the stage."""
        parts = []

        if self.is_create_or_replace:
            parts.append("CREATE OR REPLACE")
        else:
            parts.append("CREATE")

        if self.is_temporary:
            parts.append("TEMPORARY")

        parts.append("STAGE")

        if self.is_create_if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.name)

        if self.stage_params:
            parts.append(self.stage_params.to_sql())

        if self.directory_table_params:
            parts.append(self.directory_table_params.to_sql())

        if self.file_format:
            parts.append(f"FILE_FORMAT = ({self.file_format.to_sql()})")

        if self.comment:
            parts.append(f"COMMENT = {sql_quote_comment(self.comment)}")

        if self.tags:
            parts.append(f"TAG {sql_format_tags(self.tags)}")

        return " ".join(parts)


class StageBuilder:
    """Builder for Stage instances."""

    def __init__(self, name: str):
        """Initializes a new StageBuilder instance."""
        self.comment = None
        self.directory_table_params = None
        self.file_format = None
        self.is_create_if_not_exists = False
        self.is_create_or_replace = False
        self.is_temporary = False
        self.name = name
        self.stage_params = None
        self.tags = {}

    def build(self) -> Stage:
        """Builds and returns a new Stage instance."""
        if not self.name:
            raise ValueError("Stage name must be set")

        return Stage(
            comment=self.comment,
            directory_table_params=self.directory_table_params,
            file_format=self.file_format,
            is_create_if_not_exists=self.is_create_if_not_exists,
            is_create_or_replace=self.is_create_or_replace,
            is_temporary=self.is_temporary,
            name=self.name,
            stage_params=self.stage_params,
            tags=self.tags,
        )

    def with_comment(self, comment: str) -> StageBuilder:
        """Sets the stage comment."""
        self.comment = comment
        return self

    def with_create_or_replace(self) -> StageBuilder:
        """Sets the stage to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def with_create_if_not_exists(self) -> StageBuilder:
        """Sets the stage to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def with_directory_table_params(
        self,
        params: Union[
            InternalDirectoryTableParams,
            S3DirectoryTableParams,
            GCSDirectoryTableParams,
            AzureDirectoryTableParams,
        ],
    ) -> StageBuilder:
        """Sets the directory table parameters."""
        self.directory_table_params = params
        return self

    def with_file_format(self, file_format: FileFormatSpecification) -> StageBuilder:
        """Sets the file format specification."""
        self.file_format = file_format
        return self

    def with_stage_params(
        self,
        params: Union[
            InternalStageParams,
            S3ExternalStageParams,
            S3CompatibleExternalStageParams,
            GCSExternalStageParams,
            AzureExternalStageParams,
        ],
    ) -> StageBuilder:
        """Sets the stage parameters."""
        self.stage_params = params
        return self

    def with_tag(self, key: str, value: str) -> StageBuilder:
        """Adds a tag to the stage."""
        self.tags[key] = value
        return self

    def with_temporary(self) -> StageBuilder:
        """Sets the stage as temporary."""
        self.is_temporary = True
        return self
