from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Union

from .file_format import FileFormatSpecification


class StorageIntegration(str, Enum):
    """Represents storage integration types for external stages."""

    S3 = "S3"
    GCS = "GCS"
    AZURE = "AZURE"
    S3_COMPATIBLE = "S3_COMPATIBLE"


@dataclass
class InternalStageParams:
    """Parameters for internal stages."""

    url: Optional[str] = None
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        parts = []
        if self.url:
            parts.append(f"URL = '{self.url}'")
        if self.encryption:
            enc_parts = [f"{k} = '{v}'" for k, v in self.encryption.items()]
            parts.append(f"ENCRYPTION = ({' '.join(enc_parts)})")
        return " ".join(parts)


@dataclass
class S3ExternalStageParams:
    """Parameters for Amazon S3 external stages."""

    url: str
    storage_integration: Optional[str] = None
    credentials: Optional[Dict[str, str]] = None
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        parts = [f"URL = '{self.url}'"]
        if self.storage_integration:
            parts.append(f"STORAGE_INTEGRATION = {self.storage_integration}")
        if self.credentials:
            cred_parts = [f"{k} = '{v}'" for k, v in self.credentials.items()]
            parts.append(f"CREDENTIALS = ({' '.join(cred_parts)})")
        if self.encryption:
            enc_parts = [f"{k} = '{v}'" for k, v in self.encryption.items()]
            parts.append(f"ENCRYPTION = ({' '.join(enc_parts)})")
        return " ".join(parts)


@dataclass
class GCSExternalStageParams:
    """Parameters for Google Cloud Storage external stages."""

    url: str
    storage_integration: str
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        parts = [
            f"URL = '{self.url}'",
            f"STORAGE_INTEGRATION = {self.storage_integration}",
        ]
        if self.encryption:
            enc_parts = [f"{k} = '{v}'" for k, v in self.encryption.items()]
            parts.append(f"ENCRYPTION = ({' '.join(enc_parts)})")
        return " ".join(parts)


@dataclass
class AzureExternalStageParams:
    """Parameters for Microsoft Azure external stages."""

    url: str
    storage_integration: str
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        parts = [
            f"URL = '{self.url}'",
            f"STORAGE_INTEGRATION = {self.storage_integration}",
        ]
        if self.encryption:
            enc_parts = [f"{k} = '{v}'" for k, v in self.encryption.items()]
            parts.append(f"ENCRYPTION = ({' '.join(enc_parts)})")
        return " ".join(parts)


@dataclass
class S3CompatibleExternalStageParams:
    """Parameters for S3-compatible external stages."""

    url: str
    storage_integration: str
    endpoint: str
    encryption: Optional[Dict[str, str]] = None

    def to_sql(self) -> str:
        parts = [
            f"URL = '{self.url}'",
            f"STORAGE_INTEGRATION = {self.storage_integration}",
            f"ENDPOINT = '{self.endpoint}'",
        ]
        if self.encryption:
            enc_parts = [f"{k} = '{v}'" for k, v in self.encryption.items()]
            parts.append(f"ENCRYPTION = ({' '.join(enc_parts)})")
        return " ".join(parts)


@dataclass
class DirectoryTableParams:
    """Base class for directory table parameters."""

    enable: bool = True
    refresh_on_create: bool = True

    def to_sql(self) -> str:
        parts = []
        if self.enable:
            parts.append("ENABLE = true")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = true")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class InternalDirectoryTableParams(DirectoryTableParams):
    pass


@dataclass
class S3DirectoryTableParams(DirectoryTableParams):
    aws_sns_topic: Optional[str] = None
    aws_role: Optional[str] = None
    notification_integration: Optional[str] = None

    def to_sql(self) -> str:
        parts = []
        if self.enable:
            parts.append("ENABLE = true")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = true")
        if self.aws_sns_topic:
            parts.append(f"AWS_SNS_TOPIC = '{self.aws_sns_topic}'")
        if self.aws_role:
            parts.append(f"AWS_ROLE = '{self.aws_role}'")
        if self.notification_integration:
            parts.append(f"NOTIFICATION_INTEGRATION = {self.notification_integration}")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class GCSDirectoryTableParams(DirectoryTableParams):
    notification_integration: Optional[str] = None

    def to_sql(self) -> str:
        parts = []
        if self.enable:
            parts.append("ENABLE = true")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = true")
        if self.notification_integration:
            parts.append(f"NOTIFICATION_INTEGRATION = {self.notification_integration}")
        return f"DIRECTORY = ({' '.join(parts)})"


@dataclass
class AzureDirectoryTableParams(DirectoryTableParams):
    notification_integration: Optional[str] = None

    def to_sql(self) -> str:
        parts = []
        if self.enable:
            parts.append("ENABLE = true")
        if self.refresh_on_create:
            parts.append("REFRESH_ON_CREATE = true")
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

    name: str
    stage_params: Optional[
        Union[
            InternalStageParams,
            S3ExternalStageParams,
            S3CompatibleExternalStageParams,
            GCSExternalStageParams,
            AzureExternalStageParams,
        ]
    ] = None
    directory_table_params: Optional[
        Union[
            InternalDirectoryTableParams,
            S3DirectoryTableParams,
            GCSDirectoryTableParams,
            AzureDirectoryTableParams,
        ]
    ] = None
    file_format: Optional[FileFormatSpecification] = None
    comment: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    is_create_or_replace: bool = False
    is_create_if_not_exists: bool = False
    is_temporary: bool = False

    @classmethod
    def builder(cls, name: str) -> Stage:
        """Creates a new Stage builder."""
        return cls(name=name)

    def with_stage_params(
        self,
        params: Union[
            InternalStageParams,
            S3ExternalStageParams,
            S3CompatibleExternalStageParams,
            GCSExternalStageParams,
            AzureExternalStageParams,
        ],
    ) -> Stage:
        """Sets the stage parameters."""
        self.stage_params = params
        return self

    def with_directory_table_params(
        self,
        params: Union[
            InternalDirectoryTableParams,
            S3DirectoryTableParams,
            GCSDirectoryTableParams,
            AzureDirectoryTableParams,
        ],
    ) -> Stage:
        """Sets the directory table parameters."""
        self.directory_table_params = params
        return self

    def with_file_format(self, file_format: FileFormatSpecification) -> Stage:
        """Sets the file format specification."""
        self.file_format = file_format
        return self

    def with_comment(self, comment: str) -> Stage:
        """Sets the stage comment."""
        self.comment = comment
        return self

    def with_tags(self, tags: Dict[str, str]) -> Stage:
        """Sets the stage tags."""
        self.tags = tags
        return self

    def create_or_replace(self) -> Stage:
        """Sets the stage to be created or replaced."""
        self.is_create_or_replace = True
        return self

    def create_if_not_exists(self) -> Stage:
        """Sets the stage to be created only if it doesn't exist."""
        self.is_create_if_not_exists = True
        return self

    def temporary(self) -> Stage:
        """Sets the stage as temporary."""
        self.is_temporary = True
        return self

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
            parts.append(f"COMMENT = '{self.comment.replace(chr(39), chr(39)*2)}'")

        if self.tags:
            tag_parts = [f"{k} = '{v}'" for k, v in self.tags.items()]
            parts.append(f"TAGS = ({' '.join(tag_parts)})")

        return " ".join(parts)
