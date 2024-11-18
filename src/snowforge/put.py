from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

from snowforge.utilities import sql_format_boolean

from .file_format import CompressionType


class InternalStage:
    """Represents different types of internal stages in Snowflake."""

    def __init__(self, stage_type: Literal["table", "user", "named"], name: str):
        """Initializes an InternalStage instance."""
        self.stage_type = stage_type
        self.name = name

    @classmethod
    def named(cls, name: str) -> InternalStage:
        """Creates a named stage reference."""
        return cls("named", name)

    @classmethod
    def table(cls, name: str) -> InternalStage:
        """Creates a table stage reference."""
        return cls("table", name)

    @classmethod
    def user(cls, name: str) -> InternalStage:
        """Creates a user stage reference."""
        return cls("user", name)

    def __str__(self) -> str:
        """Returns the SQL representation of the stage."""
        if self.stage_type == "table":
            return f"@%{self.name}"
        elif self.stage_type == "user":
            return f"@~/{self.name}"
        else:
            return f"@{self.name}"


@dataclass
class Put:
    """
    Represents the options for the Snowflake PUT command.

    The PUT command is used to stage (upload) files to a Snowflake stage.
    This class encapsulates all the options that can be specified for a PUT operation.

    Attributes:
        file_path (Path): The path to the file to upload.
        stage (InternalStage): The target stage for the file upload.
        auto_compress (bool): Whether to automatically compress the file during upload.
        overwrite (bool): Whether to overwrite existing files in the stage.
        parallel (Optional[int]): The number of threads to use for parallel file transfers.
        source_compression (CompressionType): The compression type of the source file.
    """

    file_path: Path
    stage: InternalStage
    auto_compress: bool = False
    overwrite: bool = False
    parallel: Optional[int] = None
    source_compression: CompressionType = CompressionType.AUTO

    @classmethod
    def builder(cls) -> PutBuilder:
        """Creates a new PutBuilder instance."""
        return PutBuilder()

    def to_sql(self) -> str:
        """Generates the SQL statement for the PUT command based on the current options."""
        parts = ["PUT"]

        # Convert to absolute path and format properly
        file_path = str(self.file_path.absolute())
        if not file_path.startswith('/'):
            file_path = '/' + file_path
        parts.append(f"'file://{file_path}'")

        parts.append(str(self.stage))

        if self.parallel is not None:
            parts.append(f"PARALLEL = {self.parallel}")

        parts.append(f"AUTO_COMPRESS = {sql_format_boolean(self.auto_compress)}")

        if self.source_compression != CompressionType.AUTO:
            parts.append(f"SOURCE_COMPRESSION = {str(self.source_compression)}")

        if self.overwrite:
            parts.append("OVERWRITE = TRUE")

        return " ".join(parts)


class PutBuilder:
    """Builder class for Put command options."""

    def __init__(self):
        """Initializes a PutBuilder instance."""
        self.auto_compress: bool = False
        self.file_path: Optional[Path] = None
        self.overwrite: bool = False
        self.parallel: Optional[int] = None
        self.source_compression: CompressionType = CompressionType.AUTO
        self.stage: Optional[InternalStage] = None

    def build(self) -> Put:
        """Builds and returns a new Put instance."""
        if self.file_path is None:
            raise ValueError("file_path must be set")
        if self.stage is None:
            raise ValueError("stage must be set")

        return Put(
            auto_compress=self.auto_compress,
            file_path=self.file_path,
            overwrite=self.overwrite,
            parallel=self.parallel,
            source_compression=self.source_compression,
            stage=self.stage,
        )

    def with_auto_compress(self, auto_compress: bool) -> PutBuilder:
        """Sets whether to automatically compress the file during upload."""
        self.auto_compress = auto_compress
        return self

    def with_file_path(self, file_path: Path | str) -> PutBuilder:
        """Sets the file path to upload."""
        if isinstance(file_path, str):
            file_path = Path(file_path)
        self.file_path = file_path
        return self

    def with_overwrite(self, overwrite: bool) -> PutBuilder:
        """Sets whether to overwrite existing files in the stage."""
        self.overwrite = overwrite
        return self

    def with_parallel(self, parallel: int) -> PutBuilder:
        """Sets the number of threads to use for parallel file transfers."""
        if not 1 <= parallel <= 99:
            raise ValueError("Parallel value must be between 1 and 99")
        self.parallel = parallel
        return self

    def with_source_compression(self, compression: CompressionType) -> PutBuilder:
        """Sets the compression type of the source file."""
        self.source_compression = compression
        return self

    def with_stage(self, stage: InternalStage) -> PutBuilder:
        """Sets the target stage."""
        self.stage = stage
        return self
