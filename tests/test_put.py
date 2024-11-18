from pathlib import Path

import pytest

from snowforge.file_format import CompressionType
from snowforge.put import InternalStage, Put, PutBuilder


@pytest.fixture
def sample_file_path():
    return Path("/path/to/data.csv")


@pytest.fixture
def named_stage():
    return InternalStage.named("MY_STAGE")


@pytest.fixture
def table_stage():
    return InternalStage.table("MY_TABLE")


@pytest.fixture
def user_stage():
    return InternalStage.user("my_files")


class TestInternalStage:
    def test_named_stage_string_representation(self, named_stage):
        assert str(named_stage) == "@MY_STAGE"

    def test_table_stage_string_representation(self, table_stage):
        assert str(table_stage) == "@%MY_TABLE"

    def test_user_stage_string_representation(self, user_stage):
        assert str(user_stage) == "@~/my_files"


class TestPutBuilder:
    def test_minimal_valid_build(self, sample_file_path, named_stage):
        put = (
            PutBuilder()
            .with_file_path(sample_file_path)
            .with_stage(named_stage)
            .build()
        )
        assert isinstance(put, Put)
        assert put.file_path == sample_file_path
        assert put.stage == named_stage

    def test_complete_build(self, sample_file_path, named_stage):
        put = (
            PutBuilder()
            .with_file_path(sample_file_path)
            .with_stage(named_stage)
            .with_auto_compress(True)
            .with_overwrite(True)
            .with_parallel(4)
            .with_source_compression(CompressionType.GZIP)
            .build()
        )
        assert put.auto_compress is True
        assert put.overwrite is True
        assert put.parallel == 4
        assert put.source_compression == CompressionType.GZIP

    def test_build_without_required_fields(self):
        builder = PutBuilder()
        with pytest.raises(ValueError, match="file_path must be set"):
            builder.build()

        builder.with_file_path("/some/path")
        with pytest.raises(ValueError, match="stage must be set"):
            builder.build()

    def test_invalid_parallel_value(self, sample_file_path, named_stage):
        builder = PutBuilder().with_file_path(sample_file_path).with_stage(named_stage)

        with pytest.raises(ValueError, match="Parallel value must be between 1 and 99"):
            builder.with_parallel(0)

        with pytest.raises(ValueError, match="Parallel value must be between 1 and 99"):
            builder.with_parallel(100)

    def test_string_file_path_conversion(self, named_stage):
        put = (
            PutBuilder()
            .with_file_path("/path/to/file.csv")
            .with_stage(named_stage)
            .build()
        )
        assert isinstance(put.file_path, Path)


class TestPut:
    def test_basic_sql_generation(self, sample_file_path, named_stage):
        put = Put(file_path=sample_file_path, stage=named_stage)
        sql = put.to_sql()
        assert sql.startswith("PUT 'file:///")
        assert "AUTO_COMPRESS = FALSE" in sql
        assert "@MY_STAGE" in sql

    def test_sql_generation_with_all_options(self, sample_file_path, named_stage):
        put = Put(
            file_path=sample_file_path,
            stage=named_stage,
            auto_compress=True,
            overwrite=True,
            parallel=5,
            source_compression=CompressionType.GZIP,
        )
        sql = put.to_sql()
        assert "PARALLEL = 5" in sql
        assert "AUTO_COMPRESS = TRUE" in sql
        assert "SOURCE_COMPRESSION = GZIP" in sql
        assert "OVERWRITE = TRUE" in sql

    def test_builder_creation(self):
        builder = Put.builder()
        assert isinstance(builder, PutBuilder)

    @pytest.mark.parametrize(
        "stage_type,stage_name,expected",
        [
            ("named", "STAGE1", "@STAGE1"),
            ("table", "TABLE1", "@%TABLE1"),
            ("user", "files", "@~/files"),
        ],
    )
    def test_different_stage_types(
        self, sample_file_path, stage_type, stage_name, expected
    ):
        stage = InternalStage(stage_type, stage_name)
        put = Put(file_path=sample_file_path, stage=stage)
        sql = put.to_sql()
        assert expected in sql
