import pytest

from snowforge.copy_into import (
    CopyInto,
    CopyIntoOptions,
    CopyIntoSource,
    CopyIntoTarget,
    MatchByColumnName,
    OnError,
)


def test_copy_into_builder_basic():
    copy_into = (
        CopyInto.builder()
        .with_source(CopyIntoSource.stage("my_stage"))
        .with_target(CopyIntoTarget.table("my_table"))
        .build()
    )

    assert copy_into.source.name == "my_stage"
    assert copy_into.target.name == "my_table"
    assert copy_into.source.source_type == "stage"
    assert copy_into.target.target_type == "table"


def test_copy_into_builder_validation():
    # Create separate builder instances for each test case
    source_builder = CopyInto.builder()
    target_builder = CopyInto.builder()

    # Test missing required fields
    with pytest.raises(ValueError, match="target must be set"):
        source_builder.with_source(CopyIntoSource.stage("my_stage")).build()

    with pytest.raises(ValueError, match="source must be set"):
        target_builder.with_target(CopyIntoTarget.table("my_table")).build()


def test_copy_into_options_builder():
    options = (
        CopyIntoOptions.builder()
        .with_on_error(OnError.CONTINUE)
        .with_purge(True)
        .with_size_limit(1000)
        .with_match_by_column_name(MatchByColumnName.CASE_INSENSITIVE)
        .build()
    )

    assert options.on_error == OnError.CONTINUE
    assert options.purge is True
    assert options.size_limit == 1000
    assert options.match_by_column_name == MatchByColumnName.CASE_INSENSITIVE
