import pytest

from snowforge.stream import Stream, StreamMode, StreamType


@pytest.fixture
def basic_stream():
    return (
        Stream.builder("TEST_STREAM")
        .with_source("TEST_DB.TEST_SCHEMA.TEST_TABLE")
        .build()
    )


@pytest.fixture
def complex_stream():
    return (
        Stream.builder("COMPLEX_STREAM")
        .with_create_or_replace()
        .with_source("TEST_DB.TEST_SCHEMA.TEST_TABLE")
        .with_append_only(True)
        .with_show_initial_rows(True)
        .with_comment("Test stream")
        .with_tags({"env": "test", "owner": "data_team"})
        .with_mode(StreamMode.APPEND_ONLY)
        .with_type(StreamType.DELTA)
        .build()
    )


def test_basic_stream_creation(basic_stream):
    """Test creation of a basic stream with minimal parameters."""
    assert basic_stream.name == "TEST_STREAM"
    assert basic_stream.source == "TEST_DB.TEST_SCHEMA.TEST_TABLE"
    expected_sql = "CREATE STREAM TEST_STREAM ON TABLE TEST_DB.TEST_SCHEMA.TEST_TABLE"
    assert basic_stream.to_sql() == expected_sql


def test_stream_builder_validation():
    """Test stream builder validation."""
    # Test missing name
    with pytest.raises(ValueError, match="name must be set"):
        Stream.builder("").build()

    # Test missing source
    with pytest.raises(ValueError, match="source must be set"):
        Stream.builder("TEST_STREAM").build()


def test_stream_with_create_or_replace():
    """Test stream creation with CREATE OR REPLACE clause."""
    stream = (
        Stream.builder("TEST_STREAM")
        .with_create_or_replace()
        .with_source("TEST_TABLE")
        .build()
    )
    expected = "CREATE OR REPLACE STREAM TEST_STREAM ON TABLE TEST_TABLE"
    assert stream.to_sql() == expected


def test_stream_with_if_not_exists():
    """Test stream creation with IF NOT EXISTS clause."""
    stream = (
        Stream.builder("TEST_STREAM")
        .with_create_if_not_exists()
        .with_source("TEST_TABLE")
        .build()
    )
    expected = "CREATE STREAM IF NOT EXISTS TEST_STREAM ON TABLE TEST_TABLE"
    assert stream.to_sql() == expected


def test_stream_with_append_only():
    """Test stream creation with APPEND_ONLY option."""
    stream = (
        Stream.builder("TEST_STREAM")
        .with_source("TEST_TABLE")
        .with_append_only()
        .build()
    )
    expected = "CREATE STREAM TEST_STREAM ON TABLE TEST_TABLE APPEND_ONLY = TRUE"
    assert stream.to_sql() == expected


def test_stream_with_show_initial_rows():
    """Test stream creation with SHOW_INITIAL_ROWS option."""
    stream = (
        Stream.builder("TEST_STREAM")
        .with_source("TEST_TABLE")
        .with_show_initial_rows()
        .build()
    )
    expected = "CREATE STREAM TEST_STREAM ON TABLE TEST_TABLE SHOW_INITIAL_ROWS = TRUE"
    assert stream.to_sql() == expected


def test_stream_with_comment():
    """Test stream creation with comment."""
    stream = (
        Stream.builder("TEST_STREAM")
        .with_source("TEST_TABLE")
        .with_comment("Test comment")
        .build()
    )
    expected = "CREATE STREAM TEST_STREAM ON TABLE TEST_TABLE COMMENT = 'Test comment'"
    assert stream.to_sql() == expected


def test_stream_with_tags():
    """Test stream creation with tags."""
    stream = (
        Stream.builder("TEST_STREAM")
        .with_source("TEST_TABLE")
        .with_tags({"env": "test", "owner": "data_team"})
        .build()
    )
    expected = (
        "CREATE STREAM TEST_STREAM "
        "WITH TAG (env = 'test', owner = 'data_team') "
        "ON TABLE TEST_TABLE"
    )
    assert stream.to_sql() == expected


def test_complex_stream_configuration(complex_stream):
    """Test creation of a stream with all available options."""
    expected = (
        "CREATE OR REPLACE STREAM COMPLEX_STREAM "
        "WITH TAG (env = 'test', owner = 'data_team') "
        "ON TABLE TEST_DB.TEST_SCHEMA.TEST_TABLE "
        "APPEND_ONLY = TRUE "
        "SHOW_INITIAL_ROWS = TRUE "
        "COMMENT = 'Test stream'"
    )
    assert complex_stream.to_sql() == expected


def test_stream_mode_enum():
    """Test StreamMode enum values and string representation."""
    assert str(StreamMode.APPEND_ONLY) == "APPEND_ONLY"
    assert str(StreamMode.DEFAULT) == "DEFAULT"
    assert str(StreamMode.INSERT_ONLY) == "INSERT_ONLY"


def test_stream_type_enum():
    """Test StreamType enum values and string representation."""
    assert str(StreamType.DELTA) == "DELTA"
    assert str(StreamType.STANDARD) == "STANDARD"


# Edge Cases
def test_stream_with_special_characters():
    """Test stream creation with special characters in names and comments."""
    stream = (
        Stream.builder("TEST_STREAM_123")
        .with_source("TEST_TABLE")
        .with_comment("Test's comment with \"quotes\"")
        .build()
    )
    expected = (
        "CREATE STREAM TEST_STREAM_123 "
        "ON TABLE TEST_TABLE "
        "COMMENT = 'Test\\'s comment with \"quotes\"'"
    )
    assert stream.to_sql() == expected


def test_stream_with_empty_tags():
    """Test stream creation with empty tags dictionary."""
    stream = (
        Stream.builder("TEST_STREAM").with_source("TEST_TABLE").with_tags({}).build()
    )
    expected = "CREATE STREAM TEST_STREAM ON TABLE TEST_TABLE"
    assert stream.to_sql() == expected
