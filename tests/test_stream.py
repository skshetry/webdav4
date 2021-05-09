"""Testing stream utilities."""

from io import BytesIO, StringIO

import pytest

from webdav4.stream import read_until


def test_read_until_non_binary():
    """Test read_until with non-binary obj."""
    buff = StringIO()
    buff.write("hello world\n" * 100)
    buff.seek(0)

    assert list(read_until(buff, "\n")) == ["hello world\n"] * 100


def test_read_until_binary():
    """Test read_until with binary obj."""
    buff = BytesIO()
    buff.write(b"hello world\n" * 100)
    buff.seek(0)

    assert list(read_until(buff, "\n")) == [b"hello world\n"] * 100


@pytest.mark.parametrize("buff", [StringIO(), BytesIO()])
def test_read_until_empty(buff):
    """Test read_until with empty data."""
    assert list(read_until(buff, "\n")) == []


def test_read_until_not_found_any_match():
    """Test read_until, with no matching character."""
    d = "hello world" * 100
    buff = StringIO(d)

    assert list(read_until(buff, "\n")) == [d]
