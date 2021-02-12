"""Test IO callback wrapper."""
from collections.abc import Iterable
from io import StringIO
from os import linesep
from pathlib import Path
from typing import Union
from unittest.mock import MagicMock

import pytest

from webdav4.callback import CallbackIOWrapper, wrap_file_like


def test_wrap_file_like():
    """Test wrapper."""
    buff = StringIO()
    callback = MagicMock()
    result = wrap_file_like(buff, callback)
    assert isinstance(result, CallbackIOWrapper)


def test_callback_read():
    """Test reports read callback correctly."""
    line = "foo\n"
    buff = StringIO(line * 100)
    buff.seek(0)

    callback = MagicMock()

    wrapper = CallbackIOWrapper(buff, callback)
    assert isinstance(wrapper, Iterable)

    assert wrapper.read(20) == line * 5
    callback.assert_called_once_with(20)
    callback.reset_mock()

    assert wrapper.read(40) == line * 10
    callback.assert_called_once_with(40)
    callback.reset_mock()

    # getattr works
    assert wrapper.closed is False

    # adding to __dict__ works, and is being wrapped properly
    assert wrapper.__wrapped_stream__ is buff

    method = wrapper.write
    assert method.__self__ is buff and method == buff.write
    assert wrapper.read != buff.read
    assert wrapper.__call_back__ is callback

    # setattr works
    wrapper.add_something_here = True
    assert wrapper.add_something_here
    assert buff.add_something_here  # type: ignore


def test_callback_write():
    """Test reports write callback correctly."""
    line = "foo\n"
    read_buffer = StringIO(line * 100)
    read_buffer.seek(0)

    write_buffer = StringIO()
    callback = MagicMock()

    wrapper = CallbackIOWrapper(write_buffer, callback, method="write")
    assert isinstance(wrapper, Iterable)

    # write works
    assert wrapper.write(read_buffer.read(20)) == 20
    callback.assert_called_once_with(20)
    callback.reset_mock()

    assert wrapper.write(read_buffer.read(40)) == 40
    callback.assert_called_once_with(40)
    callback.reset_mock()

    # getattr works
    assert wrapper.closed is False

    # __dict__ overwrite works, wrapped function gets called
    assert wrapper.__wrapped_stream__ is write_buffer

    method = wrapper.read
    assert method.__self__ is write_buffer and method == write_buffer.read
    assert wrapper.write != write_buffer.write

    assert wrapper.__call_back__ is callback

    # setattr works
    wrapper.add_something_here = True
    assert wrapper.add_something_here
    assert write_buffer.add_something_here  # type: ignore


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_callback_read_iter(tmp_path: Path, mode: str):
    """Test __iter__ callbacks."""
    path = tmp_path / "file.txt"
    line = "foo" + linesep
    path.write_text(line * 100, "utf-8")

    callback = MagicMock()

    with path.open(mode=mode) as f:
        wrapper = CallbackIOWrapper(f, callback)

        expected: Union[str, bytes] = (
            line.encode("utf-8") if mode == "rb" else line
        )

        for chunk in wrapper:
            assert chunk == expected
            callback.assert_called_once_with(len(expected))
            callback.reset_mock()


def test_callback_illegal_method():
    """Test that other methods except read/write are unsupported."""
    f = StringIO()
    callback = MagicMock()
    with pytest.raises(ValueError) as exc_info:
        CallbackIOWrapper(f, callback, method="unknown")  # type: ignore

    assert str(exc_info.value) == "Can only wrap read/write methods"
