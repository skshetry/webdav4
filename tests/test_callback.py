"""Test IO callback wrapper."""
from collections.abc import Iterable
from io import StringIO
from pathlib import Path
from typing import Union, no_type_check
from unittest.mock import MagicMock, call

import pytest

from webdav4.callback import CallbackIOWrapper, wrap_file_like


def test_wrap_file_like():
    """Test wrapper."""
    buff = StringIO()
    callback = MagicMock()
    result = wrap_file_like(buff, callback)
    assert isinstance(result, CallbackIOWrapper)


@no_type_check
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
    assert buff.add_something_here


@no_type_check
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
    assert write_buffer.add_something_here


class ReadWrapper:
    """Wraps any given buffer."""

    def __init__(self, buff):
        """Wrap buff."""
        self.buff = buff

    def read(self, *args):
        """Wrap read method of the buff."""
        return self.buff.read(*args)


@pytest.mark.parametrize("mode", ["r", "rb"])
@pytest.mark.parametrize("no_iter_implemented", [True, False])
@no_type_check
def test_callback_read_iter(
    tmp_path: Path, mode: str, no_iter_implemented: bool
):
    """Test __iter__ callbacks."""
    path = tmp_path / "file.txt"

    with path.open(mode="w", newline="\n", encoding="utf-8") as f:
        for _ in range(100):
            f.write("foo\n")

    callback = MagicMock()

    def decode(ch: Union[str, bytes]) -> Union[str, bytes]:
        return ch.decode("utf-8") if isinstance(ch, bytes) else ch

    with path.open(mode=mode) as f:
        wrapper = CallbackIOWrapper(
            ReadWrapper(f) if no_iter_implemented else f, callback
        )

        chunks = list(wrapper)
        assert ["foo\n" for _ in chunks] == list(map(decode, chunks))

    callback.assert_has_calls([call(4) for _ in chunks])


def test_callback_iter_non_read_method():
    """Error if __iter__() not implemented and we are in non-read mode."""

    class ReadWriteWrapper(ReadWrapper):
        """Wraps both read and write method of the buff."""

        def write(self, *args):
            """Wrap write method of the buff."""
            return self.buff.write(*args)  # pragma: no cover

    callback = MagicMock()

    f = ReadWriteWrapper(StringIO())  # type: ignore
    with pytest.raises(TypeError) as exc_info:
        wrapper = CallbackIOWrapper(
            f, callback, method="write"  # type: ignore
        )
        for _ in wrapper:
            pass
    assert str(exc_info.value) == "'ReadWriteWrapper' object is not iterable"


def test_callback_illegal_method():
    """Test that other methods except read/write are unsupported."""
    f = StringIO()
    callback = MagicMock()
    with pytest.raises(ValueError) as exc_info:
        CallbackIOWrapper(f, callback, method="unknown")  # type: ignore

    assert str(exc_info.value) == "Can only wrap read/write methods"
