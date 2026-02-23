"""Test fs utils."""

from io import BytesIO

from webdav4.fs_utils import peek_filelike_length


def test_peek_filelike_length():
    """Test peek_filelike length for the fileobj."""
    fobj = BytesIO(b"Hello, World!")

    class ReadWrapper:
        """Wraps any given buffer."""

        def __init__(self, buff):
            """Wrap buff."""
            self.buff = buff

        def read(self, *args):
            """Wrap read method of the buff."""
            return self.buff.read(*args)

    assert peek_filelike_length(fobj) == 13
    assert peek_filelike_length(ReadWrapper(fobj)) is None  # type: ignore
    assert peek_filelike_length(object()) is None  # type: ignore
