"""Test fs utils."""
from io import BytesIO

from webdav4.fs_utils import peek_filelike_length

from .test_callback import ReadWrapper


def test_peek_filelike_length():
    """Test peek_filelike length for the fileobj."""
    fobj = BytesIO(b"Hello, World!")

    assert peek_filelike_length(fobj) == 13
    assert peek_filelike_length(ReadWrapper(fobj)) is None  # type: ignore
    assert peek_filelike_length(object()) is None  # type: ignore
