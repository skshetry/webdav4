"""Testing stream utilities."""
from io import DEFAULT_BUFFER_SIZE, BytesIO, StringIO
from typing import Any, Iterator

import pytest
from pytest import MonkeyPatch

from tests.utils import TmpDir
from webdav4.client import Client
from webdav4.fsspec import WebdavFileSystem
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


def test_retry_reconnect_on_failure(
    storage_dir: TmpDir,
    fs: WebdavFileSystem,
    client: Client,
    monkeypatch: MonkeyPatch,
):
    """Test retry/reconnect on network failures."""
    from unittest import mock

    from webdav4.http import HTTPNetworkError, HTTPResponse

    original_iter_content = HTTPResponse.iter_bytes

    def bad_iter_content(
        response: "HTTPResponse", *args: Any, **kwargs: Any
    ) -> Iterator[bytes]:
        """Simulate bad connection."""
        it = original_iter_content(response, *args, **kwargs)
        for i, chunk in enumerate(it):
            # Drop connection error on second chunk if there is one
            if i > 0:
                raise HTTPNetworkError(
                    "Simulated connection drop", request=response.request
                )
            yield chunk

    # Text should be longer than default chunk to test resume,
    # using twice of that plus something tests second resume,
    # this is important because second response is different
    text1 = "0123456789" * (client.chunk_size // 10 + 1)
    storage_dir.gen("sample.txt", text1 * 2)
    propfind_resp = client.propfind("sample.txt")

    monkeypatch.setattr(HTTPResponse, "iter_bytes", bad_iter_content)

    with mock.patch.object(client, "propfind", return_value=propfind_resp):
        with client.open("sample.txt") as fd:
            # Test various .read() variants
            assert fd.read(len(text1)) == text1
            assert fd.read() == text1
            assert fd.read() == ""

        with client.open("sample.txt", mode="rb") as fd:
            # Test various .read() variants
            assert fd.read(len(text1)) == text1.encode()
            assert fd.read() == text1.encode()
            assert fd.read() == b""

        with fs.open("sample.txt", mode="r") as fd:
            # Test various .read() variants
            assert fd.read(len(text1)) == text1
            assert fd.read() == text1
            assert fd.read() == ""

        with fs.open("sample.txt") as fd:
            # Test various .read() variants
            assert fd.read(len(text1)) == text1.encode()
            assert fd.read() == text1.encode()
            assert fd.read() == b""

        # when we cannot detect support for ranges, we should just raise error
        client.detected_features.supports_ranges = False
        with client.open("sample.txt", mode="rb") as fd:
            fd._response.headers.clear()  # type: ignore
            with pytest.raises(HTTPNetworkError):
                fd.read()

            assert fd.supports_ranges is False  # type: ignore
            with pytest.raises(ValueError) as exc_info:
                fd.seek(10)
            assert str(exc_info.value) == "server does not support ranges"

        with fs.open("sample.txt", mode="rb") as fd:
            fd.reader._response.headers.clear()  # type: ignore
            with pytest.raises(HTTPNetworkError):
                fd.read()

            assert fd.reader.supports_ranges is False  # type: ignore
            with pytest.raises(ValueError) as exc_info:
                fd.seek(10)
            assert str(exc_info.value) == "server does not support ranges"


def test_open(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test opening a remote file from webdav using fs in text mode."""
    text1 = "0123456789" * (DEFAULT_BUFFER_SIZE // 10 + 1)
    storage_dir.gen("sample.txt", text1 * 2)

    with fs.open("sample.txt", mode="r") as f:
        assert not f.closed
        assert not f.isatty()
        assert f.readable()
        assert not f.writable()
        assert f.seekable()
        assert f.tell() == 0
        assert f.read(len(text1)) == text1
        assert f.tell() == len(text1)
        assert f.seek(10) == 10
        assert f.read(len(text1) - 10) == text1[10:]
        assert f.tell() == len(text1)
        assert f.seek(len(text1) * 2) == len(text1) * 2
        assert f.read() == ""
        assert f.seek(0) == 0
    assert f.closed
    f.close()

    text2 = "0123456789\n"
    storage_dir.gen("sample2.txt", text2 * 10)

    with fs.open("sample2.txt", mode="r") as f:
        assert f.readline() == text2
        assert f.readlines() == [text2] * 9
    assert f.closed
    f.close()


def test_open_binary(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test file object in binary mode with fs."""
    text1 = b"0123456789" * (DEFAULT_BUFFER_SIZE // 10 + 1)
    storage_dir.gen("sample.txt", text1 * 2)
    with fs.open("sample.txt", mode="rb") as f:
        assert not f.closed
        assert not f.isatty()
        assert f.readable()
        assert not f.writable()
        assert f.seekable()
        assert f.tell() == 0

        assert f.read(len(text1)) == text1
        assert f.tell() == len(text1)

        assert f.seek(10) == 10
        assert f.read(len(text1) - 10) == text1[10:]
        assert f.tell() == len(text1)

        assert f.seek(len(text1), 1) == len(text1) * 2
        assert f.read() == b""
        assert f.seek(-len(text1), 1) == len(text1)

        assert f.seek(0) == 0
        buff = bytearray(5)
        assert f.readinto(buff) == 5
        assert bytes(buff) == text1[:5]
        length = f.readinto1(buff)
        assert bytes(buff) == text1[5 : 5 + length]
        assert f.seek(-10, 2) == f.tell() == len(text1) * 2 - 10
        assert f.read() == text1[-10:]
    assert f.closed
    f.close()

    with fs.open("sample.txt", mode="rb") as f:
        with pytest.raises(ValueError):
            f.seek(-10)
        with pytest.raises(ValueError):
            f.seek(10, 3)
    assert f.closed
    f.close()

    text2 = b"0123456789\n"
    storage_dir.gen("sample2.txt", text2 * 10)

    with fs.open("sample2.txt", mode="rb") as f:
        assert f.readline() == text2
        assert f.readlines() == [text2] * 9
    assert f.closed
    f.close()
