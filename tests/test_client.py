"""Tests for webdav client."""
from datetime import datetime, timezone
from http import HTTPStatus
from io import DEFAULT_BUFFER_SIZE, BytesIO
from pathlib import Path
from typing import Any, Callable, Dict
from unittest.mock import MagicMock, patch

import pytest

from webdav4.client import (
    BadGatewayError,
    Client,
    FeatureDetection,
    ForbiddenOperation,
    HTTPError,
    InsufficientStorage,
    IsACollectionError,
    MultiStatusError,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceLocked,
    ResourceNotFound,
)
from webdav4.http import Client as HTTPClient
from webdav4.http import Method as HTTPMethod
from webdav4.urls import URL

from .utils import TmpDir, approx_datetime


def lock_resource(client: Client, path: str):
    """Exclusive lock on a resource."""
    url = client.join_url(path)
    d = f"""<?xml version="1.0" encoding="utf-8" ?>
     <d:lockinfo xmlns:d='DAV:'>
       <d:lockscope><d:exclusive/></d:lockscope>
       <d:locktype><d:write/></d:locktype>
       <d:owner><d:href>{url}</d:href></d:owner>
     </d:lockinfo>"""
    resp = client.http.lock(url, content=d)
    resp.raise_for_status()


def test_init():
    """Try default initialization."""
    base_url = "http://example.org"
    auth = ("user", "password")

    c = Client(base_url=base_url, auth=auth)

    assert c.base_url, c.http.auth == (base_url, auth)


def test_init_pass_client():
    """Test passing a pre-build http client to a webdav client.

    If we test this, then it might be easier to mock in other tests.
    """
    base_url = "http://example.org/"
    auth = ("user", "password")

    http_client = HTTPClient(auth=auth)
    client = Client(base_url=base_url, http_client=http_client)

    assert client.http == http_client
    assert client.base_url == base_url


def test_get_property(storage_dir: TmpDir, client: Client):
    """Test getting a property from a resource."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    assert client.get_property("/data/foo", "content_length") == 3
    assert client.get_property("/data/", "content_length") is None
    assert client.get_property("/data/", "whatever") == ""


def test_copy_file(storage_dir: TmpDir, client: Client):
    """Test simple file copy."""
    storage_dir.gen({"data": {"foo": b"foo"}})
    client.copy("data/foo", "data/foobar")
    assert storage_dir.cat() == {"data": {"foo": "foo", "foobar": "foo"}}


def test_copy_collection(storage_dir: TmpDir, client: Client):
    """Test simple copy for collection."""
    dir_internals = {"foo": "foo", "sub": {"file": "file"}}
    storage_dir.gen({"data": dir_internals})
    client.copy("data", "data2")

    assert storage_dir.cat() == {"data": dir_internals, "data2": dir_internals}


def test_try_copy_file_when_destination_already_exists(
    storage_dir: TmpDir, client: Client
):
    """Try copying a resource to a destination that's already mapped/exists."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    with pytest.raises(ResourceAlreadyExists) as exc_info:
        client.copy("data/foo", "data/bar")

    assert str(exc_info.value) == "The resource data/bar already exists"
    assert exc_info.value.path == "data/bar"


def test_try_copy_resource_that_does_not_exist(
    storage_dir: TmpDir, client: Client
):
    """Try to copy a resource that does not exist."""
    with pytest.raises(ResourceNotFound) as exc_info:
        client.copy("data", "data2")

    assert storage_dir.cat() == {}
    assert str(exc_info.value) == (
        "The resource data could not be found in the server"
    )
    assert exc_info.value.path == "data"


@pytest.mark.parametrize("from_path", ["data", "data/foo"])
def test_try_copy_to_destination_parent_does_not_exist(
    storage_dir: TmpDir, client: Client, from_path: str
):
    """Try to copy a path to a destination whose parent does not exist yet."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})

    with pytest.raises(ResourceConflict) as exc_info:
        client.copy(from_path, "data3/bar")

    assert str(exc_info.value) == (
        "there was a conflict when trying to copy the resource"
    )
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}


def test_copy_overwrite_file(storage_dir: TmpDir, client: Client):
    """Test copy file with overwrite."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    client.copy("/data/foo", "data/bar/", overwrite=True)
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "foo"}}


def test_copy_collection_overwrite(storage_dir: TmpDir, client: Client):
    """Test copy a collection with overwrite."""
    dir_internals = {"foo": "foo", "sub": {"file": "file"}}
    storage_dir.gen({"data": dir_internals, "data2": {"foo": "foo"}})

    client.copy("/data", "/data2/", overwrite=True)
    assert storage_dir.cat() == {"data": dir_internals, "data2": dir_internals}


def test_move_file(storage_dir: TmpDir, client: Client):
    """Test simple file move."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    client.move("data/foo", "data/foobar")
    assert storage_dir.cat() == {"data": {"bar": "bar", "foobar": "foo"}}


def test_move_collection(storage_dir: TmpDir, client: Client):
    """Test simple collection move."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    client.move("data", "data2")
    assert storage_dir.cat() == {"data2": {"foo": "foo", "bar": "bar"}}


def test_try_move_resource_that_does_not_exist(
    storage_dir: TmpDir, client: Client
):
    """Test trying to move a resource that does not exist at all."""
    with pytest.raises(ResourceNotFound) as exc_info:
        client.move("data", "data2")

    assert storage_dir.cat() == {}
    assert str(exc_info.value) == (
        "The resource data could not be found in the server"
    )

    assert exc_info.value.path == "data"


def test_move_file_dest_exists_already(storage_dir: TmpDir, client: Client):
    """Test moving file to a destination that already exists."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    with pytest.raises(ResourceAlreadyExists) as exc_info:
        client.move("data/foo", "data/bar")
    assert str(exc_info.value) == ("The resource data/bar already exists")
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}


def test_move_collection_dest_exists_already(
    storage_dir: TmpDir, client: Client
):
    """Test moving a collection to a destination that already exists."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}, "data2": {}})
    with pytest.raises(ResourceAlreadyExists) as exc_info:
        client.move("data", "data2")

    assert str(exc_info.value) == ("The resource data2 already exists")
    assert storage_dir.cat() == {
        "data": {"foo": "foo", "bar": "bar"},
        "data2": {},
    }


def test_move_file_with_overwrite(storage_dir: TmpDir, client: Client):
    """Test moving a file to a dest. that already exists and overwrite."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    client.move("data/foo", "data/bar", overwrite=True)
    assert storage_dir.cat() == {"data": {"bar": "foo"}}


def test_move_collection_with_overwrite(storage_dir: TmpDir, client: Client):
    """Test moving a coll. to a dest. that already exists and overwrite."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}, "data2": {}})
    client.move("data", "data2", overwrite=True)
    assert storage_dir.cat() == {"data2": {"foo": "foo", "bar": "bar"}}


@pytest.mark.parametrize("from_path", ["data", "data/foo"])
def test_move_to_a_dest_whose_parent_does_not_exist(
    storage_dir: TmpDir, client: Client, from_path: str
):
    """Test moving a resource to a dest. whose parent don't exists yet."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})

    with pytest.raises(ResourceConflict) as exc_info:
        client.move(from_path, "data3/bar")

    assert str(exc_info.value) == (
        "there was a conflict when trying to move the resource"
    )
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}


@pytest.mark.parametrize(
    "lock_path, move_from",
    [
        # Lock a collection resource and try to move it.
        ("data", "data"),
        # Lock a non-collection resource and try to move it.
        ("data/foo", "data/foo"),
        # Lock a collection resource and try to move it's member resource.
        ("data", "data/foo"),
        # Lock a non-collection resource and try to move its parent collection.
        ("data/foo", "data"),
        # Lock a destination collection and try to move a collection into it.
        ("data2", "data"),
        # Lock a destination collection and try to move a non-collection to it.
        ("data2", "data/foo"),
        # Lock a destination non-collection and try to move a collection to it.
        ("data2/foobar", "data"),
        # Lock a destination collection and try to move a non-collection to it.
        ("data2/foobar", "data/foo"),
    ],
)
def test_try_moving_a_resource_locked(
    storage_dir: TmpDir,
    client: Client,
    move_from: str,
    lock_path: str,
):
    """Test trying to move a resource that's locked.

    (completely or partially, in src or dest)
    """
    storage_dir.gen(
        {"data": {"foo": "foo", "bar": "bar"}, "data2": {"foobar": "foobar"}}
    )
    lock_resource(client, lock_path)

    with pytest.raises(ResourceLocked) as exc_info:
        client.move(move_from, "data2")

    assert (
        str(exc_info.value)
        == "the source or the destination resource is locked"
    )

    # should not have been moved at all
    assert storage_dir.cat() == {
        "data": {"foo": "foo", "bar": "bar"},
        "data2": {"foobar": "foobar"},
    }


@pytest.mark.parametrize("method", ["copy", "move"])
def test_transfer_multistatus_failure(client: Client, method: str):
    """Test multistatus error in move and copy.

    Since it's hard to reproduce them with the current wsgidav, went with the
    xml raw response content to test with.
    """
    from httpx import Request, Response

    url = "http://www.example.com/container/"
    request = Request(HTTPMethod.MOVE, url)
    response = Response(
        status_code=207,
        request=request,
        content="""<?xml version="1.0" encoding="utf-8" ?>
        <d:multistatus xmlns:d='DAV:'>
            <d:response>
                <d:href>/othercontainer/C2/</d:href>
                <d:status>HTTP/1.1 423 Locked</d:status>
                <d:error><d:lock-token-submitted/></d:error>
            </d:response>
        </d:multistatus>""",
    )

    client.http.request = MagicMock(  # type: ignore[assignment]
        return_value=response
    )
    func = getattr(client, method)
    with pytest.raises(MultiStatusError) as exc_info:
        func("/container", "/othercontainer")

    assert str(exc_info.value) == "The resource /othercontainer/C2/ is locked"


@pytest.mark.parametrize("method", ["copy", "move"])
def test_transfer_forbidden_operations(
    client: Client, server_address: URL, method: str
):
    """Test that copy and move handle forbidden operations.

    This might happen due to number of issues, but mainly it's because
    the source and destination resource are the same.
    """
    from httpx import Request, Response

    url = server_address.join("container")
    request = Request(HTTPMethod.MKCOL, url)
    response = Response(status_code=403, request=request)

    client.http.request = MagicMock(  # type: ignore[assignment]
        return_value=response
    )

    func = getattr(client, method)
    with pytest.raises(ForbiddenOperation) as exc_info:
        func("container", "othercontainer")

    assert str(exc_info.value) == (
        "the source and the destination could be the same"
    )


def test_mkdir(storage_dir: TmpDir, client: Client):
    """Test simple mkdir creation."""
    client.mkdir("data")
    assert storage_dir.cat() == {"data": {}}


def test_mkdir_but_parent_collection_not_exist(
    storage_dir: TmpDir, client: Client
):
    """Test creating a collection but parent collection does not exist."""
    with pytest.raises(ResourceConflict) as exc_info:
        client.mkdir("data/sub")

    assert storage_dir.cat() == {}
    assert str(exc_info.value) == "parent of the collection does not exist"


def test_mkdir_collection_already_exists(storage_dir: TmpDir, client: Client):
    """Test trying to create an already-existing collection."""
    storage_dir.gen({"data": {"foo": "foo"}})
    with pytest.raises(ResourceAlreadyExists) as exc_info:
        client.mkdir("data")

    assert storage_dir.cat() == {"data": {"foo": "foo"}}
    assert str(exc_info.value) == "The resource data already exists"
    assert exc_info.value.path == "data"


def test_mkdir_forbidden_operations(client: Client, server_address: URL):
    """Test forbidden operations for mkdir.

    Notes:
        This indicates at least one of two conditions (from rfc4918):
        1) the server does not allow the creation of collections at the
            given location in its URL namespace, or
        2) the parent collection of the Request-URI exists but cannot accept
            members
    """
    from httpx import Request, Response

    url = server_address.join("container")
    request = Request(HTTPMethod.MKCOL, url)
    response = Response(status_code=403, request=request)

    client.http.request = MagicMock(  # type: ignore[assignment]
        return_value=response
    )
    with pytest.raises(ForbiddenOperation) as exc_info:
        client.mkdir("container")

    assert str(exc_info.value) == (
        "the server does not allow creation in the namespace"
        "or cannot accept members"
    )


def test_remove_collection(storage_dir: TmpDir, client: Client):
    """Test trying to remove a collection resource."""
    storage_dir.gen({"data": {"foo": "foo"}})
    client.remove("data")
    assert storage_dir.cat() == {}


def test_remove_non_collection(storage_dir: TmpDir, client: Client):
    """Test trying to remove a non-collection resource."""
    storage_dir.gen({"data": {"foo": "foo"}})
    client.remove("data/foo")
    assert storage_dir.cat() == {"data": {}}


def test_remove_not_existing_resource(client: Client):
    """Test trying to remove a resource that does not exist."""
    with pytest.raises(ResourceNotFound) as exc_info:
        client.remove("data")

    assert (
        str(exc_info.value)
        == "The resource data could not be found in the server"
    )
    assert exc_info.value.path == "data"


def test_try_remove_locked_resource_non_coll(
    storage_dir: TmpDir, client: Client
):
    """Test trying to remove a resource that is locked."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    lock_resource(client, "data/foo")

    with pytest.raises(ResourceLocked) as exc_info:
        client.remove("data/foo")

    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}
    assert str(exc_info.value) == "the resource is locked"


def test_try_remove_locked_resource_coll(
    storage_dir: TmpDir, client: Client, server_address: URL
):
    """Test trying to remove a resource that is locked."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    lock_resource(client, "data")

    with pytest.raises(MultiStatusError) as exc_info:
        client.remove("data")

    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}

    statuses = {
        client.join_url("/data").path + "/": "Locked",
        client.join_url("/data/bar").path: "Locked",
        client.join_url("/data/foo").path: "Locked",
    }
    assert str(exc_info.value) == (
        "multiple errors received: " + str(statuses)
    )


@pytest.mark.parametrize(
    "structure, path",
    [
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data"),
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data/foo"),
    ],
)
def test_client_propfind(
    structure: Dict[str, Any],
    path: str,
    storage_dir: "TmpDir",
    client: Client,
):
    """Test http client's propfind response."""
    storage_dir.gen(structure)
    client.ls(path, detail=False)


def test_open(storage_dir: TmpDir, client: Client):
    """Test opening a remote file from webdav opened in text mode."""
    text1 = "0123456789" * (DEFAULT_BUFFER_SIZE // 10 + 1)
    storage_dir.gen("sample.txt", text1 * 2)

    with client.open("sample.txt", mode="r") as f:
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

    with client.open("sample2.txt", mode="r") as f:
        assert f.readline() == text2
        assert f.readlines() == [text2] * 9
    assert f.closed
    f.close()


def test_open_binary(storage_dir: TmpDir, client: Client):
    """Test file object opened in binary mode with client."""
    text1 = b"0123456789" * (DEFAULT_BUFFER_SIZE // 10 + 1)
    storage_dir.gen("sample.txt", text1 * 2)
    with client.open("sample.txt", mode="rb") as f:
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

        assert f.readall() == text1  # type: ignore

        assert f.seek(0) == 0
        buff = bytearray(5)
        assert f.readinto(buff) == 5  # type: ignore
        assert bytes(buff) == text1[:5]
        length = f.readinto1(buff)  # type: ignore
        assert bytes(buff) == text1[5 : 5 + length]

        assert f.read1(10)  # type: ignore
        assert f.seek(-10, 2) == f.tell() == len(text1) * 2 - 10
        assert f.read() == text1[-10:]
    assert f.closed
    f.close()

    with client.open("sample.txt", mode="rb") as f:
        with pytest.raises(ValueError):
            f.seek(-10)
        with pytest.raises(ValueError):
            f.seek(10, 3)
        f.size = None  # type: ignore
        with pytest.raises(ValueError):
            f.seek(-10, 2)

    assert f.closed

    text2 = b"0123456789\n"
    storage_dir.gen("sample2.txt", text2 * 10)

    with client.open("sample2.txt", mode="rb") as f:
        assert f.readline() == text2
        assert f.readlines() == [text2] * 9
    assert f.closed
    f.close()


def test_feature_detection_util():
    """Test parsing of response in FeatureDetection util."""
    from httpx import Response

    fd = FeatureDetection()
    assert fd.supports_ranges is False
    assert not fd.dav_compliances

    response = Response(
        HTTPStatus.OK, headers={"DAV": "1,2,3", "Accept-Range": "bytes"}
    )
    fd = FeatureDetection(response)
    assert fd.supports_ranges is False
    assert fd.dav_compliances


def test_download_fobj(storage_dir: TmpDir, client: Client):
    """Test downloading a resource to a file object."""
    storage_dir.gen({"data": {"foo": "foo"}})
    buff = BytesIO()
    client.download_fileobj("/data/foo", buff)
    assert buff.getvalue() == b"foo"


def test_download_file(tmp_path: Path, storage_dir: TmpDir, client: Client):
    """Test downloading a remote resource to a local file."""
    storage_dir.gen({"data": {"foo": "foo"}})
    file_path = tmp_path / "foo.txt"
    client.download_file("/data/foo", file_path)
    assert file_path.read_text()


def test_try_download_directory(
    tmp_path: Path, storage_dir: TmpDir, client: Client
):
    """Test downloading a remote resource to a local file."""
    storage_dir.gen({"data": {"foo": "foo"}})

    with pytest.raises(IsACollectionError) as exc_info:
        client.download_file("/data", tmp_path / "foo")

    assert (
        str(exc_info.value)
        == "/data is a collection. Cannot open a collection"
    )


def test_try_downloading_not_existing_resource(tmp_path: Path, client: Client):
    """Test trying to download a resource that does not exist at all."""
    file_path = tmp_path / "foo"
    with pytest.raises(ResourceNotFound) as exc_info:
        client.download_file("foo", file_path)

    assert (
        str(exc_info.value)
        == "The resource foo could not be found in the server"
    )


def test_open_file(storage_dir: TmpDir, client: Client):
    """Testing opening a remote url to a file-like object."""
    storage_dir.gen("foo", "foo")
    with client.open("foo") as f:
        assert f.read() == "foo"
        assert f.read() == ""
        assert f.read() == ""
        assert f.read(0) == ""

    with client.open("foo", mode="rb") as f:
        assert f.read() == b"foo"
        assert f.read() == b""
        assert f.read() == b""
        assert f.read(0) == b""

    with client.open("foo", mode="rb") as f:
        assert f.readall() == b"foo"  # type: ignore

    with client.open("foo", mode="rb") as f:
        b = bytearray(range(10))
        assert f.readinto(b) == 3  # type: ignore
        assert b[:3] == bytearray(b"foo")


def test_raising_insufficient_storage():
    """Test that insufficient storage is raised."""
    from httpx import Request, Response

    client = Client("https://example.org")
    url = client.join_url("test")
    req = Request(HTTPMethod.COPY, url)
    resp = Response(status_code=507, request=req)

    client.http.request = MagicMock(  # type: ignore[assignment]
        return_value=resp
    )
    with pytest.raises(InsufficientStorage) as exc_info:
        client.request("copy", "test")

    assert str(exc_info.value) == "Insufficient Storage on the server"


def test_upload_fobj(storage_dir: TmpDir, client: Client):
    """Test uploading a resource from a file object."""
    buff = BytesIO()
    buff.write(b"foo")

    # to decide size of file in bytesio, we need to seek to the start.
    # otherwise, it'll fail to determine size of file object
    # which will then try using chunked upload which is not supported by
    # cheroot server which we use for testing.
    buff.seek(0)

    client.upload_fileobj(buff, "foo")

    assert client.exists("/foo")
    assert client.isfile("/foo")
    assert not client.isdir("/foo")
    assert storage_dir.cat() == {"foo": "foo"}


def test_upload_fobj_size_hints(client: Client):
    """Test uploading file object with provided size as hints.

    This is used to choose between chunked and non-chunked transfers.
    """
    from .test_callback import ReadWrapper

    with patch.object(client, "request") as m:
        client.upload_fileobj(BytesIO(b"foobar"), "foobar", size=3)
        assert m.call_args[1]["headers"] == {"Content-Length": "3"}

        client.upload_fileobj(BytesIO(b"foobar"), "foobar")
        assert m.call_args[1]["headers"] == {"Content-Length": "6"}

        wrapped = ReadWrapper(BytesIO(b"foobar"))  # type: ignore
        client.upload_fileobj(wrapped, "foobar")  # type: ignore
        assert m.call_args[1]["headers"] is None

        wrapped = ReadWrapper(BytesIO(b"foobar"))  # type: ignore
        client.upload_fileobj(wrapped, "foobar", size=6)  # type: ignore
        assert m.call_args[1]["headers"] == {"Content-Length": "6"}


def test_upload_file(tmp_path: Path, storage_dir: TmpDir, client: Client):
    """Test uploading a local file to a remote."""
    file_path = tmp_path / "foo.txt"
    file_path.write_text("foo")
    client.upload_file(file_path, "foo")

    assert client.exists("/foo")
    assert client.isfile("/foo")
    assert not client.isdir("/foo")
    assert storage_dir.cat() == {"foo": "foo"}


def test_try_upload_file_that_already_exists(
    tmp_path: Path, storage_dir: TmpDir, client: Client
):
    """Test uploading a local file to a remote path that is already mapped.."""
    storage_dir.gen({"foo": "foo"})

    file_path = tmp_path / "foobar"
    file_path.write_text("foobar")

    with pytest.raises(ResourceAlreadyExists) as exc_info:
        client.upload_file(file_path, "foo")

    assert str(exc_info.value) == "The resource foo already exists"


def test_upload_file_with_overwrite(
    tmp_path: Path, storage_dir: TmpDir, client: Client
):
    """Test overwriting an already existing file when uploading."""
    storage_dir.gen({"foo": "foo"})

    file_path = tmp_path / "foobar"
    file_path.write_text("foobar")

    client.upload_file(file_path, "foo", overwrite=True)
    assert storage_dir.cat() == {"foo": "foobar"}


def test_isdir_isfile(storage_dir: TmpDir, client: Client):
    """Test isdir and isfile checks."""
    storage_dir.gen({"data": {"foo": "foo"}})

    assert client.isdir("/data/")
    assert not client.isfile("/data/")

    assert client.isfile("/data/foo")
    assert not client.isdir("/data/foo")

    with pytest.raises(ResourceNotFound) as exc_info:
        client.isdir("/data/file")

    assert exc_info.value.path == "/data/file"
    assert (
        str(exc_info.value)
        == "The resource /data/file could not be found in the server"
    )

    with pytest.raises(ResourceNotFound) as exc_info:
        client.isdir("/data/file")

    assert exc_info.value.path == "/data/file"
    assert (
        str(exc_info.value)
        == "The resource /data/file could not be found in the server"
    )


def test_exists(storage_dir: TmpDir, client: Client):
    """Test exists."""
    storage_dir.gen({"data": {"foo": "foo"}})

    assert client.exists("/data/")
    assert client.exists("/data/foo")
    assert not client.exists("/data/bar")


def test_attributes(storage_dir: TmpDir, client: Client):
    """Test APIs that provides attributes of the resource."""
    storage_dir.gen({"data": {"foo": "foo"}})

    stat = (storage_dir / "data" / "foo").stat()
    assert client.content_length("/data/foo") == 3
    assert client.content_type("/data/foo") == "application/octet-stream"
    assert client.content_language("/data/foo") == ""
    etag = client.etag("/data/foo")
    assert etag and isinstance(etag, str)
    assert client.modified("/data/foo") == approx_datetime(
        datetime.fromtimestamp(int(stat.st_mtime), tz=timezone.utc)
    )
    assert client.created("/data/foo") == approx_datetime(
        datetime.fromtimestamp(int(stat.st_ctime), tz=timezone.utc)
    )


def test_attributes_dir(storage_dir: TmpDir, client: Client):
    """Test attributes response on a directory."""
    storage_dir.gen({"data": {"foo": "foo"}})

    stat = (storage_dir / "data").stat()
    assert client.content_length("/data/") is None
    assert client.content_type("/data/") == ""
    assert client.content_language("/data/") == ""
    assert client.etag("/data/") is None
    assert client.modified("/data/") == approx_datetime(
        datetime.fromtimestamp(int(stat.st_mtime), tz=timezone.utc)
    )
    assert client.created("/data/") == approx_datetime(
        datetime.fromtimestamp(int(stat.st_ctime), tz=timezone.utc)
    )


def test_attributes_file_not_exist(client: Client):
    """Test attributes' API throws exception if the file does not exist."""

    def assert_raises(func: Callable[[str], Any]) -> None:
        with pytest.raises(ResourceNotFound):
            func("data")

    assert_raises(client.content_length)
    assert_raises(client.content_type)
    assert_raises(client.content_language)
    assert_raises(client.etag)
    assert_raises(client.modified)
    assert_raises(client.created)


def test_client_bad_gateway_error(client: Client, server_address: URL):
    """Test bad gateway errors.

    This are raised mostly on proxy errors. But on webdav, it might
    also happen if the destination for the `COPY` or `MOVE` operations
    are in remote server and the remote server refused the request or
    is unavailable to address the request.
    """
    from httpx import Request, Response

    url = server_address.join("container")
    request = Request(HTTPMethod.PROPFIND, url)
    response = Response(status_code=502, request=request)

    client.http.request = MagicMock(  # type: ignore[assignment]
        return_value=response
    )
    with pytest.raises(BadGatewayError) as exc_info:
        client.request(HTTPMethod.PROPFIND, "/container")

    assert (
        str(exc_info.value) == "The destination server may have refused"
        " to accept the resource"
    )


def test_options(client: Client):
    """Test features of the webdav server.

    Note that, right now, we are only testing with `WsgiDav` server.
    If this changes, we will need to figure out how to change this
    expectation here.
    """
    assert client.options() == {"1", "2"}


def test_feature_detection(client: Client):
    """Test feature detection in the Client."""
    assert client.detected_features
    assert client._detected_features  # pylint: disable=protected-access
    assert client.detected_features.dav_compliances

    client._detected_features = None  # pylint: disable=protected-access
    # should not fail at all even if auth is invalid
    client.http.auth = ("invalid", "invalid")  # type: ignore
    assert client.detected_features.dav_compliances
    assert client.detected_features


def test_auth_errors(server_address: URL):
    """Test that auth errors are raised as http error.

    This test serves another purpose as well: it is testing that
    unhandled exceptions are re-raising `HTTPError`.
    """
    client = Client(server_address, ("wrong", "password"))
    expected = "received 401 (Not Authorized)"
    path = "resource-that-does-not-exist"
    with pytest.raises(HTTPError) as exc_info:
        client.get_props(path)
    assert str(exc_info.value) == expected

    with pytest.raises(HTTPError) as exc_info:
        client.propfind(path)
    assert str(exc_info.value) == expected

    with pytest.raises(HTTPError) as exc_info:
        client.mkdir(path)
    assert str(exc_info.value) == expected

    with pytest.raises(HTTPError) as exc_info:
        client.remove(path)
    assert str(exc_info.value) == expected

    with pytest.raises(HTTPError) as exc_info:
        client.move(path, "resource2")
    assert str(exc_info.value) == expected

    with pytest.raises(HTTPError) as exc_info:
        client.copy(path, "resource2")
    assert str(exc_info.value) == expected


def test_client_retries(client: Client, server_address: URL):
    """Test that the client retries."""
    from httpx import Request, Response

    url = server_address.join("container1")
    request = Request(HTTPMethod.PROPFIND, url)
    failed_response = Response(status_code=502, request=request)
    success_response = Response(
        status_code=HTTPStatus.OK.value, request=request
    )

    client.http.request = func = MagicMock(  # type: ignore[assignment]
        side_effect=[failed_response, failed_response, success_response]
    )
    client.copy("/container1", "/container2")
    assert func.call_count == 3
