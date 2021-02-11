"""Tests for webdav client."""
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict
from unittest.mock import MagicMock

import pytest

from webdav4.client import (
    Client,
    CopyError,
    CreateCollectionError,
    MoveError,
    MultiStatusError,
    RemoveError,
    ResourceNotFound,
)
from webdav4.http import Client as HTTPClient
from webdav4.urls import URL

from .utils import TmpDir


def lock_resource(client: Client, path: str):
    """Exclusive lock on a resource."""
    url = client.join_url(path)
    d = f"""<?xml version="1.0" encoding="utf-8" ?>
     <d:lockinfo xmlns:d='DAV:'>
       <d:lockscope><d:exclusive/></d:lockscope>
       <d:locktype><d:write/></d:locktype>
       <d:owner><d:href>{url}</d:href></d:owner>
     </d:lockinfo>"""
    resp = client.http.lock(url, data=d)
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
    with pytest.raises(MoveError) as exc_info:
        client.move("data/foo", "data/bar")
    assert str(exc_info.value) == (
        "failed to move data/foo to data/bar - "
        "the destination URL already exists"
    )

    assert exc_info.value.from_path == "data/foo"
    assert exc_info.value.to_path == "data/bar"
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}


def test_move_collection_dest_exists_already(
    storage_dir: TmpDir, client: Client
):
    """Test moving a collection to a destination that already exists."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}, "data2": {}})
    with pytest.raises(MoveError) as exc_info:
        client.move("data", "data2")

    assert str(exc_info.value) == (
        "failed to move data to data2 - " "the destination URL already exists"
    )
    assert exc_info.value.from_path == "data"
    assert exc_info.value.to_path == "data2"
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

    with pytest.raises(MoveError) as exc_info:
        client.move(from_path, "data3/bar")

    assert str(exc_info.value) == (
        f"failed to move {from_path} to data3/bar - "
        "there was conflict when trying to move the resource"
    )
    assert exc_info.value.from_path == from_path
    assert exc_info.value.to_path == "data3/bar"
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

    with pytest.raises(MoveError) as exc_info:
        client.move(move_from, "data2")

    assert (
        str(exc_info.value) == f"failed to move {move_from} to data2 - "
        "the source or the destination resource is locked"
    )
    assert exc_info.value.from_path == move_from
    assert exc_info.value.to_path == "data2"

    # should not have been moved at all
    assert storage_dir.cat() == {
        "data": {"foo": "foo", "bar": "bar"},
        "data2": {"foobar": "foobar"},
    }


@pytest.mark.parametrize("method", ["copy", "move"])
def test_move_multistatus_failure(client: Client, method: str):
    """Test multistatus error in move and copy.

    Since it's hard to reproduce them with the current wsgidav, went with the
    xml raw response content to test with.
    """
    from httpx import Request, Response

    url = "http://www.example.com/container/"
    request = Request("move", url)
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
    exc_map = {
        "move": MoveError,
        "copy": CopyError,
    }
    with pytest.raises(exc_map[method]) as exc_info:
        func("/container", "/othercontainer")

    assert (
        str(exc_info.value)
        == f"failed to {method} /container to /othercontainer - "
        "{'/othercontainer/C2/': 'Locked'}"
    )
    assert exc_info.value.from_path == "/container"
    assert exc_info.value.to_path == "/othercontainer"


def test_mkdir(storage_dir: TmpDir, client: Client):
    """Test simple mkdir creation."""
    client.mkdir("data")
    assert storage_dir.cat() == {"data": {}}


def test_mkdir_but_parent_collection_not_exist(
    storage_dir: TmpDir, client: Client
):
    """Test creating a collection but parent collection does not exist."""
    with pytest.raises(CreateCollectionError) as exc_info:
        client.mkdir("data/sub")

    assert storage_dir.cat() == {}
    assert (
        str(exc_info.value) == "failed to create collection data/sub - "
        "parent of the collection does not exist"
    )
    assert exc_info.value.path == "data/sub"


def test_mkdir_collection_already_exists(storage_dir: TmpDir, client: Client):
    """Test trying to create an already-existing collection."""
    storage_dir.gen({"data": {"foo": "foo"}})
    with pytest.raises(CreateCollectionError) as exc_info:
        client.mkdir("data")

    assert storage_dir.cat() == {"data": {"foo": "foo"}}
    assert (
        str(exc_info.value) == "failed to create collection data - "
        "collection already exists"
    )
    assert exc_info.value.path == "data"


def test_makedirs(storage_dir: TmpDir, client: Client):
    """Test makedirs."""
    client.makedirs("/data/foo/bar")
    assert storage_dir.cat() == {"data": {"foo": {"bar": {}}}}

    with pytest.raises(CreateCollectionError) as exc_info:
        client.makedirs("/data/foo/bar")

    assert (
        str(exc_info.value) == "failed to create collection data - "
        "collection already exists"
    )
    assert exc_info.value.path == "data"

    client.makedirs("/data/foo/bar", exist_ok=True)


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

    with pytest.raises(RemoveError) as exc_info:
        client.remove("data/foo")

    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}
    assert (
        str(exc_info.value) == "failed to remove data/foo - "
        "the resource is locked"
    )
    assert exc_info.value.path == "data/foo"


def test_try_remove_locked_resource_coll(
    storage_dir: TmpDir, client: Client, server_address: URL
):
    """Test trying to remove a resource that is locked."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    lock_resource(client, "data")

    with pytest.raises(RemoveError) as exc_info:
        client.remove("data")

    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}

    statuses = {
        client.join_url("/data").path + "/": "Locked",
        client.join_url("/data/bar").path: "Locked",
        client.join_url("/data/foo").path: "Locked",
    }
    assert str(exc_info.value) == (
        "failed to remove data - "
        + "multiple errors received: "
        + str(statuses)
    )
    assert exc_info.value.path == "data"


def test_check_multistatus():
    """Test MultiStatusError string representation."""
    statuses = {"/data/bar": "Locked"}
    error = MultiStatusError(statuses)
    assert str(error) == str(statuses)

    statuses = {"/data/bar": "Locked", "http://example.org": "Bad Gateway"}
    error = MultiStatusError(statuses)
    assert str(error) == "multiple errors received: " + str(statuses)


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
    """Test opening a remote file from webdav."""
    storage_dir.gen({"data": {"foo": "foo"}})
    with client.open("/data/foo") as f:
        assert f.read() == "foo"

    with client.open("/data/foo", mode="rb") as f:
        assert f.read() == b"foo"


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


def test_upload_file(tmp_path: Path, storage_dir: TmpDir, client: Client):
    """Test downloading a remote resource to a local file."""
    file_path = tmp_path / "foo.txt"
    file_path.write_text("foo")
    client.upload_file(file_path, "foo")

    assert client.exists("/foo")
    assert client.isfile("/foo")
    assert not client.isdir("/foo")
    assert storage_dir.cat() == {"foo": "foo"}


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
    assert client.modified("/data/foo") == datetime.fromtimestamp(
        int(stat.st_mtime), tz=timezone.utc
    )
    assert client.created("/data/foo") == datetime.fromtimestamp(
        int(stat.st_ctime), tz=timezone.utc
    )


def test_attributes_dir(storage_dir: TmpDir, client: Client):
    """Test attributes response on a directory."""
    storage_dir.gen({"data": {"foo": "foo"}})

    stat = (storage_dir / "data").stat()
    assert client.content_length("/data/") is None
    assert client.content_type("/data/") == ""
    assert client.content_language("/data/") == ""
    etag = client.etag("/data/")
    assert etag == ""
    assert client.modified("/data/") == datetime.fromtimestamp(
        int(stat.st_mtime), tz=timezone.utc
    )
    assert client.created("/data/") == datetime.fromtimestamp(
        int(stat.st_ctime), tz=timezone.utc
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
