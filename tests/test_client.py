"""Tests for webdav client."""

import pytest

from webdav4.client import Client, MoveError
from webdav4.http import URL
from webdav4.http import Client as HTTPClient

from .utils import TmpDir


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
    base_url = "http://example.org"
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


def test_try_move_resource_that_does_not_exist(client: Client):
    """Test trying to move a resource that does not exist at all."""
    with pytest.raises(MoveError) as exc_info:
        client.move("data", "data2")

    assert str(exc_info.value) == (
        "failed to move data to data2 - the resource could not be found"
    )

    assert exc_info.value.status_code == 404
    assert exc_info.value.from_path == "data"
    assert exc_info.value.to_path == "data2"


def test_move_file_dest_exists_already(storage_dir: TmpDir, client: Client):
    """Test moving file to a destination that already exists."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    with pytest.raises(MoveError) as exc_info:
        client.move("data/foo", "data/bar")
    assert str(exc_info.value) == (
        "failed to move data/foo to data/bar - "
        "the destination URL already exists"
    )

    assert exc_info.value.status_code == 412
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
    assert exc_info.value.status_code == 412
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
    assert exc_info.value.status_code == 409
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
    server_address: URL,
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
    url = server_address.join(lock_path)
    d = f"""<?xml version="1.0" encoding="utf-8" ?>
     <d:lockinfo xmlns:d='DAV:'>
       <d:lockscope><d:exclusive/></d:lockscope>
       <d:locktype><d:write/></d:locktype>
       <d:owner><d:href>{url}</d:href></d:owner>
     </d:lockinfo>"""
    resp = client.http.lock(url, data=d)
    resp.raise_for_status()

    with pytest.raises(MoveError) as exc_info:
        client.move(move_from, "data2")

    assert (
        str(exc_info.value) == f"failed to move {move_from} to data2 - "
        "the source or the destination resource is locked"
    )
    assert exc_info.value.status_code == 423
    assert exc_info.value.from_path == move_from
    assert exc_info.value.to_path == "data2"

    # should not have been moved at all
    assert storage_dir.cat() == {
        "data": {"foo": "foo", "bar": "bar"},
        "data2": {"foobar": "foobar"},
    }


@pytest.mark.parametrize(
    "structure, path",
    [
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data"),
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data/foo"),
    ],
)
def test_client_propfind(
    structure,
    path,
    storage_dir: "TmpDir",
    client: "Client",
):
    """Test http client's propfind response."""
    storage_dir.gen(structure)
    print(client.ls(path, detail=False))
