"""Tests for webdav client."""
from pathlib import Path
from typing import Tuple

import pytest
from httpx import URL

from webdav4.fs import WebdavFileSystem
from webdav4.http import Client


def fs_gen(root: Path, struct, text=""):
    """Creates folder structure locally from the provided structure.

    Args:
        root: root of the folder
        struct: the structure to create, can be a dict or a str.
            Dictionary can be nested, which it will create a directory.
            If it's a string, a file with `text` is created.
        text: optional, only necessary if struct is passed a string.
    """
    if isinstance(struct, (str, bytes, Path)):
        struct = {struct: text}
    for name, contents in struct.items():
        path = root / name

        if isinstance(contents, dict):
            if not contents:
                path.mkdir(parents=True, exist_ok=True)
            else:
                fs_gen(path, contents)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(contents, bytes):
                path.write_bytes(contents)
            else:
                path.write_text(contents, encoding="utf-8")
    return struct.keys()


def fs_cat(path: Path):
    """Returns (potentially multiple) paths' contents.

    Returns:
        a dict of {path: contents} if the path is a directory,
        otherwise the path contents is returned.
    """
    if path.is_dir():
        return {path.name: fs_cat(path) for path in path.iterdir()}
    return path.read_text()


@pytest.mark.parametrize(
    "structure, path, success",
    [
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data", True),
        ({"data": {"foo": "foo", "bar": "bar"}}, "/data/foo", True),
        ({"data": {"bar": "bar"}}, "/data/foo", False),
        ({"data": {"bar": "bar"}}, "/not-existing", False),
    ],
)
def test_client_propfind(
    structure,
    path,
    success,
    storage_dir: "Path",
    http_client: "Client",
    server_address: "URL",
):
    """Test http client's propfind response."""
    fs_gen(storage_dir, structure)
    resp = http_client.propfind(server_address.join(path))
    assert resp.is_error != success


def test_fs_ls(
    storage_dir: "Path", server_address: "URL", auth: Tuple[str, str]
):
    """Tests fsspec for webdav."""
    fs = WebdavFileSystem(server_address, auth)
    fs_gen(storage_dir, {"data": {"foo": "foo", "bar": "bar"}})
    assert fs.ls("/") == [
        {"name": "/data/", "size": None, "type": "directory"},
    ]
    assert fs.ls("/data/") == [
        {"name": "/data/bar", "size": 3, "type": "file"},
        {"name": "/data/foo", "size": 3, "type": "file"},
    ]
    fs.rm("/data/foo")
    assert fs.ls("/data/", detail=False) == ["/data/bar"]
    assert fs.size("/data/bar") == 3
    assert fs.modified("/data/bar")
