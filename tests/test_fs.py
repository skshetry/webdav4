"""Testing fsspec based WebdavFileSystem."""

from typing import Tuple

from webdav4.fs import WebdavFileSystem
from webdav4.http import URL

from .utils import TmpDir


def test_fs_ls(
    storage_dir: TmpDir, server_address: "URL", auth: Tuple[str, str]
):
    """Tests fsspec for webdav."""
    fs = WebdavFileSystem(server_address, auth)
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
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
    assert fs.cat("/data/bar") == b"bar"
