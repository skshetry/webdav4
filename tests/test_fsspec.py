"""Testing fsspec based WebdavFileSystem."""

from datetime import datetime, timezone
from typing import Tuple

from webdav4.fsspec import WebdavFileSystem
from webdav4.urls import URL, join_url

from .utils import TmpDir


def test_fs_ls(
    storage_dir: TmpDir, server_address: "URL", auth: Tuple[str, str]
):
    """Tests fsspec for webdav."""
    fs = WebdavFileSystem(server_address, auth)

    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    stat = (storage_dir / "data").stat()
    assert fs.ls("/") == [
        {
            "size": None,
            "created": datetime.fromtimestamp(
                int(stat.st_ctime), tz=timezone.utc
            ),
            "modified": datetime.fromtimestamp(
                int(stat.st_mtime), tz=timezone.utc
            ),
            "content_language": None,
            "content_type": None,
            "etag": None,
            "type": "directory",
            "name": "data",
            "display_name": "data",
            "href": join_url(server_address, "data").path + "/",
        }
    ]
    foo_stat = (storage_dir / "data" / "foo").stat()
    bar_stat = (storage_dir / "data" / "bar").stat()

    info = fs.ls("/data/")
    assert len(info) == 2

    d = {}
    for i in info:
        assert not isinstance(i, str)
        i.pop("etag", None)
        d[i["name"]] = i

    assert d["data/bar"] == {
        "name": "data/bar",
        "href": join_url(server_address, "data/bar").path,
        "size": 3,
        "created": datetime.fromtimestamp(
            int(bar_stat.st_ctime), tz=timezone.utc
        ),
        "modified": datetime.fromtimestamp(
            int(bar_stat.st_ctime), tz=timezone.utc
        ),
        "content_language": None,
        "content_type": "application/octet-stream",
        "type": "file",
        "display_name": "bar",
    }
    assert d["data/foo"] == {
        "name": "data/foo",
        "href": join_url(server_address, "data/foo").path,
        "size": 3,
        "created": datetime.fromtimestamp(
            int(foo_stat.st_ctime), tz=timezone.utc
        ),
        "modified": datetime.fromtimestamp(
            int(foo_stat.st_ctime), tz=timezone.utc
        ),
        "display_name": "foo",
        "content_language": None,
        "content_type": "application/octet-stream",
        "type": "file",
    }

    fs.rm("/data/foo")
    assert fs.ls("/data/", detail=False) == ["data/bar"]
    assert fs.size("/data/bar") == 3
    assert fs.modified("/data/bar")
    assert fs.cat("/data/bar") == b"bar"
