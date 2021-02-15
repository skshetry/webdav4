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
    assert fs.modified("/data/bar") == datetime.fromtimestamp(
        int(stat.st_mtime), tz=timezone.utc
    )
    assert fs.cat("/data/bar") == b"bar"

    checksum = fs.checksum("data/bar/")
    assert checksum and isinstance(checksum, str)

    fs.mv("data/bar", "data/foobar")
    assert fs.ls("data", detail=False) == ["data/foobar"]

    foobar_stat = (storage_dir / "data" / "foobar").stat()
    assert fs.created("data/foobar") == datetime.fromtimestamp(
        int(foobar_stat.st_ctime), tz=timezone.utc
    )

    fs.cp("data/foobar", "data/bar")
    assert set(fs.ls("data", detail=False)) == {"data/foobar", "data/bar"}

    fs.makedirs("data/subdir/subsubdir", exist_ok=True)
    assert fs.isdir("data/subdir/subsubdir")

    fs.mkdir("data/subdir2", create_parents=False)
    assert fs.isdir("data/subdir2")

    fs.mkdir("data/subdir2/subdir3/subdir4")
    assert fs.isdir("data/subdir2/subdir3/subdir4")


def test_open(storage_dir: TmpDir, server_address: URL, auth: Tuple[str, str]):
    """Test opening a remote file from webdav."""
    storage_dir.gen({"data": {"foo": "foo"}})

    fs = WebdavFileSystem(server_address, auth)

    with fs.open("/data/foo") as f:
        assert f.read() == b"foo"
        assert f.read() == b""
        assert f.read() == b""

    with fs.open("/data/foo", mode="r") as f:
        assert f.read() == "foo"
        assert f.read() == ""
        assert f.read() == ""

    stat = (storage_dir / "data" / "foo").stat()
    with fs.open("/data/foo", mode="r", size=stat.st_size) as f:
        assert f.read() == "foo"
        assert f.read() == ""
        assert f.read() == ""

    fobj = fs.open("/data/foo")

    assert not fobj.closed
    fobj.close()
    assert fobj.closed
    fobj.close()
    assert fobj.closed
