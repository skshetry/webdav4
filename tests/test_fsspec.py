"""Testing fsspec based WebdavFileSystem."""

from datetime import datetime, timezone

import pytest

from webdav4.fsspec import WebdavFileSystem
from webdav4.urls import URL, join_url

from .utils import TmpDir


def test_fs_ls(storage_dir: TmpDir, fs: WebdavFileSystem, server_address: URL):
    """Tests fsspec for webdav."""
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

    with pytest.raises(FileNotFoundError):
        fs.ls("data/not-existing-file")


def test_open(storage_dir: TmpDir, fs: WebdavFileSystem, server_address: URL):
    """Test opening a remote file from webdav."""
    storage_dir.gen({"data": {"foo": "foo"}})

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

    fs.put_file(storage_dir / "data" / "foo", "dir/somewhere/data2")
    assert fs.cat("dir/somewhere/data2") == b"foo"

    fs.put_file(storage_dir / "data", "dir/somewhere2")
    assert fs.isdir("dir/somewhere2")

    fs.pipe_file("dir/somewhere2/foo", b"foo")
    assert fs.cat("dir/somewhere2/foo") == b"foo"

    fs.touch("dir/file")
    assert fs.cat("dir/file") == b""

    fs.touch("dir/somewhere2/foo")
    assert fs.cat("dir/somewhere2/foo") == b""


def test_exists(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that exists complies with fsspec."""
    storage_dir.gen(
        {"data": {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}}
    )

    assert fs.exists("data")
    assert fs.exists("data/foo")
    assert fs.exists("data/bar")
    assert fs.exists("data/baz")
    assert fs.exists("data/baz/foobaz")
    assert not fs.exists("data2")
    assert not fs.exists("data/bazz")


def test_isdir(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that isdir complies with fsspec."""
    storage_dir.gen(
        {"data": {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}}
    )

    assert fs.isdir("data")
    assert not fs.isdir("data/foo")
    assert not fs.isdir("data/bar")
    assert fs.isdir("data/baz")
    assert not fs.isdir("data/baz/foobaz")

    # not existing ones should return False, instead of just failing
    assert not fs.isdir("data2")
    assert not fs.isdir("data/bazz")


def test_isfile(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that isfile complies with fsspec."""
    storage_dir.gen(
        {"data": {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}}
    )

    assert not fs.isfile("data")
    assert fs.isfile("data/foo")
    assert fs.isfile("data/bar")
    assert not fs.isfile("data/baz")
    assert fs.isfile("data/baz/foobaz")

    # not existing ones should return False, instead of just failing
    assert not fs.isfile("data2")
    assert not fs.isfile("data/bazz")


def test_created(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that created complies with fsspec."""
    with pytest.raises(FileNotFoundError):
        fs.created("not-existing-file")

    storage_dir.gen({"data": {"foo": "foo"}})

    data_stat = (storage_dir / "data" / "foo").stat()
    assert fs.created("data") == datetime.fromtimestamp(
        int(data_stat.st_ctime), tz=timezone.utc
    )

    foo_stat = (storage_dir / "data" / "foo").stat()
    assert fs.created("data/foo") == datetime.fromtimestamp(
        int(foo_stat.st_ctime), tz=timezone.utc
    )


def test_modified(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that modified complies with fsspec."""
    with pytest.raises(FileNotFoundError):
        fs.created("not-existing-file")

    storage_dir.gen({"data": {"foo": "foo"}})

    data_stat = (storage_dir / "data" / "foo").stat()
    assert fs.modified("data") == datetime.fromtimestamp(
        int(data_stat.st_mtime), tz=timezone.utc
    )

    foo_stat = (storage_dir / "data" / "foo").stat()
    assert fs.modified("data/foo") == datetime.fromtimestamp(
        int(foo_stat.st_mtime), tz=timezone.utc
    )


def test_checksum(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that checksum complies with fsspec."""
    with pytest.raises(FileNotFoundError):
        fs.created("not-existing-file")

    storage_dir.gen({"data": {"foo": "foo"}})
    assert fs.checksum("data") is None  # is a directory
    assert fs.checksum("data/foo")


def test_size(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that size complies with fsspec."""
    with pytest.raises(FileNotFoundError):
        fs.created("not-existing-file")

    storage_dir.gen({"data": {"foo": "foo"}})
    assert fs.size("data") is None  # is a directory
    assert fs.size("data/foo") == 3


def test_disk_usage(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that du complies with fsspec."""
    with pytest.raises(FileNotFoundError):
        fs.created("not-existing-file")

    storage_dir.gen({"data": {"foo": "foo"}})
    assert fs.du("data") == 3
    assert fs.du("data/foo") == 3
