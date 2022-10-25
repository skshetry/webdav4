"""Testing fsspec based WebdavFileSystem."""
import errno
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple, Union

import fsspec
import pytest

from webdav4.fsspec import WebdavFileSystem
from webdav4.urls import URL, join_url

from .test_callback import ReadWrapper
from .utils import TmpDir, approx_datetime


def test_fs_ls(storage_dir: TmpDir, fs: WebdavFileSystem, server_address: URL):
    """Tests fsspec for webdav."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    stat = (storage_dir / "data").stat()

    assert fs.ls("/") == [
        {
            "size": None,
            "created": approx_datetime(
                datetime.fromtimestamp(int(stat.st_ctime), tz=timezone.utc)
            ),
            "modified": approx_datetime(
                datetime.fromtimestamp(int(stat.st_mtime), tz=timezone.utc)
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
        "created": approx_datetime(
            datetime.fromtimestamp(int(bar_stat.st_ctime), tz=timezone.utc)
        ),
        "modified": approx_datetime(
            datetime.fromtimestamp(int(bar_stat.st_ctime), tz=timezone.utc)
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
        "created": approx_datetime(
            datetime.fromtimestamp(int(foo_stat.st_ctime), tz=timezone.utc)
        ),
        "modified": approx_datetime(
            datetime.fromtimestamp(int(foo_stat.st_ctime), tz=timezone.utc)
        ),
        "display_name": "foo",
        "content_language": None,
        "content_type": "application/octet-stream",
        "type": "file",
    }

    fs.rm("/data/foo")
    assert fs.ls("/data/", detail=False) == ["data/bar"]
    assert fs.size("/data/bar") == 3
    assert fs.modified("/data/bar") == approx_datetime(
        datetime.fromtimestamp(int(stat.st_mtime), tz=timezone.utc)
    )
    assert fs.cat("/data/bar") == b"bar"

    checksum = fs.checksum("data/bar/")
    assert checksum and isinstance(checksum, str)

    fs.mv("data/bar", "data/foobar")
    assert fs.ls("data", detail=False) == ["data/foobar"]

    foobar_stat = (storage_dir / "data" / "foobar").stat()
    assert fs.created("data/foobar") == approx_datetime(
        datetime.fromtimestamp(int(foobar_stat.st_ctime), tz=timezone.utc)
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

    with pytest.raises(IsADirectoryError):
        fs.open("data")

    with pytest.raises(FileNotFoundError):
        fs.open("data/bar")

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


def test_open_write_bytes_simple(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test simple functions from fileobj in wb mode."""
    file = fs.open("foo", "wb")
    assert not file.closed

    with pytest.raises(ValueError):
        file.info()

    assert file.fileno()
    assert not file.isatty()
    assert file.readable()
    assert file.writable()
    assert file.seekable()

    file.close()
    assert file.closed

    assert storage_dir.cat() == {"foo": ""}


def test_open_write_bytes_write(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test write functions from fileobj in wb mode."""
    with fs.open("foo", "wb") as f:
        assert f.write(b"foo\n") == 4
        assert f.write(b"bar\n") == 4
        assert f.write(b"foobar\n") == 7
        f.writelines([b"lorem\n", b"ipsum\n"])

        assert f.tell() == 27
        f.flush()

    assert storage_dir.cat() == {"foo": "foo\nbar\nfoobar\nlorem\nipsum\n"}

    with fs.open("foo", "wb") as f:
        assert f.write(b"foo\n") == 4
        f.truncate(3)
    assert storage_dir.cat() == {"foo": "foo"}


def test_open_write_bytes_read(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test read functions from fileobj in wb mode."""
    with fs.open("foo", "wb") as f:
        assert f.write(b"foo\n") == 4
        assert f.write(b"bar\n") == 4
        assert f.write(b"foobar\n") == 7
        f.seek(0)
        assert f.read() == b"foo\nbar\nfoobar\n"
        f.seek(10)
        assert f.read(2) == b"ob"
        assert f.read() == b"ar\n"
        f.seek(0)
        assert f.readlines() == [b"foo\n", b"bar\n", b"foobar\n"]
        f.seek(0)
        assert f.readline() == b"foo\n"
        assert f.readuntil(b"o") == b"bar\nfo"
        b = bytearray(5)
        f.readinto(b)
        assert bytes(b) == b"obar\n"
        assert f.tell() == 15

    assert storage_dir.cat() == {"foo": "foo\nbar\nfoobar\n"}


def test_open_write_bytes_commit_discard(
    storage_dir: TmpDir, fs: WebdavFileSystem
):
    """Test commit and discard functions from fileobj in wb mode."""
    with fs.open("foo", "wb") as f:
        f.write(b"hello")
        f.discard()

    assert storage_dir.cat() == {}

    with fs.open("foo", "wb") as f:
        f.write(b"foo")
        f.commit()

    f.discard()
    f.close()
    assert storage_dir.cat() == {"foo": "foo"}


def test_open_write_bytes_x_mode(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test opening file in x mode."""
    with fs.open("foo", "x") as f:
        f.write("foo")

    assert storage_dir.cat() == {"foo": "foo"}

    with pytest.raises(FileExistsError):
        fs.open("foo", "x")


def test_open_write_in_text_mode(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test few methods from fileobj opened in text mode."""
    with fs.open("foo", "w") as f:
        f.write("foobar")
        f.writelines(["lorem"])

        pos = f.tell()
        f.seek(0)
        assert f.read() == "foobarlorem"
        f.seek(pos)
        f.flush()

    assert storage_dir.cat() == {"foo": "foobarlorem"}


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
    assert fs.created("data") == approx_datetime(
        datetime.fromtimestamp(int(data_stat.st_ctime), tz=timezone.utc)
    )

    foo_stat = (storage_dir / "data" / "foo").stat()
    assert fs.created("data/foo") == approx_datetime(
        datetime.fromtimestamp(int(foo_stat.st_ctime), tz=timezone.utc)
    )


def test_modified(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that modified complies with fsspec."""
    with pytest.raises(FileNotFoundError):
        fs.created("not-existing-file")

    storage_dir.gen({"data": {"foo": "foo"}})

    data_stat = (storage_dir / "data" / "foo").stat()
    assert fs.modified("data") == approx_datetime(
        datetime.fromtimestamp(int(data_stat.st_mtime), tz=timezone.utc)
    )

    foo_stat = (storage_dir / "data" / "foo").stat()
    assert fs.modified("data/foo") == approx_datetime(
        datetime.fromtimestamp(int(foo_stat.st_mtime), tz=timezone.utc)
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


@pytest.mark.parametrize("detail", [False, True])
def test_ls(storage_dir: TmpDir, fs: WebdavFileSystem, detail: bool):
    """Test that ls is compliance with fsspec."""
    assert fs.ls("", detail=detail) == []

    with pytest.raises(FileNotFoundError):
        fs.ls("not-existing")

    storage_dir.gen({"empty_dir": {}})
    assert fs.ls("empty_dir", detail=detail) == []

    # try ls file
    storage_dir.gen({"foo": "foo"})
    with pytest.raises(NotADirectoryError):
        fs.ls("foo", detail=detail)

    def get_files(lst: List[Union[str, Dict[str, Any]]]) -> Set[str]:
        return {
            item["name"] if isinstance(item, dict) else item for item in lst
        }

    # try ls root
    assert get_files(fs.ls("", detail=detail)) == {"empty_dir", "foo"}

    # try ls dir with files
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar"}})
    assert get_files(fs.ls("data", detail=detail)) == {"data/foo", "data/bar"}

    # try ls with files and subdirs
    storage_dir.gen(
        {"data2": {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}}
    )
    assert get_files(fs.ls("data2", detail=detail)) == {
        "data2/foo",
        "data2/bar",
        "data2/baz",
    }

    # ls with files having same name as parent
    storage_dir.gen({"lorem": {"lorem": "lorem"}})
    assert get_files(fs.ls("lorem", detail=detail)) == {"lorem/lorem"}

    # ls with subdirs having same name as parent
    storage_dir.gen({"ipsum": {"ipsum": {"ipsum": "ipsum"}}})
    assert get_files(fs.ls("ipsum", detail=detail)) == {"ipsum/ipsum"}


def test_find(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that find is compliance with fsspec."""
    storage_dir.gen(
        {
            "data": {
                "foo": "foo",
                "bar": "bar",
                "empty": {},
                "baz": {"foobaz": "foobaz"},
            }
        }
    )
    assert set(fs.find("")) == {"data/foo", "data/bar", "data/baz/foobaz"}
    assert set(fs.find("", withdirs=True)) == {
        "data",
        "data/foo",
        "data/bar",
        "data/empty",
        "data/baz",
        "data/baz/foobaz",
    }

    assert set(fs.find("", maxdepth=1)) == set()
    assert set(fs.find("", maxdepth=1, withdirs=True)) == {"data"}

    assert set(fs.find("", maxdepth=2)) == {"data/foo", "data/bar"}
    assert set(fs.find("", maxdepth=2, withdirs=True)) == {
        "data/foo",
        "data/bar",
        "data",
        "data/baz",
        "data/empty",
    }

    assert set(fs.find("not-existing")) == set()
    assert set(fs.find("data/foo")) == {"data/foo"}


def test_info(storage_dir: TmpDir, fs: WebdavFileSystem, server_address: URL):
    """Test that info is compliance with fsspec."""
    storage_dir.gen({"data": {"foo": "foo", "bar": "bar", "empty": {}}})
    data_stat = (storage_dir / "data").stat()
    foo_stat = (storage_dir / "data" / "foo").stat()
    bar_stat = (storage_dir / "data" / "bar").stat()
    empty_stat = (storage_dir / "data" / "bar").stat()

    d = fs.info("data")
    assert d.pop("etag") is None
    assert d == {
        "size": None,
        "created": approx_datetime(
            datetime.fromtimestamp(int(data_stat.st_ctime), tz=timezone.utc)
        ),
        "modified": approx_datetime(
            datetime.fromtimestamp(int(data_stat.st_mtime), tz=timezone.utc)
        ),
        "content_language": None,
        "content_type": None,
        "type": "directory",
        "name": "data",
        "display_name": "data",
        "href": join_url(server_address, "data").path + "/",
    }

    with pytest.raises(FileNotFoundError):
        fs.info("not-existing")

    d = fs.info("data/foo")
    assert d.pop("etag")
    assert d == {
        "name": "data/foo",
        "href": join_url(server_address, "data/foo").path,
        "size": 3,
        "created": approx_datetime(
            datetime.fromtimestamp(int(foo_stat.st_ctime), tz=timezone.utc)
        ),
        "modified": approx_datetime(
            datetime.fromtimestamp(int(foo_stat.st_ctime), tz=timezone.utc)
        ),
        "display_name": "foo",
        "content_language": None,
        "content_type": "application/octet-stream",
        "type": "file",
    }

    d = fs.info("data/bar")
    assert d.pop("etag")
    assert d == {
        "name": "data/bar",
        "href": join_url(server_address, "data/bar").path,
        "size": 3,
        "created": approx_datetime(
            datetime.fromtimestamp(int(bar_stat.st_ctime), tz=timezone.utc)
        ),
        "modified": approx_datetime(
            datetime.fromtimestamp(int(bar_stat.st_ctime), tz=timezone.utc)
        ),
        "content_language": None,
        "content_type": "application/octet-stream",
        "type": "file",
        "display_name": "bar",
    }

    d = fs.info("data/empty")
    assert d.pop("etag") is None
    assert d == {
        "size": None,
        "created": approx_datetime(
            datetime.fromtimestamp(int(empty_stat.st_ctime), tz=timezone.utc)
        ),
        "modified": approx_datetime(
            datetime.fromtimestamp(int(empty_stat.st_mtime), tz=timezone.utc)
        ),
        "content_language": None,
        "content_type": None,
        "type": "directory",
        "name": "data/empty",
        "display_name": "empty",
        "href": join_url(server_address, "data/empty").path + "/",
    }


def test_walk(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that walk is compliance with fsspec."""
    assert not list(fs.walk("not-existing"))

    storage_dir.gen({"data": {"foo": "foo"}})

    assert not list(fs.walk("data/foo"))
    assert list(fs.walk("data")) == [
        ("data", [], ["foo"]),
    ]

    assert list(fs.walk("")) == [
        ("", ["data"], []),
        ("data", [], ["foo"]),
    ]


def test_mkdir_with_no_create_parents(
    storage_dir: TmpDir, fs: WebdavFileSystem
):
    """Test that mkdir is compliant with fsspec."""
    fs.mkdir("data", create_parents=False)
    assert storage_dir.cat() == {"data": {}}
    storage_dir.gen({"data": {"foo": "foo"}})

    with pytest.raises(FileExistsError):
        fs.mkdir("data", create_parents=False)

    with pytest.raises(NotADirectoryError):
        fs.mkdir("data/foo/bar", create_parents=False)

    with pytest.raises(FileNotFoundError):
        fs.mkdir("data/bar/bar", create_parents=False)

    assert storage_dir.cat() == {"data": {"foo": "foo"}}


def test_makedirs_exist_ok(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that makedirs with exist_ok=True is compliant with fsspec."""
    fs.makedirs("dir1/dir2/dir3/dir4", exist_ok=True)
    assert storage_dir.cat() == {"dir1": {"dir2": {"dir3": {"dir4": {}}}}}
    # should not raise any issues again
    fs.makedirs("dir1/dir2/dir3/dir4", exist_ok=True)

    fs.makedirs("dir1/dir2/dir5/dir6", exist_ok=True)
    assert storage_dir.cat() == {
        "dir1": {"dir2": {"dir3": {"dir4": {}}, "dir5": {"dir6": {}}}}
    }
    # should not raise any issues again
    fs.makedirs("dir1/dir2/dir5/dir6", exist_ok=True)

    storage_dir.gen({"data": {"foo": "foo"}})
    fs.makedirs("data", exist_ok=True)

    with pytest.raises(NotADirectoryError):
        fs.makedirs("data/foo/bar", exist_ok=True)

    with pytest.raises(FileExistsError):
        fs.makedirs("data/foo", exist_ok=True)

    assert storage_dir.cat() == {
        "data": {"foo": "foo"},
        "dir1": {"dir2": {"dir3": {"dir4": {}}, "dir5": {"dir6": {}}}},
    }


def test_makedirs_not_exist_ok(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test that makedirs with exist_ok=False is compliant with fsspec."""
    fs.makedirs("dir1/dir2/dir3/dir4", exist_ok=False)
    assert storage_dir.cat() == {"dir1": {"dir2": {"dir3": {"dir4": {}}}}}

    with pytest.raises(FileExistsError):
        fs.makedirs("dir1/dir2/dir3/dir4", exist_ok=False)

    fs.makedirs("dir1/dir2/dir5/dir6", exist_ok=False)
    assert storage_dir.cat() == {
        "dir1": {"dir2": {"dir3": {"dir4": {}}, "dir5": {"dir6": {}}}}
    }

    with pytest.raises(FileExistsError):
        fs.makedirs("dir1/dir2/dir5/dir6", exist_ok=False)

    storage_dir.gen({"data": {"foo": "foo"}})

    with pytest.raises(FileExistsError):
        fs.makedirs("data", exist_ok=False)

    with pytest.raises(NotADirectoryError):
        fs.makedirs("data/foo/bar", exist_ok=False)

    with pytest.raises(FileExistsError):
        fs.makedirs("data/foo", exist_ok=False)

    assert storage_dir.cat() == {
        "data": {"foo": "foo"},
        "dir1": {"dir2": {"dir3": {"dir4": {}}, "dir5": {"dir6": {}}}},
    }


@pytest.mark.parametrize("recursive", [True, False])
def test_copy(storage_dir: TmpDir, fs: WebdavFileSystem, recursive: bool):
    """Test copy."""
    files = {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}
    storage_dir.gen({"data": files})

    fs.copy("data", "data2", recursive=recursive)
    assert storage_dir.cat() == {
        "data": files,
        "data2": files if recursive else {},
    }

    fs.copy("data/foo", "data/foobar", recursive=recursive)
    assert storage_dir.cat() == {
        "data": {
            "foo": "foo",
            "foobar": "foo",
            "bar": "bar",
            "baz": {"foobaz": "foobaz"},
        },
        "data2": files if recursive else {},
    }


def test_try_copying_not_existing_file(
    storage_dir: TmpDir, fs: WebdavFileSystem
):
    """Test trying to copy a not existing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        fs.copy("not-existing-file", "bar")

    with pytest.raises(FileNotFoundError):
        fs.cp_file("not-existing-file", "bar")

    assert storage_dir.cat() == {}


@pytest.mark.parametrize("recursive", [True, False])
def test_remove_dir(
    storage_dir: TmpDir, fs: WebdavFileSystem, recursive: bool
):
    """Test rm directory."""
    # this is where we deviate a bit from LocalFS
    # it seems we need to override a lot of things to achieve this,
    # as `fs.rm` uses expand_paths that also returns dirs, and is passed
    # to `fs.rm_file` which is not supposed to remove a directory.
    files = {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}
    storage_dir.gen({"data": files})

    fs.rm("data/baz", recursive=recursive)
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}


@pytest.mark.parametrize("recursive", [True, False])
def test_remove_file(
    storage_dir: TmpDir, fs: WebdavFileSystem, recursive: bool
):
    """Test rm file."""
    files = {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}
    storage_dir.gen({"data": files})

    fs.rm("data/foo", recursive=recursive)

    expected = {k: v for k, v in files.items() if k != "foo"}
    assert storage_dir.cat() == {"data": expected}


def test_try_removing_not_existing_file(
    storage_dir: TmpDir, fs: WebdavFileSystem
):
    """Test rm and rm_file on non-existing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        fs.rm("not-existing-file")

    with pytest.raises(FileNotFoundError):
        fs.rm_file("not-existing-file")

    assert storage_dir.cat() == {}


def test_rmdir(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test rmdir."""
    storage_dir.gen({"data": {"foo": "foo"}})

    with pytest.raises(OSError) as exc_info:
        fs.rmdir("data")

    assert exc_info.value.errno == errno.ENOTEMPTY
    assert exc_info.value.filename == "data"

    with pytest.raises(NotADirectoryError) as exc_info:
        fs.rmdir("data/foo")

    assert exc_info.value.errno == errno.ENOTDIR
    assert exc_info.value.filename == "data/foo"


def test_rmdir_empty(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test rmdir success on empty dir."""
    storage_dir.gen({"data": {}})

    fs.rmdir("data")
    assert storage_dir.cat() == {}


def test_rmdir_not_existing(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test rmdir on non-existing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        fs.rmdir("not-existing-dir/file")
    assert storage_dir.cat() == {}


def test_move_no_recursive(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test move without recursive."""
    files = {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}
    storage_dir.gen({"data": files})

    fs.mv("data", "data2", recursive=False)
    assert storage_dir.cat() == {
        "data2": {},
        "data": files,
    }

    fs.mv("data/foo", "data2/foobar", recursive=False)
    assert storage_dir.cat() == {
        "data2": {"foobar": "foo"},
        "data": {"bar": "bar", "baz": {"foobaz": "foobaz"}},
    }


def test_move_recursive(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test move recursive."""
    files = {"foo": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}
    storage_dir.gen({"data": files})

    fs.mv("data", "data2", recursive=True)
    assert storage_dir.cat() == {"data2": files}

    fs.mv("data2/foo", "data2/foobar", recursive=True)
    assert storage_dir.cat() == {
        "data2": {"foobar": "foo", "bar": "bar", "baz": {"foobaz": "foobaz"}}
    }


def test_try_moving_not_existing_file(
    storage_dir: TmpDir, fs: WebdavFileSystem
):
    """Test trying to move a non-existing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        fs.mv("not-existing-file", "bar")

    assert storage_dir.cat() == {}


def test_touch_truncates(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test touch with truncate=True."""
    fs.touch("foo")
    assert storage_dir.cat() == {"foo": ""}

    storage_dir.gen({"foo": "foo"})
    fs.touch("foo")
    assert storage_dir.cat() == {"foo": ""}


def test_touch_not_truncate(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test touch not truncate."""
    fs.touch("foo", truncate=False)
    assert storage_dir.cat() == {"foo": ""}

    with pytest.raises(NotImplementedError):
        # this should update the timestamp, but it's not implemented yet.
        fs.touch("foo", truncate=False)


@pytest.mark.parametrize("with_size", [True, False])
@pytest.mark.parametrize("wrap_fobj", [True, False])
def test_upload_fileobj(
    storage_dir: TmpDir,
    fs: WebdavFileSystem,
    with_size: bool,
    wrap_fobj: bool,
):
    """Test upload_fileobj.

    If with_size, size hints are provided to the API.
    If wrap_fobj, the file object is wrapped with the ReadWrapper
       that makes the peek_fileobj_length util to not be able to
       figure out the size of the given fileobj. For the most part,
       it should still work in this case.
    """
    if wrap_fobj and not with_size:
        pytest.skip("the test server does not work without content-length :(")

    file = storage_dir / "foo"
    length = file.write_bytes(b"foo")

    with file.open(mode="rb") as fobj:
        if wrap_fobj:
            fobj = ReadWrapper(fobj)  # type: ignore
        size = length if with_size else None
        fs.upload_fileobj(fobj, "data/foobar", size=size)

    assert (storage_dir / "data").cat() == {"foobar": "foo"}


def test_callbacks(storage_dir: TmpDir, fs: WebdavFileSystem):
    """Test fsspec callbacks."""
    src_file = storage_dir / "source"
    dest_file = "data/get_put_file/dest"

    data = b"test" * 4
    size = src_file.write_bytes(data)

    class EventLogger(fsspec.Callback):
        """Log callback values."""

        def __init__(self) -> None:
            self.events: List[Tuple[str, int]] = []
            super().__init__()

        def set_size(self, size: int) -> None:
            """Log a set_size call."""
            self.events.append(("set_size", size))

        def relative_update(self, inc: int = 1) -> None:
            """Log a relative_update call."""
            self.events.append(("relative_update", inc))

    event_logger = EventLogger()
    fs.put_file(src_file, dest_file, chunk_size=4, callback=event_logger)
    assert (storage_dir / dest_file).cat() == data.decode()

    assert event_logger.events[0] == ("set_size", size)
    assert event_logger.events[1:] == [
        ("relative_update", 4),
        ("relative_update", 4),
        ("relative_update", 4),
        ("relative_update", 4),
        ("relative_update", 0),
    ]
