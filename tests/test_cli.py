"""Testing dav cli."""
import os
import sys
import textwrap
from argparse import Namespace
from datetime import datetime, timedelta
from io import StringIO
from typing import Any, Dict, List
from unittest import mock

import colorama
import pytest
from fsspec.implementations.memory import MemoryFileSystem as _MemoryFS
from pytest import CaptureFixture, MonkeyPatch

from webdav4.cli import (
    Command,
    CommandCat,
    CommandCopy,
    CommandDiskUsage,
    CommandLS,
    CommandMkdir,
    CommandMove,
    CommandRemove,
    CommandRun,
    CommandSync,
    File,
    LSTheme,
    Row,
    Size,
    color_file,
    format_datetime,
    get_parser,
    human_size,
    main,
)

from .utils import TmpDir


class MemoryFileSystem(_MemoryFS):
    """Overriding to make memory per instance and extend protocol support."""

    cachable = False

    def __init__(self, *args: Any, **storage_options: Any) -> None:
        """Initializing with a custom store for each instance."""
        super().__init__(*args, **storage_options)
        self.store: Dict[str, Any] = {}
        self.pseudo_dirs: List[str] = []

    def mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        """Adds support for protocol."""
        path = self._strip_protocol(path)
        if create_parents and self.isdir(path):
            return
        super().mkdir(path, create_parents=create_parents, **kwargs)

    def copy(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        on_error: str = None,
        **kwargs: Any
    ) -> None:
        """Adds support for protocol."""
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        super().copy(
            path1, path2, recursive=recursive, on_error=on_error, **kwargs
        )

    def mv(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Adds support for protocol."""
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        super().mv(path1, path2, **kwargs)

    def rm(
        self, path: str, recursive: bool = False, maxdepth: int = None
    ) -> None:
        """Adds support for protocol."""
        path = self._strip_protocol(path)
        super().rm(path, recursive=recursive, maxdepth=maxdepth)


@pytest.fixture(autouse=True)
def unset_envvars(monkeypatch: MonkeyPatch):
    """Unset some envvars that might affect auth or coloring."""
    envvars = [
        "WEBDAV_ENDPOINT_URL",
        "LS_COLORS",
        "FORCE_COLOR",
        "NO_COLOR",
        "TERM",
    ]
    for envvar in envvars:
        monkeypatch.delenv(envvar, raising=False)


@pytest.mark.parametrize(
    "file_name, expected_color",
    [
        ("src/README.md", colorama.Fore.YELLOW),
        ("src/file.py", None),
        ("src/photo.jpg", colorama.Fore.CYAN),
        ("src/Makefile", colorama.Fore.YELLOW),
        ("src/setup.py~", colorama.Fore.LIGHTWHITE_EX),
        ("src/#temp#", colorama.Fore.LIGHTWHITE_EX),
        ("src/file.bak", colorama.Fore.LIGHTWHITE_EX),
        ("src/music.aac", colorama.Fore.RED),
        ("src/video.mov", colorama.Fore.MAGENTA),
        ("gpg/my.gpg", colorama.Fore.LIGHTGREEN_EX),
        ("my-doc.xlsx", colorama.Fore.BLUE),
        ("folder.zip", colorama.Fore.LIGHTRED_EX),
        ("a.o", colorama.Fore.GREEN),
        ("no-extension", None),
        ("no-extension.", None),
        (".no-extension", None),
        ("src/file", None),
    ],
)
def test_file_colors(file_name, expected_color):
    """Test that file are wrapped with colors based on type/extension."""
    if expected_color:
        expected = expected_color + file_name + colorama.Style.RESET_ALL
    else:
        expected = file_name
    assert color_file(File.from_path(file_name)) == expected


@pytest.mark.parametrize("file_name", ["src/file.py", "src", "src/docs"])
def test_dir_colors(file_name):
    """Test that dirs are brightly colored."""
    assert (
        color_file(File.from_path(file_name), isdir=True)
        == colorama.Style.BRIGHT
        + colorama.Fore.BLUE
        + file_name
        + colorama.Style.RESET_ALL
    )


@mock.patch.object(sys.stdout, "isatty", return_value=True)
def test_ls_colors(m):
    """Test ls colors theme with support for lscolors envvar."""
    theme = LSTheme(lscolors="rs=0:di=01;34:ex=01;32:*.py=33:*.zip=35")
    assert theme.codes == {"rs": "0", "di": "01;34", "ex": "01;32"}
    assert theme.extensions == {"py": "33", "zip": "35"}
    assert theme.colored


def test_lstheme_should_color(monkeypatch: MonkeyPatch):
    """Test LSTheme color deciding factors."""
    theme = LSTheme()
    with mock.patch.object(sys.stdout, "isatty", return_value=False):
        assert not theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "false")
        assert not theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "")
        assert not theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "0")
        assert not theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "true")
        assert theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "1")
        assert theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "2")
        assert theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "3")
        assert theme._should_color()

    monkeypatch.delenv("FORCE_COLOR")

    with mock.patch.object(sys.stdout, "isatty", return_value=True):
        assert theme._should_color()

        monkeypatch.setenv("TERM", "dumb")
        assert not theme._should_color()
        monkeypatch.delenv("TERM")

        # NO_COLOR only cares about it being set
        monkeypatch.setenv("NO_COLOR", "1")
        assert not theme._should_color()
        monkeypatch.delenv("NO_COLOR")

        monkeypatch.setenv("FORCE_COLOR", "0")
        assert not theme._should_color()

        monkeypatch.setenv("FORCE_COLOR", "1")
        assert theme._should_color()

        # should still print in color as FORCE_COLOR is set.
        monkeypatch.setenv("NO_COLOR", "1")
        assert theme._should_color()


def test_style_path():
    """Test styling path for the ls output."""
    theme = LSTheme(lscolors="*.py=32")

    theme.colored = False
    assert theme.style_path("hello") == "hello"
    assert theme.style_path("hello", isdir=True) == "hello"

    theme.colored = True

    assert theme.style_path("path") == "path"
    assert theme.extensions == {"py": "32"}
    assert theme.style_path("path.py") == "\x1b[32mpath.py\x1b[0m"
    assert theme.style_path("path", isdir=True) == "\x1b[1m\x1b[34mpath\x1b[0m"

    theme = LSTheme(dir_trailing_slash=True)
    theme.colored = True
    assert (
        theme.style_path("path", isdir=True) == "\x1b[1m\x1b[34mpath/\x1b[0m"
    )
    theme.colored = False
    assert theme.style_path("path", isdir=True) == "path/"


def test_style_size_datetime():
    """Test styling of size and datetime in the output of ls."""
    theme = LSTheme()
    theme.colored = False

    dt = datetime.today()
    assert theme.style_datetime(format_datetime(dt)) == dt.strftime(
        "%b %d %H:%M"
    )
    assert theme.style_size(*human_size(2027)) == "2.0k"

    theme.colored = True
    assert theme.style_datetime(
        format_datetime(dt)
    ) == "\x1b[34m{0}\x1b[0m".format(dt.strftime("%b %d %H:%M"))
    assert theme.style_size(
        *human_size(2027)
    ) == "\x1b[1m\x1b[32m{0}\x1b[0m\x1b[32m{1}\x1b[0m".format("2.0", "k")

    assert theme.style_size(*human_size(None)) == "\x1b[2m-\x1b[0m"


@pytest.mark.parametrize(
    "nbytes, expected",
    [
        (0, ("0", "")),
        (2027, ("2.0", "k")),
        (389, ("389", "")),
        (6498656, ("6.2", "M")),
        (1073741824, ("1.0", "G")),
        (107374182400, ("100", "G")),
        (None, ("", "-")),
    ],
)
def test_human_size(nbytes: int, expected: str):
    """Test humanizing of n bytes/sizes."""
    assert human_size(nbytes) == expected


def test_datetime_format():
    """Test humanizing datetime format that is compatible with ls."""
    today = datetime.today()
    recent = today - timedelta(days=10)
    old = today - timedelta(days=200)

    assert format_datetime(recent) == recent.strftime("%b %d %H:%M")
    assert format_datetime(old) == old.strftime("%b %d %Y")
    assert format_datetime(None) == "-"


@pytest.mark.parametrize(
    "path, text",
    [
        ("data", "6 (6 bytes) in 2 files: data"),
        ("data/foo", "3 (3 bytes) in 1 file: data/foo"),
        ("data/bar", "3 (3 bytes) in 1 file: data/bar"),
    ],
)
def test_du_cli(capsys: CaptureFixture, path: str, text: str):
    """Test du command."""
    mfs = MemoryFileSystem()
    mfs.mkdir("data")
    mfs.pipe({"data/foo": b"foo", "data/bar": b"bar"})

    ns = Namespace(path=path)
    CommandDiskUsage(ns, mfs).run()

    out, _ = capsys.readouterr()
    assert text in out
    mfs.clear_instance_cache()


def test_cat_cli(capsys: CaptureFixture):
    """Test cat command."""
    mfs = MemoryFileSystem()
    mfs.pipe({"data": b"data" * 1000})

    ns = Namespace(path="data")
    CommandCat(ns, mfs).run()
    out, _ = capsys.readouterr()
    # printed as str
    assert "data" * 1000 in out


def test_cp_cli(storage_dir: TmpDir, monkeypatch: MonkeyPatch):
    """Test cp command."""
    memfs = MemoryFileSystem()

    memfs.mkdir("data1")
    memfs.pipe({"data1/foo": b"foo", "data1/bar": b"bar"})

    ns = Namespace(
        path1="memory://data1", path2="memory://data2", recursive=False
    )
    CommandCopy(ns, memfs).run()

    assert memfs.cat("data1", recursive=True, on_error="ignore") == {
        "/data1/bar": b"bar",
        "/data1/foo": b"foo",
    }
    assert memfs.isdir("data2")

    ns = Namespace(
        path1="memory://data1", path2="memory://data2", recursive=True
    )
    CommandCopy(ns, memfs).run()
    assert memfs.cat("data1", recursive=True, on_error="ignore") == {
        "/data1/bar": b"bar",
        "/data1/foo": b"foo",
    }
    assert memfs.cat("data2", recursive=True, on_error="ignore") == {
        "/data2/bar": b"bar",
        "/data2/foo": b"foo",
    }
    assert memfs.isdir("data2")

    ns = Namespace(
        path1="memory://data1/foo",
        path2="memory://data2/foobar",
        recursive=True,
    )
    CommandCopy(ns, memfs).run()
    assert memfs.cat_file("data2/foobar") == b"foo"

    monkeypatch.chdir(storage_dir)

    d = {"lorem": "lorem", "ipsum": "ipsum"}
    storage_dir.gen({"dir": d})
    ns = Namespace(path1="dir", path2="memory://dir", recursive=True)
    CommandCopy(ns, memfs).run()
    assert memfs.cat("dir", True, "ignore") == {
        "/dir/ipsum": b"ipsum",
        "/dir/lorem": b"lorem",
    }

    ns = Namespace(path1="memory://dir", path2="dir2", recursive=True)
    CommandCopy(ns, memfs).run()
    assert storage_dir.cat() == {"dir": d, "dir2": d}

    ns = Namespace(path1="memory://dir/ipsum", path2="ipsum", recursive=True)
    CommandCopy(ns, memfs).run()
    assert storage_dir.cat() == {"dir": d, "dir2": d, "ipsum": "ipsum"}


def test_rm_cli():
    """Test rm command."""
    memfs = MemoryFileSystem()

    memfs.mkdir("data1")
    memfs.pipe({"data1/foo": b"foo", "data1/bar": b"bar"})

    cmd = CommandRemove(Namespace(path="data1", recursive=False), memfs)
    with pytest.raises(OSError):
        cmd.run()

    cmd = CommandRemove(Namespace(path="data1/bar", recursive=False), memfs)
    cmd.run()
    assert not memfs.exists("data1/bar")

    cmd = CommandRemove(Namespace(path="data1", recursive=True), memfs)
    cmd.run()
    assert not memfs.exists("data1")


def test_mkdir_cli():
    """Test mkdir command."""
    memfs = MemoryFileSystem()

    cmd = CommandMkdir(Namespace(path="data1", parents=False), memfs)
    cmd.run()
    assert memfs.isdir("data1")

    cmd = CommandMkdir(Namespace(path="data1/dir1/dir2", parents=True), memfs)
    cmd.run()
    assert memfs.isdir("data1/dir1")
    assert memfs.isdir("data1/dir1/dir2")


def test_mv_cli(storage_dir: TmpDir, monkeypatch: MonkeyPatch):
    """Test mv command."""
    memfs = MemoryFileSystem()

    memfs.mkdir("data1")
    memfs.pipe({"data1/foo": b"foo", "data1/bar": b"bar"})

    ns = Namespace(
        path1="memory://data1", path2="memory://data2", recursive=True
    )
    CommandMove(ns, memfs).run()
    assert not memfs.isdir("data1")
    assert memfs.cat("data2", recursive=True, on_error="ignore") == {
        "/data2/bar": b"bar",
        "/data2/foo": b"foo",
    }
    assert memfs.isdir("data2")

    ns = Namespace(
        path1="memory://data2/foo",
        path2="memory://data2/foobar",
        recursive=True,
    )
    CommandMove(ns, memfs).run()
    assert memfs.cat_file("data2/foobar") == b"foo"
    assert not memfs.exists("memory://data2/foo")

    monkeypatch.chdir(storage_dir)

    d = {"lorem": "lorem", "ipsum": "ipsum"}
    storage_dir.gen({"dir": d})
    ns = Namespace(path1="dir", path2="memory://dir", recursive=True)
    CommandMove(ns, memfs).run()
    assert memfs.cat("dir", True, "ignore") == {
        "/dir/ipsum": b"ipsum",
        "/dir/lorem": b"lorem",
    }

    ns = Namespace(path1="memory://dir", path2="dir2", recursive=True)
    CommandMove(ns, memfs).run()
    assert storage_dir.cat() == {"dir": d, "dir2": d}


def test_ls_cli(capsys: CaptureFixture):
    """Test ls command."""
    mfs = MemoryFileSystem()

    mfs.mkdir("data")
    mfs.mkdir("dir")
    mfs.pipe(
        {
            "data/foo": b"foo",
            "data/bar": b"bar",
            "dir/nested/foo": b"dirfoo" * 1000,
        }
    )

    ns = Namespace(path="data", recursive=False, level=None, full_path=False)
    assert set(CommandLS(ns, mfs).ls()) == {
        Row("-", Size("3"), "foo"),
        Row("-", Size("3"), "bar"),
    }

    ns = Namespace(
        path="data/foo", recursive=False, level=None, full_path=False
    )
    assert set(CommandLS(ns, mfs).ls()) == {Row("-", Size("3"), "foo")}

    ns = Namespace(
        path="data/bar", recursive=False, level=None, full_path=False
    )
    assert set(CommandLS(ns, mfs).ls()) == {Row("-", Size("3"), "bar")}

    with pytest.raises(FileNotFoundError):
        ns = Namespace(
            path="not-existing", recursive=False, level=None, full_path=False
        )
        assert set(CommandLS(ns, mfs).ls())

    ns = Namespace(path="data", recursive=False, level=None, full_path=True)
    assert set(CommandLS(ns, mfs).ls()) == {
        Row("-", Size("3"), "data/foo"),
        Row("-", Size("3"), "data/bar"),
    }

    ns = Namespace(path="", recursive=True, level=2, full_path=True)
    assert set(CommandLS(ns, mfs).ls()) == {
        Row("-", Size("3"), "data/foo"),
        Row("-", Size("3"), "data/bar"),
        Row("-", Size("0"), "dir/nested", isdir=True),
    }

    ns = Namespace(path="", recursive=True, level=None, full_path=False)
    assert set(CommandLS(ns, mfs).ls()) == {
        Row("-", Size("3"), "data/foo"),
        Row("-", Size("3"), "data/bar"),
        Row("-", Size("5.9", "k"), "dir/nested/foo"),
    }

    CommandLS.render([])
    assert ("", "") == capsys.readouterr()

    CommandLS.render(
        [
            Row("Apr 05 09:40", Size("148.0", "k"), "README.md"),
            Row("Jun 03 03:33", Size("6.2", "M"), "my-docs.docx"),
        ]
    )

    out, _ = capsys.readouterr()
    assert (
        textwrap.dedent(
            """\
        Apr 05 09:40 148.0k README.md
        Jun 03 03:33   6.2M my-docs.docx"""
        )
        in out
    )

    ns = Namespace(
        path="data/foo", recursive=True, level=None, full_path=False
    )
    CommandLS(ns, mfs).run()
    assert capsys.readouterr() == ("-  3 foo\n", "")

    with pytest.raises(FileNotFoundError):
        ns = Namespace(
            path="not-existing", recursive=True, level=None, full_path=False
        )
        CommandLS(ns, mfs).run()


def test_sync_cli_local_to_remote(
    storage_dir: TmpDir, monkeypatch: MonkeyPatch
):
    """Test syncing between local to remote filesystem."""
    storage_dir.gen({"data": {"foo": "foo"}})
    memfs = MemoryFileSystem()

    monkeypatch.chdir(storage_dir)
    ns = Namespace(path1="data", path2="memory://data", delete=False)

    cmd = CommandSync(ns, memfs)
    cmd.run()
    assert memfs.cat("data", True, "ignore") == {"/data/foo": b"foo"}

    (storage_dir / "data" / "bar").write_text("bar")
    cmd.run()
    assert memfs.cat("data", True, "ignore") == {
        "/data/foo": b"foo",
        "/data/bar": b"bar",
    }

    (storage_dir / "data" / "bar").write_text("new updates")
    cmd.run()
    assert memfs.cat("data", True, "ignore") == {
        "/data/foo": b"foo",
        "/data/bar": b"new updates",
    }

    (storage_dir / "data" / "bar").unlink()
    ns = Namespace(path1="data", path2="memory://data", delete=True)
    cmd = CommandSync(ns, memfs)
    cmd.run()
    assert memfs.cat("data", True, "ignore") == {"/data/foo": b"foo"}

    (storage_dir / "data" / "dir").gen({"lorem": "lorem"})
    cmd.run()
    assert memfs.cat("data", True, "ignore") == {
        "/data/foo": b"foo",
        "/data/dir/lorem": b"lorem",
    }

    # nothing should change
    cmd.run()
    assert memfs.cat("data", True, "ignore") == {
        "/data/foo": b"foo",
        "/data/dir/lorem": b"lorem",
    }

    # trying to make src to be a directory, we should abort in this case
    (storage_dir / "data" / "foo").unlink()
    (storage_dir / "data").gen({"foo": {"foo": "foo"}})
    with pytest.raises(TypeError) as exc_info:
        cmd.run()
    assert (
        str(exc_info.value) == "cannot sync between different types, "
        "src is directory, dest is file"
    )


def test_sync_remote_to_local(storage_dir: TmpDir, monkeypatch: MonkeyPatch):
    """Test syncing between remote to local filesystem."""
    memfs = MemoryFileSystem()
    memfs.mkdir("data")
    memfs.pipe({"data/foo": b"foo"})

    monkeypatch.chdir(storage_dir)
    ns = Namespace(path1="memory://data", path2="data", delete=False)

    cmd = CommandSync(ns, memfs)
    cmd.run()
    assert storage_dir.cat() == {"data": {"foo": "foo"}}

    memfs.pipe({"data/bar": b"bar"})
    cmd.run()
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "bar"}}

    memfs.pipe({"data/bar": b"new updates"})
    cmd.run()
    assert storage_dir.cat() == {"data": {"foo": "foo", "bar": "new updates"}}

    memfs.rm("data/bar")
    ns = Namespace(path1="memory://data", path2="data", delete=True)
    cmd = CommandSync(ns, memfs)
    cmd.run()
    assert storage_dir.cat() == {"data": {"foo": "foo"}}

    memfs.mkdir("data/dir")
    memfs.pipe({"data/dir/lorem": b"lorem"})
    cmd.run()
    assert storage_dir.cat() == {
        "data": {"foo": "foo", "dir": {"lorem": "lorem"}}
    }

    # nothing should change
    cmd.run()
    assert storage_dir.cat() == {
        "data": {"foo": "foo", "dir": {"lorem": "lorem"}}
    }


def test_sync_remote_to_remote():
    """Test syncing between remote to remote filesystem."""
    memfs = MemoryFileSystem()

    memfs.mkdir("data")
    memfs.pipe({"data/foo": b"foo"})

    ns = Namespace(path1="memory://data", path2="memory://data2", delete=False)

    cmd = CommandSync(ns, memfs)
    cmd.run()
    assert memfs.cat("data2", True, "ignore") == {"/data2/foo": b"foo"}

    memfs.pipe({"data/bar": b"bar"})
    cmd.run()
    assert memfs.cat("data2", True, "ignore") == {
        "/data2/foo": b"foo",
        "/data2/bar": b"bar",
    }

    memfs.pipe({"data/bar": b"new updates"})
    cmd.run()
    assert memfs.cat("data2", True, "ignore") == {
        "/data2/foo": b"foo",
        "/data2/bar": b"new updates",
    }

    memfs.rm("data/bar")
    ns = Namespace(path1="memory://data", path2="memory://data2", delete=True)
    cmd = CommandSync(ns, memfs)
    cmd.run()
    assert memfs.cat("data2", True, "ignore") == {"/data2/foo": b"foo"}

    memfs.mkdir("data/dir")
    memfs.pipe({"data/dir/lorem": b"lorem"})
    cmd.run()
    assert memfs.cat("data2", True, "ignore") == {
        "/data2/foo": b"foo",
        "/data2/dir/lorem": b"lorem",
    }

    # nothing should change
    cmd.run()
    assert memfs.cat("data2", True, "ignore") == {
        "/data2/foo": b"foo",
        "/data2/dir/lorem": b"lorem",
    }


def test_run_cli(monkeypatch: MonkeyPatch):
    """Test run command."""
    memfs = MemoryFileSystem()

    memfs.mkdir("data")
    memfs.pipe({"data/foo": b"foo"})

    buff = StringIO("ls" + os.linesep)
    buff.write("du" + os.linesep)
    buff.write("sync memory://data memory://data2" + os.linesep)
    buff.write("# comments" + os.linesep)
    buff.write("rm data2 --recursive" + os.linesep)
    buff.write("cat memory://data/foo" + os.linesep)
    buff.write("mv memory://data memory://data3 --recursive" + os.linesep)
    buff.write("ls memory://data3" + os.linesep)
    buff.write("du memory://data3" + os.linesep)
    buff.write("mkdir memory://data3 -p" + os.linesep)
    buff.seek(0)

    monkeypatch.setattr("sys.stdin", buff)
    _, subparsers = get_parser()
    cmd = CommandRun(Namespace(path="-", subparsers=subparsers), memfs)
    cmd.run()

    pos = buff.tell()
    buff.write("not-existing-command" + os.linesep)
    buff.seek(pos)
    with pytest.raises(ValueError) as exc_info:
        cmd.run()
    assert str(exc_info.value) == "unknown command: not-existing-command"

    with pytest.raises(ValueError) as exc_info:
        cmd = CommandRun(Namespace(path=None, subparsers=subparsers), memfs)
        cmd.run()
    assert str(exc_info.value) == "no path specified or contents piped"


def test_shorthand_url():
    """Test curl-like shorthand url and auto adding scheme support."""
    ns = Namespace(endpoint_url=":3000/foo", user=None, password=None)
    assert Command(ns).fs.client.base_url == "http://localhost:3000/foo"

    ns.endpoint_url = ":/foo"
    assert Command(ns).fs.client.base_url == "http://localhost/foo"

    ns.endpoint_url = "example.org/path"
    assert Command(ns).fs.client.base_url == "https://example.org/path"

    ns.endpoint_url = "https://example.org/path"
    assert Command(ns).fs.client.base_url == "https://example.org/path"


def test_auth(monkeypatch: MonkeyPatch):
    """Test auth from args and url."""
    ns = Namespace(
        endpoint_url="http://server.com", user="user", password="pwd"
    )
    assert Command(ns).auth == ("user", "pwd")

    ns.endpoint_url = "http://user:pwd@server.com"
    assert Command(ns).auth == ("user", "pwd")

    monkeypatch.setenv("WEBDAV_ENDPOINT_URL", "http://user2:pwd2@server.com")
    # --endpoint-url should still take precedence
    assert Command(ns).auth == ("user", "pwd")

    ns = Namespace(endpoint_url=None, user="user", password="pwd")
    assert Command(ns).auth == ("user2", "pwd2")

    monkeypatch.delenv("WEBDAV_ENDPOINT_URL")


def test_main():
    """Test main command line entrypoint."""
    assert main(["-v", "ls"]) == 1
    assert main(["ls"]) == 1
