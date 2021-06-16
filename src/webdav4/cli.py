# pylint: disable=too-many-lines,invalid-name
"""CLI for the Webdav."""

import argparse
import errno
import fileinput
import logging
import os
import posixpath
import re
import sys
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from posixpath import sep
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    cast,
)

from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractFileSystem
from fsspec.utils import stringify_path

from webdav4 import urls

from .fsspec import WebdavFileSystem
from .urls import URL

if TYPE_CHECKING:
    from argparse import ArgumentParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


URL_SCHEME_RE = re.compile(r"^[a-z][a-z0-9.+-]*://", re.IGNORECASE)


class File(NamedTuple):
    """DS that helps us to not compute ext and basename again and again."""

    path: str
    ext: str
    basename: str

    @classmethod
    def from_path(cls, path: str) -> "File":
        """Build the File object from the given path."""
        without_dot = slice(1, None)  # we don't want `.` in the ext
        _, ext = posixpath.splitext(path)
        return cls(path, ext[without_dot], posixpath.basename(path))


def lexical_relpath(path: str, start: str) -> str:
    """Calculates relpath lexically.

    It differs on how it returns the basename for the same paths,
    and bails out if the prefix does not match, as opposed to the
    os.path.relpath.
    """
    if path == start:
        return posixpath.basename(path)
    if start and f"{start}{sep}" in path:
        return path[len(start) + 1 :]
    return path


class LSTheme:
    """Theming support for `ls` command.

    Though, we do use it in other places as well.
    """

    def __init__(
        self, lscolors: str = None, dir_trailing_slash: bool = False
    ) -> None:
        """Build theme from optional lscolors and other configs."""
        self.extensions: Dict[str, str] = {}
        self.codes: Dict[str, str] = {}
        self.dir_trailing_slash: bool = dir_trailing_slash
        self.lscolors: str = lscolors or os.environ.get("LS_COLORS") or ""

        self.colored: bool = self._should_color()
        self._load_ls_colors(self.lscolors)

    @staticmethod
    def _should_color() -> bool:
        """See if we should print in colors or not.

        This honors FORCE_COLOR and NO_COLOR envvars as well. :).
        """
        force_color = os.getenv("FORCE_COLOR")
        if force_color and force_color in ("0", "false"):
            return False
        if force_color:
            return True
        if not sys.stdout.isatty() or os.getenv("TERM") == "dumb":
            return False
        return "NO_COLOR" not in os.environ

    # see:
    # https://github.com/iterative/dvc/blob/1335c7f/dvc/command/ls/ls_colors.py
    def _load_ls_colors(self, lscolors: str) -> None:
        """Loads lscolors."""
        for item in lscolors.split(":"):
            try:
                code, color = item.split("=", 1)
            except ValueError:
                continue
            if code.startswith("*."):
                self.extensions[code[2:]] = color
            else:
                self.codes[code] = color

    def style_path(self, path: str, **info: Any) -> str:
        """Style path in the ls output."""
        isdir: bool = info.get("isdir", False)

        if self.dir_trailing_slash and isdir and not path.endswith(sep):
            path += sep

        if not self.colored:
            return path

        file = File.from_path(path)
        val = None
        if isdir:
            val = self.codes.get("di")
        elif file.ext:
            val = self.extensions.get(file.ext, None)

        if val:
            reset = self.codes.get("rs", 0)
            return f"\033[{val}m{file.path}\033[{reset}m"
        return color_file(file, isdir=isdir)

    def style_size(self, size: str, suff: str = "") -> str:
        """Style size in the ls output, also humanizes it."""
        if suff == "-" and self.colored:
            return size + colored(suff, style="dim")
        if self.colored:
            return colored(size, "green", "bright") + colored(suff, "green")
        return size + suff

    def style_datetime(self, mtime: str) -> str:
        """Humanize and style datetime in the ls output."""
        return colored(mtime, "blue") if self.colored else mtime


theme = LSTheme()


# Ported from exa, with some small tweaks
# https://github.com/ogham/exa/blob/a6754f3cc3/src/info/filetype.rs
def is_build_config_or_readme(file: File) -> bool:
    """is_immediate in terms of exa.

    This file build or generate something (eg: Makefile).
    """
    return file.basename.lower().startswith("readme") or file.basename in {
        "ninja",
        "Makefile",
        "Cargo.toml",
        "SConstruct",
        "CMakeLists.txt",
        "build.gradle",
        "pom.xml",
        "Rakefile",
        "package.json",
        "Gruntfile.js",
        "Gruntfile.coffee",
        "BUILD",
        "BUILD.bazel",
        "WORKSPACE",
        "build.xml",
        "webpack.config.js",
        "meson.build",
        "composer.json",
        "RoboFile.php",
        "PKGBUILD",
        "Justfile",
        "Procfile",
        "Dockerfile",
        "Containerfile",
        "Vagrantfile",
        "Brewfile",
        "Gemfile",
        "Pipfile",
        "build.sbt",
        "mix.exs",
        "bsconfig.json",
        "tsconfig.json",
    }


def is_image(file: File) -> bool:
    """See if the ext is an image type."""
    return file.ext in {
        "png",
        "jpeg",
        "jpg",
        "gif",
        "bmp",
        "tiff",
        "tif",
        "ppm",
        "pgm",
        "pbm",
        "pnm",
        "webp",
        "raw",
        "arw",
        "svg",
        "stl",
        "eps",
        "dvi",
        "ps",
        "cbr",
        "jpf",
        "cbz",
        "xpm",
        "ico",
        "cr2",
        "orf",
        "nef",
        "heif",
    }


def is_video(file: File) -> bool:
    """See if the ext is a Video type."""
    return file.ext in {
        "avi",
        "flv",
        "m2v",
        "m4v",
        "mkv",
        "mov",
        "mp4",
        "mpeg",
        "mpg",
        "ogm",
        "ogv",
        "vob",
        "wmv",
        "webm",
        "m2ts",
        "heic",
    }


def is_music(file: File) -> bool:
    """See if the ext is a Music type."""
    return file.ext in {
        "aac",
        "m4a",
        "mp3",
        "ogg",
        "wma",
        "mka",
        "opus",
        "alac",
        "ape",
        "flac",
        "wav",
    }


def is_crypto(file: File) -> bool:
    """See if the file ext is related to the crypto."""
    return file.ext in {
        "asc",
        "enc",
        "gpg",
        "pgp",
        "sig",
        "signature",
        "pfx",
        "p12",
    }


def is_document(file: File) -> bool:
    """See if the ext is a document type."""
    return file.ext in {
        "djvu",
        "doc",
        "docx",
        "dvi",
        "eml",
        "eps",
        "fotd",
        "key",
        "keynote",
        "numbers",
        "odp",
        "odt",
        "pages",
        "pdf",
        "ppt",
        "pptx",
        "rtf",
        "xls",
        "xlsx",
    }


def is_compressed(file: File) -> bool:
    """See if the ext is a compressed filetype."""
    return file.ext in {
        "zip",
        "tar",
        "Z",
        "z",
        "gz",
        "bz2",
        "a",
        "ar",
        "7z",
        "iso",
        "dmg",
        "tc",
        "rar",
        "par",
        "tgz",
        "xz",
        "txz",
        "lz",
        "tlz",
        "lzma",
        "deb",
        "rpm",
        "zst",
    }


def is_temp(file: File) -> bool:
    """See if the file is a temporary one."""
    return (
        file.basename.endswith("~")
        or (file.basename.startswith("#") and file.basename.endswith("#"))
        or file.ext in {"tmp", "swp", "swo", "swn", "bak", "bk"}
    )


def is_compiled(file: File) -> bool:
    """See if the file is a compiled one."""
    return file.ext in {"class", "elc", "hi", "o", "pyc", "zwc", "ko"}


def _color_file(  # noqa: C901
    file: File, isdir: bool = False
) -> Optional[str]:
    """Returns a color appropriate for file/dir based on ext/path etc."""
    try:
        import colorama  # pylint: disable=import-outside-toplevel
    except ModuleNotFoundError:  # pragma: no cover
        return None

    color: Optional[str] = None
    if isdir:
        color = colorama.Style.BRIGHT + colorama.Fore.BLUE
    elif is_temp(file):
        color = colorama.Fore.LIGHTWHITE_EX
    elif is_build_config_or_readme(file):
        color = colorama.Fore.YELLOW
    elif is_image(file):
        color = colorama.Fore.CYAN
    elif is_music(file):
        color = colorama.Fore.RED
    elif is_video(file):
        color = colorama.Fore.MAGENTA
    elif is_crypto(file):
        color = colorama.Fore.LIGHTGREEN_EX
    elif is_document(file):
        color = colorama.Fore.BLUE
    elif is_compressed(file):
        color = colorama.Fore.LIGHTRED_EX
    elif is_compiled(file):
        color = colorama.Fore.GREEN

    return color if color else None


def color_file(file: File, isdir: bool = False) -> str:
    """Apply an appropriate color on a file."""
    color = _color_file(file, isdir=isdir)
    if color:
        try:
            import colorama  # pylint: disable=import-outside-toplevel
        except ModuleNotFoundError:  # pragma: no cover
            pass
        else:
            reset = colorama.Style.RESET_ALL
            assert isinstance(reset, str)
            return color + file.path + reset
    return file.path


def colored(name: str, color: str = "", style: str = "") -> str:
    """Colors a string with given color name."""
    try:
        import colorama  # pylint: disable=import-outside-toplevel

        colors = {
            "green": colorama.Fore.GREEN,
            "red": colorama.Fore.RED,
            "blue": colorama.Fore.BLUE,
            "gray": colorama.Fore.LIGHTWHITE_EX,
        }
        styles = {"bright": colorama.Style.BRIGHT, "dim": colorama.Style.DIM}
        reset = colorama.Style.RESET_ALL
    except ModuleNotFoundError:  # pragma: no cover
        colors = {}
        reset = ""
        styles = {}

    _color = colors.get(color, "")
    _style = styles.get(style, "")
    return f"{_style}{_color}{name}{reset}"


def to_fixed_width(n: float, max_width: int) -> str:
    """Converts a float to the fixed max_width."""
    for i in range(max_width - 2, -1, -1):
        float_str = f"{n:.{i}f}"
        if len(float_str) <= max_width:
            break
    assert max_width >= 2  # pragma: no cover
    return float_str


def human_size(nbytes: Optional[float]) -> Tuple[str, str]:
    """Converts bytes to human-readable size."""
    if nbytes is None:
        return "", "-"
    if nbytes < 1024:
        return str(nbytes), ""

    suffixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    for suff in suffixes:  # noqa: B007, pragma: no cover
        if nbytes < 1024:
            break
        nbytes /= 1024

    return to_fixed_width(nbytes, 3), suff


def format_datetime(mtime: Any) -> str:
    """Converts mtime to ls-compatible output."""
    if not isinstance(mtime, datetime):
        return "-"
    fmt = "%b %d %H:%M"
    # it's mostly for presentation, so we don't care much about tz
    if mtime.replace(tzinfo=None) < datetime.today() - timedelta(days=180):
        fmt = "%b %d %Y"
    return mtime.strftime(fmt)


def process_url(url: str) -> str:
    """Support url without schemes and the shorthand format for localhost."""
    if not URL_SCHEME_RE.match(url):
        # See if we're using curl style shorthand for localhost (:3000/foo)
        shorthand = re.match(r"^:(?!:)(\d*)(/?.*)$", url)
        if shorthand:
            port = shorthand.group(1)
            rest = shorthand.group(2)
            url = "http://" + "localhost"
            if port:
                url += ":" + port
            url += rest
        else:
            url = "https://" + url
    return url


def prepare_url_auth(  # noqa: C901
    args: Namespace,
) -> Tuple[urls.URL, Optional[Tuple[str, str]]]:
    """Process url and auth from the given arguments or from the envvar.

    That includes using user and password from url itself.
    """
    url = args.endpoint_url
    if not url:
        url = os.getenv("WEBDAV_ENDPOINT_URL")

    if not url:
        raise ValueError(
            "no endpoint url specified, "
            "please specify it through --endpoint-url "
            "or via WEBDAV_ENDPOINT_URL envvar."
        )

    url_obj = URL(process_url(url))
    user, password = None, None
    auth = None

    if url_obj.username:
        user = url_obj.username
    elif args.user:
        user = args.user

    if url_obj.password:
        password = url_obj.password
    elif args.password:
        password = args.password

    if user and password:
        auth = user, password

    return url_obj, auth


class Command:
    """Base class for all commands."""

    def __init__(self, args: Namespace, fs: AbstractFileSystem = None) -> None:
        """Pass the arguments and optionally fs."""
        self.args = args
        self.auth = None
        self.endpoint_url = None
        if fs:
            self.fs = fs
            return

        self.endpoint_url, self.auth = prepare_url_auth(args)
        logger.debug("base url set to %s", self.endpoint_url)
        self.fs = WebdavFileSystem(self.endpoint_url, auth=self.auth)

    def run(self) -> None:
        """Override this function to do some operations."""
        raise NotImplementedError


class Size(NamedTuple):
    """Size data structure carrying suffix and bytes."""

    nbytes: str
    suff: str = ""


class Row(NamedTuple):
    """Row data structure representing each row for the ls output."""

    date: str
    size: Size
    file: str
    isdir: bool = False


class CommandLS(Command):
    """Command for ls."""

    def ls(self) -> Iterator[Row]:
        """Returns a list of rows that are styled."""
        # if user set a `-L`, we would still like to show
        # directory in L level.
        withdirs = not self.args.recursive or bool(self.args.level)
        depth = self.args.level if self.args.recursive else 1
        details = self.fs.find(
            self.args.path, maxdepth=depth, detail=True, withdirs=withdirs
        )

        path = self.fs._strip_protocol(  # pylint: disable=protected-access
            self.args.path
        ).strip("/")
        path_level = path.count(sep) + 1 if path else 0

        if not details:
            # if there are no information, it's likely that
            # the file does not exist at all.
            self.fs.info(path)
        elif len(details) == 1 and self.fs.isfile(path):
            # or, the entry was a file, so fsspec did not return
            # details for it.
            details = {path: self.fs.info(path)}

        for file_path, info in details.items():
            isdir = info.get("type") == "directory"
            file_path = file_path.strip("/")
            within_depth = depth and (
                0 <= file_path.count(sep) - path_level < depth - 1
            )

            if withdirs and isdir and within_depth:
                continue

            file_name = file_path
            if not self.args.full_path:
                file_name = lexical_relpath(file_path, path)

            date = format_datetime(info.get("modified"))
            size = Size(*human_size(info.get("size")))
            yield Row(date=date, size=size, file=file_name, isdir=isdir)

    @staticmethod
    def render(details: List[Row]) -> None:
        """Display provided information in a columnar format."""
        if not details:
            return None

        date_maxsize = max(len(row.date) for row in details)
        size_maxsize = max(len(row.size.nbytes) for row in details)

        for row in details:
            date = theme.style_datetime(row.date.ljust(date_maxsize))

            size_just = size_maxsize if row.size.suff else size_maxsize + 1
            size = theme.style_size(
                row.size.nbytes.rjust(size_just), suff=row.size.suff
            )
            file = theme.style_path(row.file, isdir=row.isdir)
            print(date, size, file)

        return None

    def run(self) -> None:
        """List and render the ls output."""
        try:
            details = list(self.ls())
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                errno.ENOENT, "No such file or directory", self.args.path
            ) from exc
        return self.render(details)


class CommandTransfer(Command):
    """Shared class for move and copy."""

    def transfer_remote(self) -> None:
        """Transfer between remote-remote operations."""
        raise NotImplementedError

    def run(self) -> None:
        """Run transfer between two paths."""
        path1 = self.args.path1
        path2 = self.args.path2
        recursive = self.args.recursive

        if is_fqpath(self.fs, path1) and is_fqpath(self.fs, path2):
            self.transfer_remote()
        elif is_fqpath(self.fs, path1):
            if self.fs.isdir(path1):
                os.makedirs(path2, exist_ok=True)
            self.fs.download(path1, path2, recursive=recursive)
        else:
            self.fs.upload(path1, path2, recursive=recursive)


class CommandCopy(CommandTransfer):
    """Command for cp."""

    def transfer_remote(self) -> None:
        """Copy files/diretories between remotes."""
        self.fs.cp(
            self.args.path1, self.args.path2, recursive=self.args.recursive
        )


class CommandMove(CommandTransfer):
    """Command for mv.

    Does not remove src, is similar to cp for now.
    """

    def transfer_remote(self) -> None:
        """Move files/diretories between remotes."""
        self.fs.mv(
            self.args.path1, self.args.path2, recursive=self.args.recursive
        )


class CommandRemove(Command):
    """Command for rm."""

    def run(self) -> None:
        """Remove files/directories in the remote."""
        details = self.fs.info(self.args.path)
        if details["type"] == "directory" and not self.args.recursive:
            self.fs.rmdir(self.args.path)
        else:
            self.fs.rm(self.args.path, recursive=self.args.recursive)


class CommandRun(Command):
    """Command for run."""

    def run(self) -> None:
        """Runs the command from stdin/file."""
        if not sys.stdout.isatty() and not self.args.path:
            raise ValueError("no path specified or contents piped")

        count = 0
        for line in fileinput.input(files=(self.args.path or "-",)):
            if line.startswith("#"):
                continue

            cmd, *args = line.strip().split()
            subparsers = self.args.subparsers
            if cmd not in subparsers:
                raise ValueError(f"unknown command: {cmd}")

            subparser = subparsers[cmd]
            sub_args = subparser.parse_args(args)

            namespace = Namespace(**vars(self.args))
            namespace.__dict__.update(vars(sub_args))
            logger.debug(str(vars(namespace)))

            print("\n" if count else "", line, sep="", end="")
            run_cmd(namespace, self.fs)
            count += 1


class CommandCat(Command):
    """Command for cat."""

    def run(self) -> None:
        """Prints the content of the file."""
        with self.fs.open(self.args.path, mode="r") as fobj:
            print(fobj.read())


def is_fqpath(fs: AbstractFileSystem, path: str) -> bool:
    """Check if the path is fully qualified."""
    path = stringify_path(path)
    protos = (fs.protocol,) if isinstance(fs.protocol, str) else fs.protocol
    for protocol in protos:
        if path.startswith(protocol + "://"):
            return True
    return False


class CommandSync(Command):
    """Command for sync."""

    @staticmethod
    def copy_fs(
        src: str,
        dest: str,
        src_fs: AbstractFileSystem,
        dest_fs: AbstractFileSystem,
        recursive: bool = False,
    ) -> None:
        """Supports copy in local-remote, remote-local, and remote-remote."""
        assert not (
            isinstance(src_fs, LocalFileSystem)
            and isinstance(dest_fs, LocalFileSystem)
        )

        if isinstance(src_fs, LocalFileSystem):
            dest_fs.upload(src, dest, recursive=recursive)
        elif isinstance(dest_fs, LocalFileSystem):
            if src_fs.isdir(src):
                dest_fs.makedirs(dest, exist_ok=True)
            src_fs.download(src, dest, recursive=recursive)
        else:
            dest_fs.copy(src, dest, recursive=recursive)

    @classmethod  # noqa: C901
    def changed(  # noqa: C901
        cls, src_details: Dict[str, Any], dest_details: Dict[str, Any]
    ) -> bool:
        """See if a src and dest have changed or not."""

        def get_mtime(info: Dict[str, Any]) -> Optional[datetime]:
            mtime = (
                info.get("mtime")
                or info.get("modified")
                or info.get("created")
            )
            if isinstance(mtime, float):
                return datetime.utcfromtimestamp(mtime).replace(
                    tzinfo=timezone.utc
                )
            return mtime

        src_type = src_details.get("type")
        src_size = src_details.get("size")
        src_modified = get_mtime(src_details)

        dest_type = dest_details.get("type")
        dest_size = dest_details.get("size")
        dest_modified = get_mtime(dest_details)

        if not dest_details:
            return True
        if src_type == dest_type == "file":
            if src_size != dest_size:
                return True
            if src_modified and dest_modified and src_modified > dest_modified:
                return True
        if src_type == dest_type == "directory":
            return True
        if src_type != dest_type:
            raise TypeError(
                "cannot sync between different types, "
                f"src is {src_type}, "
                f"dest is {dest_type}"
            )
        return False

    @classmethod
    def diff(
        cls,
        src_info: Dict[str, Dict[str, Any]],
        dest_info: Dict[str, Dict[str, Any]],
    ) -> Tuple[List[str], Set[str]]:
        """Diff between src info and dest info to see if they have changed."""
        only_in_dest: Set[str] = set(dest_info) - set(src_info)
        changed: List[str] = []
        for file, src_d in src_info.items():
            dest_d = dest_info.get(file)
            if cls.changed(src_d, dest_d or {}):
                changed.append(file)

        return changed, only_in_dest

    @staticmethod
    def _transform_info(info: Dict[str, Any], rel: str) -> Dict[str, Any]:
        """Convert to relative paths for easier diff comparison."""
        return {os.path.relpath(k, rel): v for k, v in info.items()}

    def sync(
        self,
        src: str,
        dest: str,
        src_fs: AbstractFileSystem,
        dest_fs: AbstractFileSystem,
    ) -> None:
        """Sync between src and dest."""
        details_src = src_fs.info(src)
        try:
            details_dest = dest_fs.info(dest)
        except IOError:
            isdir = details_src.get("type") == "directory"
            print(
                "copy:",
                theme.style_path(src, isdir=isdir),
                "to",
                theme.style_path(dest, isdir=isdir),
            )
            self.copy_fs(src, dest, src_fs, dest_fs, recursive=True)
            return None

        if details_src["type"] == "file" and self.changed(
            details_src, details_dest
        ):
            print("copy:", theme.style_path(src), "to", theme.style_path(dest))
            self.copy_fs(src, dest, src_fs, dest_fs, recursive=False)
            return None

        src_info = src_fs.find(src, withdirs=True, detail=True, maxdepth=1)
        dest_info = dest_fs.find(dest, withdirs=True, detail=True, maxdepth=1)

        src_d = self._transform_info(src_info, src)
        dest_d = self._transform_info(dest_info, dest)
        changed, only_in_dest = self.diff(src_d, dest_d)

        for file in changed:
            new_src = posixpath.join(src, file)
            new_dest = posixpath.join(dest, file)
            self.sync(new_src, new_dest, src_fs, dest_fs)

        for file in only_in_dest if self.args.delete else []:
            new_dest = posixpath.join(dest, file)
            is_dir = dest_d[file].get("type") == "directory"
            print("delete:", theme.style_path(new_dest, isdir=is_dir))
            dest_fs.rm(new_dest, recursive=True)
        return None

    def run(self) -> None:
        """Run sync between src and dest."""
        src = self.args.path1
        dest = self.args.path2

        src_is_fq = is_fqpath(self.fs, src)
        dest_is_fq = is_fqpath(self.fs, dest)

        if src_is_fq and dest_is_fq:
            src_fs, dest_fs = self.fs, self.fs
        elif src_is_fq:
            src_fs, dest_fs = self.fs, LocalFileSystem()
        else:
            src_fs, dest_fs = LocalFileSystem(), self.fs

        # pylint: disable=protected-access
        src = src_fs._strip_protocol(src)
        dest = dest_fs._strip_protocol(dest)
        return self.sync(src, dest, src_fs, dest_fs)


class CommandMkdir(Command):
    """Command for mkdir."""

    def run(self) -> None:
        """Create directory."""
        self.fs.mkdir(self.args.path, create_parents=self.args.parents)


class CommandDiskUsage(Command):
    """Command for du."""

    def run(self) -> None:
        """Displays disk usages for the given path."""
        path = self.args.path
        isdir = True
        details = self.fs.find(path, detail=True, withdirs=False)
        if len(details) == 1 and self.fs.isfile(path):
            isdir = False
            details = {path: self.fs.info(path)}

        size = sum(info.get("size", 0) for _, info in details.items())
        sized = Size(*human_size(size))
        size_styled = theme.style_size(sized.nbytes, suff=sized.suff)
        path_styled = theme.style_path(path, isdir=isdir)
        files_str = "files" if len(details) > 1 else "file"
        print(
            size_styled,
            f"({size} bytes) in",
            len(details),
            f"{files_str}:",
            path_styled,
        )


def get_parser() -> Tuple["ArgumentParser", Dict[str, "ArgumentParser"]]:
    """Returns the parser and dict of subparsers.

    We use this dict of subparsers to parse commands in `run` command.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show more information",
        default=False,
    )
    parser.add_argument(
        "--endpoint-url",
        help="Endpoint url to connect to. Will be considered as a root path.\n"
        "Can also be specified through WEBDAV_ENDPOINT_URL envvar.",
        metavar="URL",
        default=None,
    )
    parser.add_argument(
        "--user", "-u", help="Account Username", default=None, required=False
    )
    parser.add_argument(
        "--password",
        "-p",
        help="Account Password",
        default=None,
        required=False,
    )

    subparsers = parser.add_subparsers(
        title="actions", help="Available subcommands"
    )
    subparsers.required = True
    subparsers.dest = "command"

    ls_parser = subparsers.add_parser("ls", help="List files")
    ls_parser.add_argument(
        "--recursive",
        "-R",
        default=False,
        action="store_true",
        help="Recurse into directories",
    )
    ls_parser.add_argument(
        "-L",
        "--level",
        required=False,
        default=None,
        type=int,
        help="Limit the depth of recursion",
    )
    ls_parser.add_argument(
        "--full-path",
        required=False,
        action="store_true",
        help="Show full path of the files and the directories",
    )
    ls_parser.add_argument(
        "path", nargs="?", default="dav://", help="Path to list from"
    )
    ls_parser.set_defaults(func=CommandLS)

    cp_parser = subparsers.add_parser(
        "cp",
        help="Copies a local file or remote file "
        "to another location locally or in remote.",
    )
    cp_parser.add_argument(
        "--recursive", "-R", default=False, action="store_true", help=""
    )
    cp_parser.add_argument(
        "path1",
        help="Path to copy from (locally or remotely if dav:// url is given)",
    )
    cp_parser.add_argument(
        "path2",
        help="Path to copy to (locally or remotely if dav:// url is given)",
    )
    cp_parser.set_defaults(func=CommandCopy)

    mv_parser = subparsers.add_parser(
        "mv",
        help="Moves a local file or remote file "
        "to another location locally or in remote.",
    )
    mv_parser.add_argument(
        "--recursive", "-R", default=False, action="store_true", help=""
    )
    mv_parser.add_argument(
        "path1",
        help="Path to move from (locally or remotely if dav:// url is given)",
    )
    mv_parser.add_argument(
        "path2",
        help="Path to move to (locally or remotely if dav:// url is given)",
    )
    mv_parser.set_defaults(func=CommandMove)

    rm_parser = subparsers.add_parser(
        "rm", help="Removes a file from the remote server."
    )
    rm_parser.add_argument(
        "--recursive", "-R", default=False, action="store_true", help=""
    )
    rm_parser.add_argument("path", help="Path to remove")
    rm_parser.set_defaults(func=CommandRemove)

    mkdir_parser = subparsers.add_parser(
        "mkdir", help="Creates a directory/collection in the remote server."
    )
    mkdir_parser.add_argument(
        "--parents",
        "-p",
        default=False,
        action="store_true",
        help="no error if existing, " "make parent directories as needed",
    )
    mkdir_parser.add_argument("path", help="Path to remove")
    mkdir_parser.set_defaults(func=CommandMkdir)

    run_parser = subparsers.add_parser("run", help="Run multiple commands")
    run_parser.add_argument(
        "path", metavar="FILE", help="files to read, if empty, stdin is used"
    )
    run_parser.set_defaults(func=CommandRun)

    cat_parser = subparsers.add_parser("cat", help="Print remote file content")
    cat_parser.add_argument("path", metavar="FILE", help="File to read")
    cat_parser.set_defaults(func=CommandCat)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Sync local file/folder to/from the remote file/directories",
    )
    sync_parser.add_argument(
        "path1",
        help="Path to move from (locally or remotely if dav:// url is given)",
    )
    sync_parser.add_argument(
        "path2",
        help="Path to move to (locally or remotely if dav:// url is given)",
    )
    sync_parser.add_argument(
        "--delete",
        action="store_true",
        default=False,
        help="Files that exist in the destination "
        "but not in the source are deleted during sync.",
    )
    sync_parser.set_defaults(func=CommandSync)

    du_parser = subparsers.add_parser("du", help="Print remote file content")
    du_parser.add_argument(
        "path",
        default="dav://",
        nargs="?",
        help="Show size of directories/files",
    )
    du_parser.set_defaults(func=CommandDiskUsage)

    return parser, {
        "ls": ls_parser,
        "cp": cp_parser,
        "mv": mv_parser,
        "rm": rm_parser,
        "cat": cat_parser,
        "sync": sync_parser,
        "du": du_parser,
        "mkdir": mkdir_parser,
    }


def run_cmd(args: Namespace, fs: AbstractFileSystem = None) -> Optional[int]:
    """Run cmd from given args."""
    cmd = cast(Command, args.func(args, fs=fs))
    cmd.run()
    return 0


def main(argv: List[str] = None) -> Optional[int]:
    """Command line entrypoint."""
    parser, subparsers = get_parser()
    args = parser.parse_args(argv)
    args.subparsers = subparsers

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        return run_cmd(args)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("%s: %s", type(exc).__name__, exc, exc_info=args.verbose)
        return 1
