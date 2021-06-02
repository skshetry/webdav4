"""fsspec compliant webdav file system."""
import errno
import io
import os
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    TextIO,
    Tuple,
    Type,
    Union,
)

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

from .client import (
    Client,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceNotFound,
)
from .func_utils import reraise

if TYPE_CHECKING:
    from datetime import datetime
    from os import PathLike
    from typing import AnyStr

    from .types import AuthTypes, URLTypes


mapping = {"content_length": "size", "path": "name", "type": "type"}


def translate_info(item: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Translate info from the client to as per fsspec requirements."""
    assert not isinstance(item, str)
    return {mapping.get(key, key): value for key, value in item.items()}


class WebdavFileSystem(AbstractFileSystem):
    """Provides access to webdav through fsspec-compliant APIs."""

    def __init__(
        self,
        base_url: "URLTypes",
        auth: "AuthTypes" = None,
        client: "Client" = None,
        **client_opts: Any,
    ) -> None:
        """Instantiate WebdavFileSystem with base_url and auth.

        Args:
            base_url: base url of the server
            auth: Authentication to the server
                Refer to HTTPX's auth for more information.
            client: Webdav client to use instead, useful for testing/mocking,
                or extending WebdavFileSystem.
            client_opts: Extra args that are passed to Webdav Client.
                (refer to it's documenting for more information).
        """
        super().__init__()
        self.client = client or Client(base_url, auth=auth, **client_opts)

    @reraise(ResourceNotFound, FileNotFoundError)
    def ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> List[Union[str, Dict[str, Any]]]:
        """`ls` implementation for fsspec, see fsspec for more information."""
        if not self.client.isdir(path):
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)

        data = self.client.ls(path, detail=detail)
        if not detail:
            return data

        return [translate_info(item) for item in data]

    @reraise(ResourceNotFound, FileNotFoundError)
    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        """Return information about the current path."""
        return translate_info(self.client.info(path))

    @reraise(ResourceNotFound, FileNotFoundError)
    def rm_file(self, path: str) -> None:
        """Remove a file."""
        # not checking if it's a directory as `rm` also passes a directory
        return self.client.remove(path)

    _rm = rm_file

    @reraise(ResourceNotFound, FileNotFoundError)
    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file from one path to the other."""
        # not checking if it's a directory as `cp` also passes a directory
        return self.client.copy(path1, path2)

    def rmdir(self, path: str) -> None:
        """Remove a directory, if empty."""
        if self.ls(path):
            raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
        return self.client.remove(path)

    def rm(
        self, path: str, recursive: bool = False, maxdepth: int = None
    ) -> None:
        """Delete files and directories."""
        if recursive and not maxdepth and self.isdir(path):
            return self.rm_file(path)
        super().rm(path, recursive=recursive, maxdepth=maxdepth)
        return None

    def copy(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        on_error: str = None,
        **kwargs: Any,
    ) -> None:
        """Copy files and directories."""
        if recursive and not kwargs.get("maxdepth") and self.isdir(path1):
            try:
                return self.cp_file(path1, path2)
            except FileNotFoundError:
                if on_error in (None, "ignore"):
                    return None
                raise

        if not recursive and self.isdir(path1):
            return self.makedirs(path2)

        super().copy(path1, path2, recursive=recursive, **kwargs)
        return None

    def mv(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: bool = None,
        **kwargs: Any,
    ) -> None:
        """Move a file/directory from one path to the other."""
        if recursive and not maxdepth and self.isdir(path1):
            return self.client.move(path1, path2)

        if not recursive and self.isdir(path1):
            return self.makedirs(path2)

        super().mv(
            path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs
        )
        return None

    def _mkdir(self, path: str, exist_ok: bool = False) -> None:
        """Creates directory and translates to an appropriate exceptions.

        Internally, Client tries to do as much less API call as possible.
        And, usually, the ResourceAlreadyExists and ResourceConflict are
        enough for a standard server. But, in fsspec, for better exceptions,
        we have to distinguish between different conditions:

        1. parent not being a directory (NotADirectoryError),
        2. parent does not exists (FileExistsError),
        3. path already exists (FileNotFoundError), etc.

        We also spend some API calls to be sure on error.
        """
        try:
            return self.client.mkdir(path)
        except ResourceAlreadyExists as exc:
            details = self.info(path)
            if details and details["type"] == "directory" and exist_ok:
                return None

            raise FileExistsError(errno.EEXIST, "File exists", path) from exc
        except ResourceConflict as exc:
            parent = self._parent(path)
            details = self.info(parent)
            if details["type"] == "directory":
                raise  # pragma: no cover
            raise NotADirectoryError(
                errno.ENOTDIR, "Not a directory", parent
            ) from exc

    def mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        """Create directory."""
        if create_parents:
            return self.makedirs(path, exist_ok=True)
        return self._mkdir(path)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Creates directory to the given path."""
        parent = self._parent(path)
        if not ({"", self.root_marker} & {path, parent}) and not self.exists(
            parent
        ):
            self.makedirs(parent, exist_ok=exist_ok)

        return self._mkdir(path, exist_ok=exist_ok)

    @reraise(ResourceNotFound, FileNotFoundError)
    def created(self, path: str) -> Optional["datetime"]:
        """Returns creation time/date."""
        return self.client.created(path)

    @reraise(ResourceNotFound, FileNotFoundError)
    def modified(self, path: str) -> Optional["datetime"]:
        """Returns last modified time/data."""
        return self.client.modified(path)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: Dict[str, str] = None,
        **kwargs: Any,
    ) -> "WebdavFile":
        """Return a file-like object from the filesystem."""
        size = kwargs.pop("size", None)
        if mode == "rb" and not size:
            size = self.size(path)

        return WebdavFile(
            self,
            path,
            block_size=block_size,
            autocommit=autocommit,
            mode=mode,
            size=size,
            cache_options=cache_options,
            **kwargs,
        )

    @reraise(ResourceNotFound, FileNotFoundError)
    def checksum(self, path: str) -> Optional[str]:
        """Returns checksum/etag of the path."""
        return self.client.etag(path)

    @reraise(ResourceNotFound, FileNotFoundError)
    def size(self, path: str) -> Optional[int]:
        """Returns size of the path."""
        return self.client.content_length(path)

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> None:
        """Create a signed URL representing the given path."""
        raise NotImplementedError

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Upload the contents to given file in the remote webdav server."""
        buff = io.BytesIO(value)
        kwargs.setdefault("overwrite", True)
        # maybe it's not a bad idea to make a `self.open` for `mode="rb"`
        # on top of `io.BytesIO`?
        self.client.upload_fileobj(buff, path, **kwargs)

    def put_file(
        self, lpath: "PathLike[AnyStr]", rpath: str, **kwargs: Any
    ) -> None:
        """Copy file to remote webdav server."""
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            self.mkdirs(os.path.dirname(rpath), exist_ok=True)
            kwargs.setdefault("overwrite", True)
            self.client.upload_file(lpath, rpath, **kwargs)

    def touch(self, path: str, truncate: bool = True, **kwargs: Any) -> None:
        """Create empty file, or update timestamp (not supported yet)."""
        if truncate or not self.exists(path):
            kwargs.setdefault("overwrite", True)
            return self.client.upload_fileobj(io.BytesIO(), path, **kwargs)

        # might be a bad idea to add support for
        # ownCloud/nextCloud do seem to support this
        # if there is a need for this, we would need
        # to add support for `PROPSET` to update `lastmodified`
        # in the `Client`.
        raise NotImplementedError


class WebdavFile(AbstractBufferedFile):
    """WebdavFile that provides file-like access to remote file."""

    def __init__(
        self,
        fs: "WebdavFileSystem",
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: Dict[str, str] = None,
        **kwargs: Any,
    ) -> None:
        """Instantiate a file-like object with the provided options.

        See fsspec for more information.
        """
        if mode not in {"rt", "rb", "r"}:
            raise NotImplementedError("File mode not supported")

        size = kwargs.get("size")
        self.details = {"name": path, "size": size, "type": "file"}
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )
        encoding = kwargs.get("encoding")
        self.fobj = fs.client.open(
            self.path,
            mode=self.mode,
            encoding=encoding,
            block_size=self.blocksize,
        )
        self.reader: Union[TextIO, BinaryIO] = self.fobj.__enter__()
        self.closed: bool = False

    def read(self, length: int = -1) -> Union[str, bytes, None]:
        """Read chunk of bytes."""
        chunk = self.reader.read(length)
        if chunk:
            self.loc += len(chunk)
        return chunk

    def __enter__(self) -> "WebdavFile":
        """Start streaming."""
        return self

    def _fetch_range(self, start: int, end: int) -> None:
        """Not essential. Creating stub to make pylint happy."""
        raise NotImplementedError

    def close(self) -> None:
        """Close stream."""
        if self.closed:
            return

        self.closed = True
        self.reader.close()

    def __reduce_ex__(
        self, protocol: int
    ) -> Tuple[Callable[["ReopenArgs"], Type["WebdavFile"]], "ReopenArgs"]:
        """Recreate/reopen file when restored."""
        return reopen, ReopenArgs(  # pragma: no cover
            WebdavFile,
            self.fs,
            self.path,
            self.blocksize,
            self.mode,
            self.size,
        )


class ReopenArgs(NamedTuple):  # pylint: disable=inherit-non-class
    """Args to reopen the file."""

    file: Type[WebdavFile]
    fs: "WebdavFileSystem"
    path: str
    blocksize: Optional[int]
    mode: str
    size: Optional[int]


def reopen(args: ReopenArgs) -> WebdavFile:
    """Reopen file when unpickled."""
    return args.file(  # pragma: no cover
        args.fs,
        args.path,
        blocksize=args.blocksize,
        mode=args.mode,
        size=args.size,
    )