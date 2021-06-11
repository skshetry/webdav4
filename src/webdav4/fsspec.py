"""fsspec compliant webdav file system."""
import errno
import io
import os
import tempfile
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    NoReturn,
    Optional,
    TextIO,
    Tuple,
    Type,
    Union,
    cast,
)

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

from .client import (
    Client,
    IsACollectionError,
    IsAResourceError,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceNotFound,
)
from .stream import read_into

if TYPE_CHECKING:
    from array import ArrayType
    from datetime import datetime
    from mmap import mmap
    from os import PathLike
    from typing import AnyStr

    from .types import AuthTypes, URLTypes


mapping = {"content_length": "size", "path": "name", "type": "type"}


def translate_info(item: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Translate info from the client to as per fsspec requirements."""
    assert not isinstance(item, str)
    return {mapping.get(key, key): value for key, value in item.items()}


@contextmanager
def translate_exceptions() -> Iterator[None]:
    """Translate exceptions from Client to the fsspec compatible one."""
    try:
        yield
    except ResourceNotFound as exc:
        raise FileNotFoundError(
            errno.ENOENT, "No such file or directory", exc.path
        ) from exc
    except IsACollectionError as exc:
        raise IsADirectoryError(
            errno.EISDIR, "Is a directory", exc.path
        ) from exc
    except IsAResourceError as exc:
        raise NotADirectoryError(
            errno.ENOTDIR, "Not a directory", exc.path
        ) from exc


class WebdavFileSystem(AbstractFileSystem):
    """Provides access to webdav through fsspec-compliant APIs."""

    protocol = ("webdav", "dav")

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
        client_opts.setdefault("chunk_size", self.blocksize)
        self.client = client or Client(base_url, auth=auth, **client_opts)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Strips protocol from the given path, overriding for type-casting."""
        stripped = super()._strip_protocol(path)
        return cast(str, stripped)

    @translate_exceptions()
    def ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> List[Union[str, Dict[str, Any]]]:
        """`ls` implementation for fsspec, see fsspec for more information."""
        path = self._strip_protocol(path).strip()
        data = self.client.ls(
            path, detail=detail, allow_listing_resource=False
        )
        if not detail:
            return data
        return [translate_info(item) for item in data]

    @translate_exceptions()
    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        """Return information about the current path."""
        path = self._strip_protocol(path)
        return translate_info(self.client.info(path))

    @translate_exceptions()
    def rm_file(self, path: str) -> None:
        """Remove a file."""
        path = self._strip_protocol(path)
        # not checking if it's a directory as `rm` also passes a directory
        return self.client.remove(path)

    _rm = rm_file

    @translate_exceptions()
    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file from one path to the other."""
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        # not checking if it's a directory as `cp` also passes a directory
        return self.client.copy(path1, path2)

    def rmdir(self, path: str) -> None:
        """Remove a directory, if empty."""
        path = self._strip_protocol(path)
        if self.ls(path):
            raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
        return self.client.remove(path)

    def rm(
        self, path: str, recursive: bool = False, maxdepth: int = None
    ) -> None:
        """Delete files and directories."""
        path = self._strip_protocol(path)
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
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)

        if recursive and not kwargs.get("maxdepth") and self.isdir(path1):
            return self.cp_file(path1, path2)

        if not recursive and self.isdir(path1):
            return self.makedirs(path2)

        super().copy(
            path1, path2, recursive=recursive, on_error=on_error, **kwargs
        )
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
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)

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
        path = self._strip_protocol(path)
        if create_parents:
            return self.makedirs(path, exist_ok=True)
        return self._mkdir(path)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Creates directory to the given path."""
        path = self._strip_protocol(path)
        parent = self._parent(path)
        if not ({"", self.root_marker} & {path, parent}) and not self.exists(
            parent
        ):
            self.makedirs(parent, exist_ok=exist_ok)

        return self._mkdir(path, exist_ok=exist_ok)

    @translate_exceptions()
    def created(self, path: str) -> Optional["datetime"]:
        """Returns creation time/date."""
        path = self._strip_protocol(path)
        return self.client.created(path)

    @translate_exceptions()
    def modified(self, path: str) -> Optional["datetime"]:
        """Returns last modified time/data."""
        path = self._strip_protocol(path)
        return self.client.modified(path)

    @translate_exceptions()
    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: Dict[str, str] = None,
        **kwargs: Any,
    ) -> Union["WebdavFile", "UploadFile"]:
        """Return a file-like object from the filesystem."""
        size = kwargs.pop("size", None)
        assert "a" not in mode

        if "x" in mode and self.exists(path):
            raise FileExistsError(errno.EEXIST, "File exists", path)
        if set(mode) & {"w", "x"}:
            return UploadFile(
                self, path=path, mode=mode, block_size=block_size
            )

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

    @translate_exceptions()
    def checksum(self, path: str) -> Optional[str]:
        """Returns checksum/etag of the path."""
        path = self._strip_protocol(path)
        return self.client.etag(path)

    @translate_exceptions()
    def size(self, path: str) -> Optional[int]:
        """Returns size of the path."""
        path = self._strip_protocol(path)
        return self.client.content_length(path)

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> None:
        """Create a signed URL representing the given path."""
        raise NotImplementedError

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Upload the contents to given file in the remote webdav server."""
        path = self._strip_protocol(path)
        buff = io.BytesIO(value)
        kwargs.setdefault("overwrite", True)
        # maybe it's not a bad idea to make a `self.open` for `mode="rb"`
        # on top of `io.BytesIO`?
        self.client.upload_fileobj(buff, path, **kwargs)

    def put_file(
        self, lpath: "PathLike[AnyStr]", rpath: str, **kwargs: Any
    ) -> None:
        """Copy file to remote webdav server."""
        rpath = self._strip_protocol(rpath)
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            self.mkdirs(os.path.dirname(rpath), exist_ok=True)
            kwargs.setdefault("overwrite", True)
            self.client.upload_file(lpath, rpath, **kwargs)


class WebdavFile(AbstractBufferedFile):
    """WebdavFile that provides file-like access to remote file."""

    size: int

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
            chunk_size=self.blocksize,
        )
        self.reader: Union[TextIO, BinaryIO] = self.fobj.__enter__()

        # only get the file size if GET request didnot send Content-Length
        # or was retrieved before.
        if not self.size:
            if getattr(self.reader, "size", None):
                self.size = self.reader.size  # type: ignore
            else:
                self.size = self.fs.size(self.path)

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

    def seek(self, loc: int, whence: int = 0) -> int:
        """Set current file location."""
        super().seek(loc, whence=whence)
        return self.reader.seek(loc, whence=whence)

    def isatty(self) -> bool:
        """Check if it is an interactive fileobj."""
        return False

    def close(self) -> None:
        """Close stream."""
        if self.closed:
            return
        if hasattr(self, "reader"):
            # fs.client.open might have raised an error
            self.reader.close()
        self.closed = True

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


# TODO: if we need this to be serializable, we might want to use tempfile
#  directly.


class UploadFile(tempfile.SpooledTemporaryFile):
    """UploadFile for Webdav, that uses SpooledTemporaryFile.

    In Webdav, you cannot upload in chunks. Similar to http, we need some kind
    of protocol on top to be able to do that. Sabredav provides a way to upload
    chunks, but it's not implemented in Owncloud and Nextcloud. They have a
    different chunking mechanism. On top of it, there are TUS implementations
    in newer version of Owncloud.

    However, as they are only for chunking, it needs to be seen if it is
    possible to implement full-breadth of FileObj API through that (I doubt it)

    Note that, fsspec's put_file/pipe_file don't use this technique and are
    thus faster as they don't need buffering. It is recommended to use that
    if you don't need to upload content dynamically.
    """

    def __init__(  # pylint: disable=invalid-name
        self,
        fs: "WebdavFileSystem",
        path: str,
        mode: str = "wb",
        block_size: int = None,
    ):
        """Extended interface with path and fs."""
        assert fs
        assert path
        self.blocksize = (
            AbstractBufferedFile.DEFAULT_BLOCK_SIZE
            if block_size in ["default", None]
            else block_size
        )
        self.fs: WebdavFileSystem = fs  # pylint: disable=invalid-name
        assert mode
        self.path: str = path

        # whatever the mode be, we should try to open the file
        # in both rw mode.
        super().__init__(max_size=self.blocksize, mode="wb+")

    def __exit__(self, *exc: Any) -> None:
        """Upload file by seeking to first byte on exit."""
        self.close()

    def readable(self) -> bool:  # pylint: disable=no-self-use
        """It is readable."""
        return True

    def writable(self) -> bool:  # pylint: disable=no-self-use
        """It is writable."""
        return True

    def seekable(self) -> bool:  # pylint: disable=no-self-use
        """It is seekable."""
        return True

    def commit(self) -> None:
        """Commits the file to the given path.

        As we cannot upload in chunk, this is where we really upload file.
        """
        self.seek(0)
        fileobj = cast(BinaryIO, self)
        self.fs.client.upload_fileobj(
            fileobj, self.path, chunk_size=self.blocksize, overwrite=True
        )

    def close(self) -> None:
        """Close the file."""
        if not self.closed:
            self.commit()
            super().close()

    def discard(self) -> None:
        """Discard the file."""
        if not self.closed:
            super().close()

    def info(self) -> NoReturn:  # pylint: disable=no-self-use
        """Info about the file upload that is in progress."""
        raise ValueError("cannot provide info in write-mode")

    def readinto(
        self, sequence: Union[bytearray, memoryview, "ArrayType[Any]", "mmap"]
    ) -> int:
        """Read bytes into the given buffer."""
        return read_into(self, sequence)

    def readuntil(self, char: bytes = b"\n", blocks: int = None) -> bytes:
        """Read until the given character is found."""
        ret = AbstractBufferedFile.readuntil(self, char=char, blocks=blocks)
        return cast(bytes, ret)
