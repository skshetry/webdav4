"""fsspec compliant webdav file system."""

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
    cast,
)

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

from .client import Client

if TYPE_CHECKING:
    from datetime import datetime

    from .types import AuthTypes, URLTypes


class WebdavFileSystem(AbstractFileSystem):
    """Provides access to webdav through fsspec-compliant APIs."""

    def __init__(
        self, base_url: "URLTypes", auth: "AuthTypes", client: "Client" = None
    ) -> None:
        """Instantiate WebdavFileSystem with base_url and auth.

        Args:
            base_url: base url of the server
            auth: Authentication to the server
                Refer to HTTPX's auth for more information.
            client: Webdav client to use instead, useful for testing/mocking,
                or extending WebdavFileSystem.
        """
        super().__init__()
        self.client = client or Client(base_url, auth=auth)

    def ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> List[Union[str, Dict[str, Any]]]:
        """`ls` implementation for fsspec, see fsspec for more information."""
        data = self.client.ls(path, detail=detail)
        if not detail:
            return data

        mapping = {"content_length": "size", "path": "name", "type": "type"}

        def extract_info(item: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
            assert not isinstance(item, str)
            return {
                mapping.get(key, key): value for key, value in item.items()
            }

        return [extract_info(item) for item in data]

    def rm_file(self, path: str) -> None:
        """Remove a file."""
        return self.client.remove(path)

    def _rm(self, path: str) -> None:
        """Old API for deleting single file, please use `rm_file` instead."""
        return self.rm_file(path)

    def mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        """Create directory."""
        if create_parents:
            return self.makedirs(path, exist_ok=True)
        return self.client.mkdir(path)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Creates directory to the given path."""
        return self.client.makedirs(path, exist_ok=exist_ok)

    def created(self, path: str) -> Optional["datetime"]:
        """Returns creation time/date."""
        return self.client.created(path)

    def modified(self, path: str) -> Optional["datetime"]:
        """Returns last modified time/data."""
        return self.client.modified(path)

    def mv(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: bool = None,
        **kwargs: Any,
    ) -> None:
        """Move a file/directory from one path to the other."""
        return self.client.move(path1, path2)

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file/directory from one path to the other."""
        return self.client.copy(path1, path2)

    def open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        cache_options: Dict[str, str] = None,
        **kwargs: Any,
    ) -> "WebdavFile":
        """Return a file-like object from the filesystem."""
        size = kwargs.get("size")
        if mode == "rb" and not size:
            size = self.size(path)

        return WebdavFile(
            self,
            path,
            block_size=block_size,
            mode=mode,
            size=size,
            cache_options=cache_options,
            **kwargs,
        )

    def checksum(self, path: str) -> Optional[str]:
        """Returns checksum/etag of the path."""
        return self.client.etag(path)

    def size(self, path: str) -> Optional[int]:
        """Returns size of the path."""
        return self.client.content_length(path)

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> None:
        """Create a signed URL representing the given path."""
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
        if mode != "rb":
            raise NotImplementedError("File mode not supported")

        size = kwargs.get("size") or self.size()
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
        self.reader: Optional[Union[TextIO, BinaryIO]] = None

    def read(self, length: int = -1) -> Union[str, bytes, None]:
        """Read chunk of bytes."""
        assert self.reader
        chunk = self.reader.read(length)
        if chunk is not None:
            self.loc += len(chunk)
        return chunk

    def __enter__(self) -> "WebdavFile":
        """Start streaming."""
        self.reader = self.fobj.__enter__()
        return self

    def _fetch_range(self, start: int, end: int) -> None:
        """Not essential. Creating stub to make pylint happy."""
        raise NotImplementedError

    def close(self) -> None:
        """Close stream."""
        closed = cast(bool, self.closed)
        if closed:
            return

        closed = True
        if self.reader:
            self.reader.close()
            self.reader = None

    def __reduce_ex__(
        self, protocol: int
    ) -> Tuple[Callable[["ReopenArgs"], Type["WebdavFile"]], "ReopenArgs"]:
        """Recreate/reopen file when restored."""
        return reopen, ReopenArgs(
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
    return args.file(
        args.fs,
        args.path,
        blocksize=args.blocksize,
        mode=args.mode,
        size=args.size,
    )
