"""A File abstraction that could be used in `fs.open()` calls."""

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    cast,
)

from fsspec.caching import AllBytes
from fsspec.spec import AbstractBufferedFile

if TYPE_CHECKING:
    from fsspec.caching import BaseCache

    from ._types import HTTPResponse, URLTypes
    from .fs import WebdavFileSystem


def yield_chunks(
    response: "HTTPResponse",
    expected_size: int = None,
) -> Iterable[bytes]:
    """Yield chunks in chunk_size, errors out if it crosses expected size."""
    size = 0
    for chunk in response.iter_bytes():
        size += len(chunk)
        # data size unknown, let's see if it goes too big
        if expected_size and size > expected_size:
            msg = (
                "Got more bytes so far "
                f"(>{size}) than requested ({expected_size})"
            )
            raise ValueError(msg)
        yield chunk


class WebdavFile(AbstractBufferedFile):
    """WebdavFile that provides file-like access to remote file."""

    def __init__(
        self,
        fs: "WebdavFileSystem",
        url: "URLTypes",
        block_size: int = None,
        mode: str = "rb",
        cache_type: str = "bytes",
        size: int = None,
        **kwargs: Any,
    ) -> None:
        """Instantiate a file-like object with the provided options.

        See fsspec for more information.
        """
        if mode != "rb":
            raise NotImplementedError("File mode not supported")
        self.url = url
        self.http = fs.client.http
        self.details = {"name": url, "size": size, "type": "file"}
        super().__init__(
            fs=fs,
            path=str(url),
            mode=mode,
            block_size=block_size,
            cache_type=cache_type,
            **kwargs,
        )
        self.fs = fs
        self.size: int = self.size
        self.cache: "BaseCache" = self.cache

    def read(self, length: int = -1) -> bytes:
        """Read bytes from file.

        Parameters
        ----------
        length: int
            Read up to this many bytes. If negative, read all content to end of
            file. If the server has not supplied the filesize, attempting to
            read only part of the data will raise a ValueError.
        """
        if (
            (length < 0 and self.loc == 0)
            or (length > (self.size or length))  # explicit read all
            or (  # read more than there is
                self.size and self.size < self.blocksize
            )  # all fits in one block anyway
        ):
            self._fetch_all()
        if self.size is None:
            if length < 0:
                self._fetch_all()
        else:
            length = min(self.size - self.loc, length)
        return cast(bytes, super().read(length))

    def _fetch_all(self) -> None:
        """Read whole file in one shot, without caching.

        This is only called when position is still at zero,
        and read() is called without a byte-count.
        """
        if isinstance(self.cache, AllBytes):
            return

        response = self.http.get(self.url)
        response.raise_for_status()
        out = response.read()
        self.cache = AllBytes(
            size=len(out), fetcher=None, blocksize=None, data=out
        )
        self.size = len(out)

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Download a block of data.

        The expectation is that the server returns only the requested bytes,
        with HTTP code 206. If this is not the case, we first check the
        headers, and then stream the output - if the data size is bigger than
        we requested, an exception is raised.
        """
        expected_size = end - start
        with self.http.stream(
            "GET", self.url, headers={"Range": f"bytes={start}-{end - 1}"}
        ) as response:
            if response.status_code == 416:
                return b""  # range request outside file

            response.raise_for_status()
            if response.status_code == 206:
                return response.read()  # partial content, as expected

            if "Content-Length" in response.headers:
                content_length = int(response.headers["Content-Length"])
                if content_length <= expected_size:
                    return response.read()  # data size OK
                msg = (
                    "Got more bytes "
                    f"({content_length}) than requested ({expected_size})"
                )
                raise ValueError(msg)

        return b"".join(yield_chunks(response, expected_size=expected_size))

    def commit(self) -> None:
        """Move from temp to final destination."""
        return self.fs.mv(self.location, self.path)

    def discard(self) -> None:
        """Discard temp. file."""
        return self.fs.rm_file(self.location)

    def __reduce__(
        self,
    ) -> Tuple[Callable[["ReopenArgs"], Type["WebdavFile"]], "ReopenArgs"]:
        """Recreate/reopen file when restored."""
        return reopen, ReopenArgs(
            WebdavFile,
            self.fs,
            self.url,
            self.blocksize,
            self.mode,
            self.cache.name,
            self.size,
        )


class ReopenArgs(NamedTuple):  # pylint: disable=inherit-non-class
    """Args to reopen the file."""

    file: Type["WebdavFile"]
    fs: "WebdavFileSystem"
    url: "URLTypes"
    blocksize: Optional[int]
    mode: str
    cache: Optional[str]
    size: Optional[int]


def reopen(args: "ReopenArgs") -> "WebdavFile":
    """Reopen file when unpickled."""
    return args.file(
        args.fs,
        args.url,
        blocksize=args.blocksize,
        mode=args.mode,
        cache=args.cache,
        size=args.size,
    )
