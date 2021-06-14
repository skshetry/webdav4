"""Handle streaming response for file."""
from contextlib import contextmanager
from functools import partial
from http import HTTPStatus
from io import DEFAULT_BUFFER_SIZE, RawIOBase
from itertools import takewhile
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from .func_utils import repeat_func
from .http import HTTPNetworkError, HTTPTimeoutException
from .http import Method as HTTPMethod

if TYPE_CHECKING:
    from array import ArrayType
    from mmap import mmap

    from .client import Client
    from .http import Client as HTTPClient
    from .types import HTTPResponse, URLTypes


Buffer = Union[bytearray, memoryview, "ArrayType[Any]", "mmap"]


def request(
    client: "HTTPClient", url: "URLTypes", pos: int = 0
) -> "HTTPResponse":
    """Streams a file from url from given position."""
    headers = {}
    if pos:
        headers.update({"Range": f"bytes={pos}-"})

    req = client.build_request(HTTPMethod.GET, url, headers=headers)
    response = client.send(req, stream=True, allow_redirects=True)
    return response


@contextmanager  # noqa: C901
def iter_url(  # noqa: C901
    client: "Client",
    url: "URLTypes",
    chunk_size: int = None,
    pos: int = 0,
) -> Iterator[Tuple["HTTPResponse", Iterator[bytes]]]:
    """Iterate over chunks requested from url.

    Reopens connection on network failure.
    """

    def gen(
        response: "HTTPResponse",
    ) -> Generator[bytes, None, None]:
        nonlocal pos
        try:
            while True:
                if (
                    response.status_code
                    == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE
                ):
                    return  # range request outside file
                response.raise_for_status()

                try:
                    for chunk in response.iter_bytes(chunk_size=chunk_size):
                        pos += len(chunk)
                        yield chunk
                    break
                except (HTTPTimeoutException, HTTPNetworkError):
                    response.close()
                    if not (
                        response.headers.get("Accept-Ranges") == "bytes"
                        or client.detected_features.supports_ranges
                    ):
                        raise
                    response = request(client.http, url, pos=pos)
        finally:
            response.close()

    response = request(client.http, url, pos=pos)
    chunks = gen(response)
    try:
        yield response, chunks
    finally:
        chunks.close()  # Ensure connection is closed


class IterStream(RawIOBase):
    """Create a streaming file-like object."""

    def __init__(
        self,
        client: "Client",
        url: "URLTypes",
        chunk_size: int = None,
    ) -> None:
        """Pass a iterator to stream through."""
        super().__init__()

        self.buffer = b""
        # setting chunk_size is not possible yet with httpx
        # though it is to be released in a new version.
        self.chunk_size = chunk_size or client.chunk_size
        self.client = client
        self.url = url
        self._loc: int = 0
        self._cm = iter_url(client, self.url, chunk_size=chunk_size)
        self.size: Optional[int] = None
        self._iterator: Optional[Iterator[bytes]] = None
        self._response: Optional["HTTPResponse"] = None

    @property
    def supports_ranges(self) -> bool:
        """Checks whether the server supports ranges for the resource.

        Even if it does not advertise, we'll use base url's OPTIONS request
        to see if the server supports Range header or not. And, we want to
        avoid checking that as much as possible.
        """
        response = self._response
        if response and response.headers.get("Accept-Ranges") == "bytes":
            return True
        # consider if checking Accept-Ranges from OPTIONS request on self.url
        # would be a better solution than using base url.
        return self.client.detected_features.supports_ranges

    @property
    def loc(self) -> int:
        """Keep track of location of the stream/file for callbacks."""
        return self._loc

    @loc.setter
    def loc(self, value: int) -> None:
        """Update location, and run callbacks."""
        self._loc = value

    def __enter__(self) -> "IterStream":
        """Send a streaming response."""
        #  pylint: disable=no-member
        response, self._iterator = self._cm.__enter__()
        # we don't want to get this on Ranged requests or retried ones
        content_length: str = response.headers.get("Content-Length", "")
        self._response = response
        self.size = int(content_length) if content_length.isdigit() else None
        return self

    def __exit__(self, *args: Any) -> None:
        """Close the response."""
        self.close()

    @property
    def encoding(self) -> Optional[str]:
        """Encoding of the response."""
        assert self._response
        return self._response.encoding

    def close(self) -> None:
        """Close response if not already."""
        if self._iterator or self._response:
            self._cm.__exit__(None, None, None)  # pylint: disable=no-member

        self._iterator = None
        self._response = None
        self.buffer = b""

    def seek(self, offset: int, whence: int = 0) -> int:  # noqa: C901
        """Seek the file object."""
        if whence == 0:
            loc = offset
        elif whence == 1:
            if offset >= 0:
                self.read(offset)
                return self.loc
            loc = self.loc + offset
        elif whence == 2:
            if not self.size:
                raise ValueError("cannot seek to the end of file")
            loc = self.size + offset
        else:
            raise ValueError(f"invalid whence ({whence}, should be 0, 1 or 2)")
        if loc < 0:
            raise ValueError("Seek before start of file")
        if loc and not self.supports_ranges:
            raise ValueError("server does not support ranges")

        self.close()
        self._cm = iter_url(
            self.client, self.url, pos=loc, chunk_size=self.chunk_size
        )
        #  pylint: disable=no-member
        self._response, self._iterator = self._cm.__enter__()
        self.loc = loc
        return self.loc

    def tell(self) -> int:
        """Return current position of the fileobj."""
        return self.loc

    @property
    def closed(self) -> bool:  # pylint: disable=invalid-overridden-method
        """Check whether the stream was closed or not."""
        return not any([self._response, self._iterator])

    def readable(self) -> bool:
        """Stream is readable."""
        return True

    def seekable(self) -> bool:
        """Stream is not seekable."""
        return True

    def writable(self) -> bool:
        """Stream not writable."""
        return False

    def readall(self) -> bytes:
        """Read all of the bytes."""
        return b"".join(iter(partial(self.read1, -1), b""))

    def read(self, num: int = -1) -> bytes:
        """Read n bytes at max."""
        if num < 0:
            return self.readall()

        buff = b""
        while len(buff) < num:
            chunk = self.read1(num - len(buff))
            if not chunk:
                break
            buff += chunk
        return buff

    def read1(self, num: int = -1) -> bytes:
        """Read at maximum once."""
        assert self._iterator
        try:
            chunk = self.buffer or next(self._iterator)
        except StopIteration:
            return b""

        if num <= 0:
            self.buffer = b""
            return chunk

        output, self.buffer = chunk[:num], chunk[num:]
        self.loc += len(output)
        return output

    def readinto(self, sequence: Buffer) -> int:
        """Read into the buffer."""
        return read_into(self, sequence)  # type: ignore

    def readinto1(self, sequence: Buffer) -> int:
        """Read into the buffer with 1 read at max."""
        return read_into(self, sequence, read_once=True)  # type: ignore


def read_chunks(obj: IO[AnyStr], chunk_size: int = None) -> Iterator[AnyStr]:
    """Read file object in chunks."""
    func = partial(obj.read, chunk_size or DEFAULT_BUFFER_SIZE)
    return takewhile(bool, repeat_func(func))


def split_chunk(part: AnyStr, char: AnyStr) -> Tuple[AnyStr, Optional[AnyStr]]:
    """Split chunk into two parts, based on given delimiter character.

    It will return tuple of those two chunks. If no such delim exists,
    the first element of tuple will have the complete chunk whereas the other
    will be None.
    """
    found = part.find(char)
    if found > -1:
        return part[: found + len(char)], part[found + len(char) :]

    return part, None


def read_until(obj: IO[AnyStr], char: str) -> Iterator[AnyStr]:  # noqa: C901
    """Read chunks until the char is reached."""
    is_bytes = isinstance(obj.read(0), bytes)
    # `c` and the `joiner` should be the same type as the file `obj` is of.
    assert char
    _char: AnyStr = cast(AnyStr, char.encode() if is_bytes else char)
    joiner: AnyStr = cast(AnyStr, b"" if is_bytes else "")

    reader = read_chunks(obj)
    leftover = None
    while True:
        out: List[AnyStr] = []
        while True:
            try:
                part = leftover or next(reader)
            except StopIteration:
                part = joiner

            if not part:
                if out:
                    yield joiner.join(out)
                return

            output, leftover = split_chunk(part, _char)
            out.append(output)
            if leftover is not None:
                break

        yield joiner.join(out)


def read_into(
    obj: IO[AnyStr], sequence: Buffer, read_once: bool = False
) -> int:
    """Read into the buffer."""
    out = memoryview(sequence).cast("B")
    func = obj.read1 if read_once else obj.read  # type: ignore
    data = func(out.nbytes)

    out[: len(data)] = data
    return len(data)
