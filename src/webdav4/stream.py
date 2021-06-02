"""Handle streaming response for file."""
from functools import partial
from io import DEFAULT_BUFFER_SIZE, RawIOBase
from itertools import takewhile
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from .callback import CallbackFn, do_nothing
from .func_utils import repeat_func
from .http import Method as HTTPMethod

if TYPE_CHECKING:
    from array import ArrayType
    from mmap import mmap

    from .http import Client as HTTPClient
    from .types import HTTPResponse, URLTypes

Buffer = Union[bytearray, memoryview, "ArrayType[Any]", "mmap"]


class IterStream(RawIOBase):
    """Create a streaming file-like object."""

    def __init__(
        self,
        client: "HTTPClient",
        url: "URLTypes",
        chunk_size: int = None,
        callback: CallbackFn = None,
    ) -> None:
        """Pass a iterator to stream through."""
        super().__init__()

        self.buffer = b""
        # setting chunk_size is not possible yet with httpx
        # though it is to be released in a new version.
        self.chunk_size = chunk_size or DEFAULT_BUFFER_SIZE
        self.request = client.build_request(HTTPMethod.GET, url)
        self.client = client
        self.url = url
        self.response: Optional["HTTPResponse"] = None
        self._loc: int = 0
        self.callback = callback or do_nothing
        self._iterator: Optional[Iterator[bytes]] = None

    @property
    def loc(self) -> int:
        """Keep track of location of the stream/file for callbacks."""
        return self._loc

    @loc.setter
    def loc(self, value: int) -> None:
        """Update location, and run callbacks."""
        self._loc = value
        self.callback(value - self._loc)

    def __enter__(self) -> "IterStream":
        """Send a streaming response."""
        self.response = response = self.client.send(
            self.request, stream=True, allow_redirects=True
        )
        response.raise_for_status()
        return self

    def __exit__(self, *args: Any) -> None:
        """Close the response."""
        self.close()

    @property
    def encoding(self) -> Optional[str]:
        """Encoding of the response."""
        assert self.response
        return self.response.encoding

    @property
    def iterator(self) -> Iterator[bytes]:
        """Iterating through streaming response."""
        if self._iterator is None:
            assert self.response
            self._iterator = self.response.iter_bytes()

        for chunk in self._iterator:
            self.loc += len(chunk)
            yield chunk

    def close(self) -> None:
        """Close response if not already."""
        if self.response:
            self.response.close()
            self.response = None
            self.buffer = b""

    @property
    def closed(self) -> bool:  # pylint: disable=invalid-overridden-method
        """Check whether the stream was closed or not."""
        return self.response is None

    def readable(self) -> bool:
        """Stream is readable."""
        return True

    def seekable(self) -> bool:
        """Stream is not seekable."""
        return False

    def writable(self) -> bool:
        """Stream not writable."""
        return False

    def readall(self) -> bytes:
        """Read all of the bytes."""
        return self.read()

    def read(self, n: int = -1) -> bytes:
        """Read n bytes at max."""
        try:
            chunk = self.buffer or next(self.iterator)
        except StopIteration:
            return b""

        if n <= 0:
            self.buffer = b""
            return chunk

        output, self.buffer = chunk[:n], chunk[n:]
        return output

    def readinto(self, sequence: Buffer) -> int:
        """Read into the buffer."""
        return read_into(self, sequence)  # type: ignore

    readinto1 = readinto
    read1 = read


def read_chunks(obj: IO[AnyStr], chunk_size: int = None) -> Iterator[AnyStr]:
    """Read file object in chunks."""
    func = partial(obj.read, chunk_size or 1024 * 1024)
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


def read_into(obj: IO[AnyStr], sequence: Buffer) -> int:
    """Read into the buffer."""
    out = memoryview(sequence).cast("B")
    data = obj.read(out.nbytes)

    # https://github.com/python/typeshed/issues/4991
    out[: len(data)] = data  # type: ignore[assignment]
    return len(data)
