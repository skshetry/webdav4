"""Handle streaming response for file."""

import typing
from io import DEFAULT_BUFFER_SIZE, RawIOBase
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Union

from .http import Method as HTTPMethod

if TYPE_CHECKING:
    from array import ArrayType
    from mmap import mmap

    from .http import Client as HTTPClient
    from .types import HTTPResponse, URLTypes


class IterStream(RawIOBase):
    """Create a streaming file-like object."""

    def __init__(
        self,
        client: "HTTPClient",
        url: "URLTypes",
        chunk_size: int = None,
        callback: Callable[[int], Any] = None,
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
        self.callback = callback
        self._iterator: Optional[Iterator[bytes]] = None

    @property
    def loc(self) -> int:
        """Keep track of location of the stream/file for callbacks."""
        return self._loc

    @loc.setter
    def loc(self, value: int) -> None:
        """Update location, and run callbacks."""
        self._loc = value
        if not self.callback:
            return

        self.callback(self._loc)

    def __enter__(self) -> "IterStream":
        """Send a streaming response."""
        self.response = response = self.client.send(
            self.request, stream=True, allow_redirects=True
        )
        if response.status_code == 404:
            raise FileNotFoundError(f"Can't open {self.url}")
        response.raise_for_status()
        return self

    def __exit__(self, *args: typing.Any) -> None:
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

    def readinto(
        self,
        sequence: Union[
            bytearray, memoryview, "ArrayType[typing.Any]", "mmap"
        ],
    ) -> int:
        """Read into the buffer."""
        out = memoryview(sequence).cast("B")
        data = self.read(out.nbytes)

        # https://github.com/python/typeshed/issues/4991
        out[: len(data)] = data  # type: ignore[assignment]
        return len(data)

    readinto1 = readinto
    read1 = read
