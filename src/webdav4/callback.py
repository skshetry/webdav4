"""Utilities to wrap file objects for callback purposes."""
from collections.abc import Iterable
from functools import wraps
from typing import IO, TYPE_CHECKING, Any, AnyStr, Callable, Iterator, cast

if TYPE_CHECKING:
    from typing_extensions import Literal


CallbackFn = Callable[[int], Any]


class CallbackIOWrapper(Iterable):  # type: ignore[type-arg]
    """Wrap a file-like's read/write method to report length to callback."""

    def __init__(
        self,
        stream: IO[AnyStr],
        callback: CallbackFn = None,
        method: "Literal['read', 'write']" = "read",
    ) -> None:
        """Pass stream and callback and appropriate method to wrap."""
        if method not in {"read", "write"}:
            raise ValueError("Can only wrap read/write methods")

        def do_nothing(_: int) -> Any:
            pass

        cb_func = callback or do_nothing
        sread = stream.read

        @wraps(sread)
        def fread(*args: Any, **kwargs: Any) -> AnyStr:
            result = sread(*args, **kwargs)
            cb_func(len(result))
            return result

        swrite = stream.write

        @wraps(swrite)
        def fwrite(chunk: AnyStr, *args: Any, **kwargs: Any) -> int:
            result = swrite(chunk, *args, **kwargs)  # type: ignore[call-arg]
            cb_func(len(chunk))
            return result

        self.__dict__.update(
            {
                "__wrapped_stream__": stream,
                method: fread if method == "read" else fwrite,
                "__call_back__": cb_func,
            }
        )

    def __iter__(self) -> Iterator[AnyStr]:
        """Iterate through the stream and call callback method with size.

        Note: We need this because, HTTPX uses `in iter` when uploading files.
        """

        def func(chunk: AnyStr) -> AnyStr:
            self.__call_back__(len(chunk))
            return chunk

        return map(func, self.__wrapped_stream__)

    def __setattr__(self, attr: str, value: Any) -> Any:
        """Setting attr on the stream."""
        setattr(self.__wrapped_stream__, attr, value)

    def __getattr__(self, attr: str) -> Any:
        """Getting attr on the stream."""
        return getattr(self.__wrapped_stream__, attr)


def wrap_file_like(
    file_obj: IO[AnyStr],
    callback: CallbackFn = None,
    method: "Literal['read', 'write']" = "read",
) -> IO[AnyStr]:
    """Wrap a file-like object that reports to callback on r/w operation.

    This method exists to cast the type to the file-like on return.
    """
    wrapper = CallbackIOWrapper(file_obj, callback, method=method)
    return cast(IO[AnyStr], wrapper)
