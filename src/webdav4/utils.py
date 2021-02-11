"""Common file utilities."""
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Optional,
    TextIO,
    Union,
)

from dateutil.parser import parse
from httpx._utils import peek_filelike_length

if TYPE_CHECKING:
    from datetime import datetime

try_to_guess_filelength = peek_filelike_length


def patch_file_like_read(
    file_obj: Union[TextIO, BinaryIO], callback: Optional[Callable[[int], Any]]
) -> None:
    """Patch file-like object's read with the one that supports callback."""
    if not callback:
        return None

    func = file_obj.read

    @wraps(func)
    def wrapper(
        _self: Union[TextIO, BinaryIO], item: int = -1
    ) -> Optional[Union[str, bytes]]:
        res = func(item)
        assert callback
        if res is not None:
            callback(len(res))
        return res

    file_obj.read = wrapper  # type: ignore[assignment]
    return None


def fromisoformat(datetime_string: str) -> "datetime":
    """Convert ISO 8601 datetime string to datetime object."""
    return parse(datetime_string)
