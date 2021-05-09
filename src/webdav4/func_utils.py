"""Some functional utilities."""

from itertools import repeat
from typing import Callable, Iterator, TypeVar

_T = TypeVar("_T")


def repeat_func(func: Callable[[], _T], times: int = None) -> Iterator[_T]:
    """Repeatedly calls a function multiple times.

    It will call infinite times if not specified, otherwise as many times as
    specified in `times` arg.
    """
    args = (None, times) if times else (None,)
    return (func() for i in repeat(*args))
