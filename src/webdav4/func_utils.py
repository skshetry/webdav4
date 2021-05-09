"""Some functional utilities."""

import time
from functools import wraps
from itertools import repeat
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Type,
    TypeVar,
    Union,
    cast,
    no_type_check,
)

_T = TypeVar("_T")
_F = TypeVar("_F", bound=Callable[..., Any])


def repeat_func(func: Callable[[], _T], times: int = None) -> Iterator[_T]:
    """Repeatedly calls a function multiple times.

    It will call infinite times if not specified, otherwise as many times as
    specified in `times` arg.
    """
    args = (None, times) if times else (None,)
    return (func() for i in repeat(*args))


def retry(  # noqa: C901
    retries: int,
    errors: Iterable[Type[Exception]],
    timeout: Union[float, Callable[[int], float]] = 0,
    filter_errors: Callable[[Exception], bool] = None,
) -> Callable[..., _T]:
    """Retry a given function."""
    _errors = tuple(errors)

    def wrapped_function(func: Callable[..., _T]) -> _T:
        for attempt in range(retries):
            try:
                return func()
            except _errors as exc:  # pylint: disable=catching-non-exception
                if not (filter_errors is None or filter_errors(exc)):
                    raise
                if attempt + 1 == retries:  # reraise on the last attempt
                    raise

            value = timeout(attempt) if callable(timeout) else timeout
            # TODO: do we need circuit breaking?
            time.sleep(value)

        raise AssertionError("it should never reach here")  # pragma: no cover

    return wrapped_function


def reraise(
    catch: Type[Exception], _raise: Union[Exception, Type[Exception]]
) -> Callable[[_F], _F]:
    """Catches an exception and raises it as a different one."""

    def decorated(func: _F) -> _F:
        @no_type_check
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except catch as exc:
                raise _raise from exc

        return cast(_F, wrapped_function)

    return decorated


def wrap_fn(
    func: Callable[..., _T], *args: Any, **kwargs: Any
) -> Callable[[], _T]:
    """Wraps a function and turns into a 0-arity function."""

    @wraps(func)
    @no_type_check
    def wrapped():
        return func(*args, **kwargs)

    return cast("Callable[[], _T]", wrapped)
