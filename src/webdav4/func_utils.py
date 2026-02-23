"""Some functional utilities."""

import time
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from typing_extensions import ParamSpec

    P = ParamSpec("P")

_T = TypeVar("_T")


def retry(
    retries: int,
    errors: Iterable[Type[Exception]],
    timeout: Union[float, Callable[[int], float]] = 0,
    filter_errors: Optional[Callable[[Exception], bool]] = None,
) -> Callable[[Callable[[], _T]], _T]:
    """Retry a given function."""
    _errors = tuple(errors)

    def wrapped_function(func: Callable[[], _T]) -> _T:
        for attempt in range(retries):
            try:
                return func()
            except _errors as exc:
                if not (filter_errors is None or filter_errors(exc)):
                    raise
                if attempt + 1 == retries:  # reraise on the last attempt
                    raise

            value = timeout(attempt) if callable(timeout) else timeout
            # TODO: do we need circuit breaking?
            time.sleep(value)

        raise AssertionError("it should never reach here")  # pragma: no cover

    return wrapped_function


def wrap_fn(
    func: "Callable[P, _T]", *args: "P.args", **kwargs: "P.kwargs"
) -> Callable[[], _T]:
    """Wraps a function and turns into a 0-arity function."""

    @wraps(func)
    def wrapped() -> _T:
        return func(*args, **kwargs)

    return wrapped
