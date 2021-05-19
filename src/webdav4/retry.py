"""Retry mechanism for the webdav Client."""
from http import HTTPStatus
from typing import Callable, TypeVar, cast

from .func_utils import retry as retry_func
from .http import BANDWIDTH_LIMIT_EXCEEDED

_T = TypeVar("_T")

BACKOFF: float = 1


def filter_errors(exc: Exception) -> bool:
    """Filter these errors and retry if they fall in these categories."""
    # pylint: disable=import-outside-toplevel
    from .client import HTTPError

    if isinstance(exc, HTTPError):
        return exc.status_code in (
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
            BANDWIDTH_LIMIT_EXCEEDED,
        )
    return True


def _exp_backoff(attempt: int) -> float:
    """Backoff exponentially."""
    # for some reason, mypy is unable to figure out types
    return cast(float, BACKOFF * 2 ** attempt)


def retry(
    arg: bool = False, tries: int = 3
) -> Callable[[Callable[[], _T]], _T]:
    """Retry if arg upto `tries` times."""
    # pylint: disable=import-outside-toplevel
    from .client import BadGatewayError, HTTPError, ResourceLocked

    return retry_func(
        tries if arg is True else 1,
        (ResourceLocked, BadGatewayError, HTTPError),
        timeout=_exp_backoff,
        filter_errors=filter_errors,
    )
