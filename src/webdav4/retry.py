"""Retry mechanism for the webdav Client."""

import time
from http import HTTPStatus
from typing import Callable, Protocol, TypeVar

from .http import BANDWIDTH_LIMIT_EXCEEDED

_T = TypeVar("_T")

BACKOFF: float = 1


class RetryFunc(Protocol):
    """Retry function protocol."""

    def __call__(self, f: Callable[[], _T], /) -> _T:
        """Callable wrapper."""


def retry(arg: bool = False, tries: int = 3) -> RetryFunc:
    """Retry if arg up to `tries` times."""
    from .client import BadGatewayError, HTTPError, ResourceLocked  # noqa: PLC0415

    retries = tries if arg is True else 1

    def wrapper(func: Callable[[], _T]) -> _T:
        for attempt in range(retries):
            try:
                return func()
            except (ResourceLocked, BadGatewayError, HTTPError) as exc:
                if isinstance(exc, HTTPError) and exc.status_code not in (
                    HTTPStatus.TOO_MANY_REQUESTS,
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    HTTPStatus.GATEWAY_TIMEOUT,
                    BANDWIDTH_LIMIT_EXCEEDED,
                ):
                    raise
                if attempt + 1 == retries:
                    raise
            time.sleep(BACKOFF * 2**attempt)
        raise AssertionError("unreachable")  # pragma: no cover

    return wrapper
