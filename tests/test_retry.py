"""Test retrying in the client."""
from http import HTTPStatus
from unittest import mock

import pytest
from httpx import Request, Response

from webdav4.client import BadGatewayError, HTTPError, ResourceLocked
from webdav4.retry import retry

request = Request("get", "example.com")


@pytest.mark.parametrize(
    "exc",
    [
        BadGatewayError(),
        HTTPError(
            Response(HTTPStatus.TOO_MANY_REQUESTS.value, request=request)
        ),
        ResourceLocked("beep boop bop"),
    ],
)
def test_retry(exc):
    """Test that the function is rerun/retried on failure."""
    response = Response(HTTPStatus.OK.value)
    func = mock.MagicMock(side_effect=[exc, response])
    assert retry(True)(func) == response
    assert func.call_count == 2


@pytest.mark.parametrize(
    "status_code, retries ",
    [
        # this is not handled, so it should not retry
        (HTTPStatus.CONFLICT, 1),
        # it tries to retry in case of this error
        (HTTPStatus.SERVICE_UNAVAILABLE, 3),
    ],
)
def test_retry_negative(status_code, retries):
    """Test retry but with the end-result being always a failure.

    Example: one case where it is asked not to be handled and the other case
    where even though the error is asked to be handled, every attempt fails
    and reaches maximum retries allowed.
    """
    func = mock.MagicMock(
        side_effect=HTTPError(Response(status_code.value, request=request))
    )
    with pytest.raises(HTTPError) as exc_info:
        retry(True)(func)
    assert exc_info.value.status_code == status_code
    assert func.call_count == retries
