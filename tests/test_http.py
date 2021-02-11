"""Testing http client."""

from unittest import mock

from webdav4.http import Client


def test_webdav_methods():
    """Test webdav methods that are added on httpx.Client."""
    url = "https://example.org"
    client = Client(auth=("user", "password"))
    response = mock.Mock()

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.propfind(url, data="<xml/>") == response
        m.assert_called_once_with("PROPFIND", url, data="<xml/>")

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.proppatch(url, data="<xml/>") == response
        m.assert_called_once_with("PROPPATCH", url, data="<xml/>")

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.mkcol(url) == response
        m.assert_called_once_with("MKCOL", url)

    headers = {"Destination": "https://example.com"}

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.copy(url, headers=headers) == response
        m.assert_called_once_with("COPY", url, headers=headers)

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.move(url, headers=headers) == response
        m.assert_called_once_with("MOVE", url, headers=headers)

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.lock(url) == response
        m.assert_called_once_with("LOCK", url)

    with mock.patch.object(client, "request", return_value=response) as m:
        assert client.unlock(url) == response
        m.assert_called_once_with("UNLOCK", url)
