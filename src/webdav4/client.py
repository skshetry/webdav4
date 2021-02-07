"""Client for the webdav."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from urllib.parse import urljoin

from .http import URL
from .http import Client as HTTPClient
from .http import HTTPStatusError
from .propfind import PropfindData, Response, prepare_propfind_request_data

if TYPE_CHECKING:
    from ._types import AuthTypes, HTTPResponse, URLTypes
    from .propfind import ResourceProps


class ClientError(Exception):
    """Custom exception thrown by the Client."""

    def __init__(self, msg: str, *args):
        """Instantiate exception with a msg."""
        self.msg: str = msg
        super().__init__(msg, *args)


class MoveError(ClientError):
    """Error returned during `move` operation."""

    ERROR_MESSAGE = {
        403: "the source and the destination could be same",
        404: "the resource could not be found",
        409: "there was conflict when trying to move the resource",
        412: "the destination URL already exists",
        423: "the source or the destination resource is locked",
        502: "the destination server may have refused to accept the resource",
    }

    def __init__(self, from_path: str, to_path: str, response: "HTTPResponse"):
        """Exception when moving file from one path to the other.

        Args:
            from_path: the source path trying to move the resource from
            to_path: the destination path to move the resource to
            response: response received during the failed move operation
        """
        self.status_code = status_code = response.status_code
        self.response = response
        self.from_path = from_path
        self.to_path = to_path

        hint = self.ERROR_MESSAGE.get(status_code, f"received {status_code}")
        super().__init__(f"failed to move {from_path} to {to_path} - {hint}")


class Client:
    """Provides higher level APIs for interacting with Webdav server."""

    def __init__(
        self,
        base_url: "URLTypes",
        auth: "AuthTypes" = None,
        http_client: "HTTPClient" = None,
    ) -> None:
        """Instantiate client for webdav.

        Args:
            base_url: base url of the Webdav server
            auth:  Auth for the webdav
            http_client: http client to use instead, useful in mocking
                (when extending, it is expected to have implemented additional
                verbs from webdav)
        """
        assert auth or http_client
        self.http = http_client or HTTPClient(auth=auth)
        self.base_url = URL(base_url)

    def _join(self, path: str) -> URL:
        """Join resource path with base url of the webdav server."""
        return URL(urljoin(str(self.base_url), path))

    def _get_props(
        self, path: str, data: Optional[str] = None
    ) -> "ResourceProps":
        """Returns properties of the specific resource by propfind request."""
        response = self.http.propfind(self._join(path), data=data)
        response.raise_for_status()
        prop_data = PropfindData(response)
        resp = prop_data.get_response_for_path(path)
        return resp.props

    def get_property(self, path: str, name: str, namespace: str = None) -> Any:
        """Returns appropriate property from the propfind response.

        Also supports getting named properties
        (for now restricted to a single string with the given namespace)
        """
        data = prepare_propfind_request_data(name, namespace)
        props = self._get_props(path, data=data)
        return getattr(props, name, "")

    def set_property(self):
        """Setting additional property to a resource."""

    def move(
        self, from_path: str, to_path: str, overwrite: bool = False
    ) -> None:
        """Move resource to a new destination (with or without overwriting)."""
        from_url = self._join(from_path)
        to_url = self._join(to_path)
        headers = {
            "Destination": str(to_url),
            "Overwrite": "T" if overwrite else "F",
        }

        http_resp = self.http.move(from_url, headers=headers)
        try:
            http_resp.raise_for_status()
        except HTTPStatusError as exc:
            raise MoveError(from_path, to_path, http_resp) from exc
        assert (
            http_resp.status_code != 207
        )  # how to handle Multistatus response?

    def copy(self, from_path: str, to_path: str, depth: int = 1) -> None:
        """Copy resource."""
        from_path = self._join(from_path)
        to_path = self._join(to_path)
        headers = {"Destination": str(to_path), "Depth": depth}
        self.http.copy(from_path, headers=headers)

    def mkdir(self, path: str) -> None:
        """Create a collection."""
        self.http.mkcol(self._join(path))

    def remove(self, path: str) -> None:
        """Remove a resource."""
        self.http.delete(self._join(path))

    def ls(  # pylint: disable=invalid-name
        self, path: str, detail: bool = True
    ) -> List[Union[str, Dict[str, Any]]]:
        """List items in a resource/collection.

        Args:
            path: Path to the resource
            detail: If detail=True, additional information is returned
                in a dictionary
        """
        headers = {"Depth": "1"}
        url = self.base_url.join(path)
        http_resp = self.http.propfind(url, headers=headers)
        http_resp.raise_for_status()
        data = PropfindData(http_resp)

        def prepare_result(response: Response) -> Union[str, Dict[str, Any]]:
            href = response.href
            if not detail:
                return href
            return {
                "name": href,
                "status": response.status,
                "size": response.props.content_length,
                "created": response.props.created,
                "modified": response.props.modified,
                "language": response.props.content_language,
                "content_type": response.props.content_type,
                "etag": response.props.etag,
                "type": "directory" if response.props.collection else "file",
            }

        responses = list(data.responses.values())
        if len(data.responses) > 1:
            responses = [
                resp
                for href, resp in data.responses.items()
                if url != self._join(href)
            ]

        return list(map(prepare_result, responses))
