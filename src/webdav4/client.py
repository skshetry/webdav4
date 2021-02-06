"""Client for the webdav."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from urllib.parse import urljoin

from .http import URL
from .http import Client as HTTPClient
from .propfind import PropfindData, Response, prepare_propfind_request_data

if TYPE_CHECKING:
    from ._types import AuthTypes, URLTypes
    from .propfind import ResourceProps


class Client:
    """Provides higher level APIs for interacting with Webdav server."""

    def __init__(self, base_url: "URLTypes", auth: "AuthTypes") -> None:
        """Instantiate client for webdav.

        Args:
            base_url: base url of the Webdav server
            auth:  Auth for the webdav
        """
        self.http = HTTPClient(auth=auth)
        self.base_url = URL(base_url)

    def _join(self, path: str) -> URL:
        """Join resource path with base url of the webdav server."""
        return URL(urljoin(str(self.base_url), path))

    def _get_props(
        self, path: str, data: Optional[str] = None
    ) -> "ResourceProps":
        """Returns properties of the specific resource by propfind request."""
        response = self.http.propfind(self._join(path), data=data)
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
        from_path = self._join(from_path)
        to_path = self._join(to_path)
        headers = {
            "Destination": to_path,
            "Overwrite": "T" if overwrite else "F",
        }
        self.http.move(from_path, headers=headers)

    def copy(self, from_path: str, to_path: str, depth: int = 1) -> None:
        """Copy resource."""
        from_path = self._join(from_path)
        to_path = self._join(to_path)
        headers = {"Destination": to_path, "Depth": depth}
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
        data = PropfindData(http_resp)

        def prepare_result(response: Response) -> Union[str, Dict[str, Any]]:
            href = response.href
            if not detail:
                return href
            return {
                "name": href,
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
