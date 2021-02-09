"""Client for the webdav."""

import locale
import logging
from contextlib import contextmanager
from io import TextIOWrapper, UnsupportedOperation
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TextIO,
    Union,
    cast,
)
from urllib.parse import urljoin

from .http import URL
from .http import Client as HTTPClient
from .http import HTTPStatusError
from .propfind import PropfindData, Response, prepare_propfind_request_data
from .stream import IterStream
from .utils import patch_file_like_read, try_to_guess_filelength

if TYPE_CHECKING:
    from os import PathLike

    from ._types import AuthTypes, HTTPResponse, URLTypes
    from .propfind import ResourceProps


logger = logging.getLogger(__name__)


class ClientError(Exception):
    """Custom exception thrown by the Client."""

    def __init__(self, msg: str, *args: Any) -> None:
        """Instantiate exception with a msg."""
        self.msg: str = msg
        super().__init__(msg, *args)


class MultiStatusError(ClientError):
    """Raised when multistatus response has failures in it."""

    def __init__(self, statuses: Dict[str, str]) -> None:
        """Pass multiple statuses, which is displayed when error is raised."""
        self.statuses = statuses

        msg = str(self.statuses)
        if len(self.statuses) > 1:
            msg = "multiple errors received: " + msg

        super().__init__(msg)


def check_error_in_multistatus(response: "HTTPResponse") -> None:
    """Check if there are errors received in multistatus response."""
    if response.status_code != 207:
        return None

    data = PropfindData(response)
    raise MultiStatusError(
        {
            href: resp.reason_phrase
            for href, resp in data.responses.items()
            if resp.reason_phrase
            and resp.status_code
            and 400 <= resp.status_code <= 599
        }
    )


class RemoveError(ClientError):
    """Error returned during `delete` operation."""

    ERROR_MESSAGE = {
        404: "the resource could not be found",
        423: "the resource is locked",
    }

    def __init__(
        self, path: str, status_code: int = None, default_msg: str = None
    ) -> None:
        """Exception when trying to delete a resource.

        Args:
            path: the path trying to create a collection
            status_code: status code from the response.
            default_msg: message to show instead, useful in multistatus calls

        Either status_code or default_msg should be passed.
        """
        self.path = path
        self.status_code = status_code

        assert status_code or default_msg
        hint = (
            self.ERROR_MESSAGE.get(status_code, f"received {status_code}")
            if status_code
            else default_msg
        )
        super().__init__(f"failed to remove {path} - {hint}")


class CopyError(ClientError):
    """Error returned during `copy` operation."""

    ERROR_MESSAGE = {
        403: "the source and the destination could be same",
        404: "the resource could not be found",
        409: "there was conflict when trying to move the resource",
        412: "the destination URL already exists",
        423: "the source or the destination resource is locked",
        502: "the destination server may have refused to accept the resource",
        507: "insufficient storage to execute the operation",
    }
    OPERATION = "copy"

    def __init__(
        self,
        from_path: str,
        to_path: str,
        status_code: int = None,
        default_msg: str = None,
    ) -> None:
        """Exception when trying to delete a resource.

        Args:
            from_path: the source path trying to move the resource from
            to_path: the destination path to move the resource to
            status_code: optional, status code of the response received
            default_msg: optional, msg to show instead, useful in multistatus
                responses

        Either status_code or default_msg should be passed.
        """
        self.from_path = from_path
        self.to_path = to_path
        self.status_code = status_code

        assert status_code or default_msg
        hint = (
            self.ERROR_MESSAGE.get(status_code, f"received {status_code}")
            if status_code
            else default_msg
        )
        super().__init__(
            f"failed to {self.OPERATION} {from_path} to {to_path} - {hint}"
        )


class CreateCollectionError(ClientError):
    """Error returned during `mkcol` operation."""

    ERROR_MESSAGE = {
        403: "the server does not allow creation in the namespace"
        "or cannot accept members",
        405: "collection already exists",
        409: "parent of the collection does not exist",
        415: "the server does not support the request body type",
        507: "insufficient storage",
    }

    def __init__(self, path: str, response: "HTTPResponse") -> None:
        """Exception when creating a collection.

        Args:
            path: the path trying to create a collection
            response: response from failed mkcol
        """
        self.path = path
        self.response = response
        self.status_code = status_code = response.status_code

        hint = self.ERROR_MESSAGE.get(status_code, f"received {status_code}")
        super().__init__(f"failed to create collection {path} - {hint}")


class MoveError(CopyError):
    """Error returned during `move` operation."""

    OPERATION = "move"


class ResourceAlreadyExists(ClientError):
    """Error returned if the resource already exists."""


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

    def join(self, path: str) -> URL:
        """Join resource path with base url of the webdav server."""
        return URL(urljoin(str(self.base_url), path))

    def _get_props(
        self,
        path: str,
        name: str = None,
        namespace: str = None,
        data: str = None,
    ) -> "ResourceProps":
        """Returns properties of the specific resource by propfind request."""
        if not data and name:
            data = prepare_propfind_request_data(name, namespace)

        response = self.http.propfind(self.join(path), data=data)
        response.raise_for_status()
        prop_data = PropfindData(response)
        resp = prop_data.get_response_for_path(path)
        return resp.props

    def get_property(self, path: str, name: str, namespace: str = None) -> Any:
        """Returns appropriate property from the propfind response.

        Also supports getting named properties
        (for now restricted to a single string with the given namespace)
        """
        props = self._get_props(path, name=name, namespace=namespace)
        return getattr(props, name, "")

    def set_property(self) -> None:
        """Setting additional property to a resource."""

    def move(
        self, from_path: str, to_path: str, overwrite: bool = False
    ) -> None:
        """Move resource to a new destination (with or without overwriting)."""
        from_url = self.join(from_path)
        to_url = self.join(to_path)
        headers = {
            "Destination": str(to_url),
            "Overwrite": "T" if overwrite else "F",
        }

        http_resp = self.http.move(from_url, headers=headers)
        try:
            http_resp.raise_for_status()
        except HTTPStatusError as exc:
            raise MoveError(from_path, to_path, http_resp.status_code) from exc

        try:
            check_error_in_multistatus(http_resp)
        except MultiStatusError as exc:
            raise MoveError(from_path, to_path, default_msg=exc.msg) from exc

        logger.debug(
            "move %s->%s - received %s, overwrite: %s",
            from_path,
            to_path,
            http_resp.status_code,
            overwrite,
        )

    def copy(
        self,
        from_path: str,
        to_path: str,
        shallow: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Copy resource."""
        from_url = self.join(from_path)
        to_url = self.join(to_path)
        headers = {
            "Destination": str(to_url),
            "Depth": "0" if shallow else "infinity",
            "Overwrite": "T" if overwrite else "F",
        }
        http_resp = self.http.copy(from_url, headers=headers)

        try:
            http_resp.raise_for_status()
        except HTTPStatusError as exc:
            raise CopyError(from_path, to_path, http_resp.status_code) from exc

        try:
            check_error_in_multistatus(http_resp)
        except MultiStatusError as exc:
            raise CopyError(from_path, to_path, default_msg=exc.msg) from exc

        logger.debug(
            "move %s->%s - received %s, depth: %s, overwrite: %s",
            from_path,
            to_path,
            http_resp.status_code,
            headers["Depth"],
            overwrite,
        )

    def mkdir(self, path: str) -> None:
        """Create a collection."""
        response = self.http.mkcol(self.join(path))
        try:
            response.raise_for_status()
        except HTTPStatusError as exc:
            raise CreateCollectionError(path, response) from exc

        assert response.status_code in (200, 201)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Creates a directory and its intermediate paths.

        Args:
            path: path till the directory to create
            exist_ok: if it exists already, it will be silently ignored
        """
        parts = list(filter(bool, path.split("/")))
        paths = ["/".join(parts[: n + 1]) for n in range(len(parts))]

        for parent in paths:
            try:
                self.mkdir(parent)
            except CreateCollectionError as exc:
                if exist_ok and exc.status_code == 405:
                    continue
                raise

    def remove(self, path: str) -> None:
        """Remove a resource."""
        http_resp = self.http.delete(self.join(path))
        try:
            http_resp.raise_for_status()
        except HTTPStatusError as exc:
            raise RemoveError(path, http_resp.status_code) from exc

        try:
            check_error_in_multistatus(http_resp)
        except MultiStatusError as exc:
            raise RemoveError(path, default_msg=exc.msg) from exc

        logger.debug("remove %s - received %s", path, http_resp.status_code)

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
                "href": href,
                "content_length": response.props.content_length,
                "created": response.props.created,
                "modified": response.props.modified,
                "content_language": response.props.content_language,
                "content_type": response.props.content_type,
                "etag": response.props.etag,
                "type": response.props.resource_type,
            }

        responses = list(data.responses.values())
        if len(data.responses) > 1:
            responses = [
                resp
                for href, resp in data.responses.items()
                if url != self.join(href)
            ]

        return list(map(prepare_result, responses))

    def exists(self, path: str) -> bool:
        """Checks whether the resource with the given path exists or not."""
        try:
            self._get_props(path)
            return True
        except HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return False
            raise

    def isdir(self, path: str) -> bool:
        """Checks whether the resource with the given path is a directory."""
        return self._get_props(path).collection

    def isfile(self, path: str) -> bool:
        """Checks whether the resource with the given path is a file."""
        return not self.isdir(path)

    def content_length(self, path: str) -> Optional[int]:
        """Returns content-length of the resource with the given path."""
        return not self._get_props(path, "content_length").content_length

    def created(self, path: str) -> Optional[str]:
        """Returns creationdate of the resource with the given path."""
        return self._get_props(path, "created").created

    def modified(self, path: str) -> Optional[str]:
        """Returns getlastmodified of the resource with the given path."""
        return self._get_props(path, "modified").modified

    def etag(self, path: str) -> Optional[str]:
        """Returns etag of the resource with the given path."""
        return self._get_props(path, "etag").etag

    def content_type(self, path: str) -> Optional[str]:
        """Returns content type of the resource with the given path."""
        return self._get_props(path, "content_type").content_type

    def content_language(self, path: str) -> Optional[str]:
        """Returns content language of the resource with the given path."""
        return self._get_props(path, "content_language").content_language

    @contextmanager
    def open(
        self,
        path: str,
        mode: str = "r",
        encoding: str = None,
        block_size: int = None,
        callback: Callable[[int], Any] = None,
    ) -> Iterator[Union[TextIO, BinaryIO]]:
        """Returns file-like object to a resource."""
        if self.isdir(path):
            raise ValueError("Cannot open a collection.")

        assert mode in {"r", "rt", "rb"}

        with IterStream(  # type: ignore[abstract]
            self.http,
            self.join(path),
            chunk_size=block_size,
            callback=callback,
        ) as buffer:
            if mode == "rb":
                yield buffer
            else:
                encoding = (
                    encoding
                    or buffer.encoding
                    or locale.getpreferredencoding(False)
                )
                yield TextIOWrapper(buffer, encoding=encoding)

    def download_fileobj(
        self,
        from_path: str,
        file_obj: BinaryIO,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Write stream from path to given file object."""
        with self.open(from_path, mode="rb", callback=callback) as remote_obj:
            # TODO: fix typings for open to always return BinaryIO on mode=rb
            remote_obj = cast(BinaryIO, remote_obj)
            file_obj.write(remote_obj.read())

    def download_file(
        self,
        from_path: str,
        to_path: "PathLike[str]",
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Download file from remote path to local path."""
        with open(to_path, mode="wb") as fobj:
            self.download_fileobj(from_path, fobj, callback=callback)

    def upload_file(
        self,
        from_path: "PathLike[str]",
        to_path: str,
        overwrite: bool = True,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Upload file from local path to a given remote path."""
        with open(from_path, mode="rb") as fobj:
            self.upload_fileobj(
                fobj, to_path, overwrite=overwrite, callback=callback
            )

    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        to_path: str,
        overwrite: bool = True,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Upload file from file object to given path."""
        try:
            length = try_to_guess_filelength(file_obj)
        except (TypeError, AttributeError, UnsupportedOperation):
            length = 0

        headers = {"Content-Length": str(length)} if length else None
        patch_file_like_read(file_obj, callback)

        if not overwrite and self.exists(to_path):
            raise ResourceAlreadyExists(f"{to_path} already exists.")

        url = self.join(to_path)
        http_resp = self.http.put(url, content=file_obj, headers=headers)
        http_resp.raise_for_status()
