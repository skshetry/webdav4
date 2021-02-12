"""Client for the webdav."""

import locale
import logging
import shutil
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
    Type,
    Union,
    cast,
)

from .callback import wrap_file_like
from .fs_utils import peek_filelike_length
from .http import Client as HTTPClient
from .http import HTTPStatusError
from .http import Method as HTTPMethod
from .multistatus import (
    MultiStatusError,
    Response,
    parse_multistatus_response,
    prepare_propfind_request_data,
)
from .stream import IterStream
from .urls import URL, join_url

if TYPE_CHECKING:
    from datetime import datetime
    from os import PathLike

    from .multistatus import DAVProperties, MultiStatusResponse
    from .types import AuthTypes, HeaderTypes, HTTPResponse, URLTypes


logger = logging.getLogger(__name__)


class ClientError(Exception):
    """Custom exception thrown by the Client."""

    def __init__(self, msg: str, *args: Any) -> None:
        """Instantiate exception with a msg."""
        self.msg: str = msg
        super().__init__(msg, *args)


class OperationError(ClientError):
    """Base Exception for Webdav operations/requests."""

    ERROR_MESSAGE: Dict[int, str] = {}


class RemoveError(OperationError):
    """Error returned during `delete` operation."""

    ERROR_MESSAGE = {
        404: "the resource could not be found",
        423: "the resource is locked",
    }

    def __init__(self, path: str, msg: str) -> None:
        """Exception when trying to delete a resource.

        Args:
            path: the path trying to create a collection
            msg: message to show

        Either status_code or default_msg should be passed.
        """
        self.path = path
        super().__init__(f"failed to remove {path} - {msg}")


class CopyError(OperationError):
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

    def __init__(self, from_path: str, to_path: str, msg: str) -> None:
        """Exception when trying to delete a resource.

        Args:
            from_path: the source path trying to move the resource from
            to_path: the destination path to move the resource to
            msg:  msg to show instead

        Either status_code or default_msg should be passed.
        """
        self.from_path = from_path
        self.to_path = to_path
        super().__init__(
            f"failed to {self.OPERATION} {from_path} to {to_path} - {msg}"
        )


class MoveError(CopyError):
    """Error returned during `move` operation."""

    OPERATION = "move"


class CreateCollectionError(OperationError):
    """Error returned during `mkcol` operation."""

    ERROR_MESSAGE = {
        403: "the server does not allow creation in the namespace"
        "or cannot accept members",
        405: "collection already exists",
        409: "parent of the collection does not exist",
        415: "the server does not support the request body type",
        507: "insufficient storage",
    }

    def __init__(self, path: str, msg: str) -> None:
        """Exception when creating a collection.

        Args:
            path: the path trying to create a collection
            msg: message to show
        """
        self.path = path
        super().__init__(f"failed to create collection {path} - {msg}")


class ResourceAlreadyExists(ClientError):
    """Error returned if the resource already exists."""


class HTTPError(ClientError):
    """Custom Exception for our HTTPStatusError."""

    def __init__(self, response: "HTTPResponse") -> None:
        """Instantiate exception with the failed response."""
        self.response = response
        self.status_code = response.status_code
        self.request = response.request

        super().__init__(
            f"received {self.status_code} ({self.response.reason_phrase})"
        )


class ResourceNotFound(ClientError):
    """Error when the resource does not exist on the server."""

    def __init__(self, path: str) -> None:
        """Instantiate exception with path that does not exist."""
        self.path = path
        super().__init__(
            f"The resource {path} could not be found in the server"
        )


class InsufficientStorage(ClientError):
    """Error when the resource does not exist on the server."""

    def __init__(self, path: str) -> None:
        """Instantiate exception with the path for which the request failed."""
        self.path = path
        super().__init__("Insufficient Storage on the server.")


@contextmanager
def error_handler(exc_cls: Type[OperationError], *args: Any) -> Iterator[None]:
    """Handle error message properly for exceptions.

    Provides hints for common status codes, and handles multistatus error
    messages as well.
    """
    try:
        yield
    except (HTTPError, MultiStatusError) as exc:
        msg = None
        if isinstance(exc, HTTPError):
            code = exc.response.status_code
            if issubclass(exc_cls, OperationError):
                msg = exc_cls.ERROR_MESSAGE.get(code)

        raise exc_cls(*args, msg or exc.msg) from exc


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
        self.http = http_client or HTTPClient(base_url=base_url, auth=auth)
        self.base_url = URL(base_url)

    def join_url(self, path: str) -> URL:
        """Join resource path with base url of the webdav server."""
        return join_url(self.base_url, path)

    def propfind(
        self, path: str, data: str = None, headers: "HeaderTypes" = None
    ) -> "MultiStatusResponse":
        """Returns properties of the specific resource by propfind request."""
        http_resp = self._request(
            HTTPMethod.PROPFIND, path, data=data, headers=headers
        )
        return parse_multistatus_response(http_resp)

    def get_props(
        self,
        path: str,
        name: str = None,
        namespace: str = None,
        data: str = None,
    ) -> "DAVProperties":
        """Returns properties of a resource by doing a propfind request.

        Can also selectively request the properties by passing name or data.
        """
        data = data or prepare_propfind_request_data(name, namespace)
        headers = {"Content-Type": "application/xml"} if data else {}
        msr = self.propfind(path, headers=headers, data=data)
        response = msr.get_response_for_path(self.base_url.path, path)
        return response.properties

    def get_property(self, path: str, name: str, namespace: str = None) -> Any:
        """Returns appropriate property from the propfind response.

        Also supports getting named properties
        (for now restricted to a single string with the given namespace)
        """
        props = self.get_props(path, name=name, namespace=namespace)
        return getattr(props, name, "")

    def set_property(self) -> None:
        """Setting additional property to a resource."""

    def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> "HTTPResponse":
        """Internal method for sending request to the server.

        It handles joining path correctly and checks for common http errors.
        """
        url = self.join_url(path)
        http_resp = self.http.request(method, url, **kwargs)

        if http_resp.status_code == 404:
            raise ResourceNotFound(path)
        if http_resp.status_code == 507:
            raise InsufficientStorage(path)

        try:
            http_resp.raise_for_status()
        except HTTPStatusError as exc:
            raise HTTPError(http_resp) from exc

        return http_resp

    def request(self, method: str, path: str, **kwargs: Any) -> "HTTPResponse":
        """Sends request to a server with given method and path.

        Also checks for Multistatus response and other http errors.
        """
        http_resp = self._request(method, path, **kwargs)
        if http_resp.status_code == 207:
            # if it's 207, it's most likely an error
            # or, a partial success (however you see it).
            # except for the propfind, for which we use `_request` directly)
            # in the above `propfind` function.
            msr = parse_multistatus_response(http_resp)
            msr.raise_for_status()
        return http_resp

    def move(
        self, from_path: str, to_path: str, overwrite: bool = False
    ) -> None:
        """Move resource to a new destination (with or without overwriting)."""
        to_url = self.join_url(to_path)
        headers = {
            "Destination": str(to_url),
            "Overwrite": "T" if overwrite else "F",
        }

        with error_handler(MoveError, from_path, to_path):
            http_resp = self.request(
                HTTPMethod.MOVE, from_path, headers=headers
            )

        status_code = http_resp.status_code
        log_msg = ("move %s -> %s (overwrite: %s) - received %s",)
        logger.debug(log_msg, from_path, to_path, overwrite, status_code)

    def copy(
        self,
        from_path: str,
        to_path: str,
        shallow: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Copy resource."""
        to_url = self.join_url(to_path)
        headers = {
            "Destination": str(to_url),
            "Depth": "0" if shallow else "infinity",
            "Overwrite": "T" if overwrite else "F",
        }

        with error_handler(CopyError, from_path, to_path):
            http_resp = self.request(
                HTTPMethod.COPY, from_path, headers=headers
            )

        logger.debug(
            "move %s->%s (depth: %s, overwrite: %s) - received %s",
            from_path,
            to_path,
            headers["Depth"],
            overwrite,
            http_resp.status_code,
        )

    def mkdir(self, path: str, exist_ok: bool = False) -> None:
        """Create a collection."""
        with error_handler(CreateCollectionError, path):
            try:
                http_resp = self.request(HTTPMethod.MKCOL, path)
            except HTTPError as exc:
                if exist_ok and exc.status_code == 405:
                    return
                raise

        assert http_resp.status_code in (200, 201)
        logger.debug("mkcol %s - received %s", path, http_resp.status_code)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Creates a directory and its intermediate paths.

        Args:
            path: path till the directory to create
            exist_ok: if it exists already, it will be silently ignored
        """
        parts = list(filter(bool, path.split("/")))
        paths = ["/".join(parts[: n + 1]) for n in range(len(parts))]

        for parent in paths:
            self.mkdir(parent, exist_ok=exist_ok)

    def remove(self, path: str) -> None:
        """Remove a resource."""
        with error_handler(RemoveError, path):
            http_resp = self.request(HTTPMethod.DELETE, path)

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

        def prepare_result(response: Response) -> Union[str, Dict[str, Any]]:
            rel = response.path_relative_to(self.base_url)
            if not detail:
                return rel
            return {
                "name": rel,
                "href": response.href,
                **response.properties.as_dict(),
            }

        msr = self.propfind(path, headers={"Depth": "1"})
        responses = msr.responses

        if len(responses) > 1:
            url = self.join_url(path)
            responses.pop(url.path, None)

        return list(map(prepare_result, responses.values()))

    def exists(self, path: str) -> bool:
        """Checks whether the resource with the given path exists or not."""
        try:
            self.propfind(path)
        except ResourceNotFound:
            return False

        return True

    def isdir(self, path: str) -> Optional[bool]:
        """Checks whether the resource with the given path is a directory."""
        return self.get_props(path).collection

    def isfile(self, path: str) -> bool:
        """Checks whether the resource with the given path is a file."""
        return not self.isdir(path)

    def content_length(self, path: str) -> Optional[int]:
        """Returns content-length of the resource with the given path."""
        return self.get_props(path, "content_length").content_length

    def created(self, path: str) -> Optional["datetime"]:
        """Returns creationdate of the resource with the given path."""
        return self.get_props(path, "created").created

    def modified(self, path: str) -> Optional["datetime"]:
        """Returns getlastmodified of the resource with the given path."""
        return self.get_props(path, "modified").modified

    def etag(self, path: str) -> Optional[str]:
        """Returns etag of the resource with the given path."""
        return self.get_props(path, "etag").etag

    def content_type(self, path: str) -> Optional[str]:
        """Returns content type of the resource with the given path."""
        return self.get_props(path, "content_type").content_type

    def content_language(self, path: str) -> Optional[str]:
        """Returns content language of the resource with the given path."""
        return self.get_props(path, "content_language").content_language

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

        with IterStream(
            self.http,
            self.join_url(path),
            chunk_size=block_size,
            callback=callback,
        ) as buffer:
            buff = cast(BinaryIO, buffer)

            if mode == "rb":
                yield buff
            else:
                encoding = (
                    encoding
                    or buffer.encoding
                    or locale.getpreferredencoding(False)
                )
                yield TextIOWrapper(buff, encoding=encoding)

    def download_fileobj(
        self,
        from_path: str,
        file_obj: BinaryIO,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Write stream from path to given file object."""
        with self.open(from_path, mode="rb") as remote_obj:
            # TODO: fix typings for open to always return BinaryIO on mode=rb
            remote_obj = cast(BinaryIO, remote_obj)
            wrapped = wrap_file_like(file_obj, callback, method="write")
            shutil.copyfileobj(remote_obj, wrapped)

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
            length = peek_filelike_length(file_obj)
        except (TypeError, AttributeError, UnsupportedOperation):
            length = 0

        headers = {"Content-Length": str(length)} if length else None

        if not overwrite and self.exists(to_path):
            raise ResourceAlreadyExists(f"{to_path} already exists.")

        wrapped = wrap_file_like(file_obj, callback)
        http_resp = self.request(
            HTTPMethod.PUT, to_path, content=wrapped, headers=headers
        )
        http_resp.raise_for_status()
