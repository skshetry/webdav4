"""Client for the webdav."""

import locale
import shutil
from contextlib import contextmanager, suppress
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
    Set,
    TextIO,
    Union,
    cast,
)

from .callback import wrap_file_like
from .fs_utils import peek_filelike_length
from .http import Client as HTTPClient
from .http import HTTPStatusError
from .http import Method as HTTPMethod
from .multistatus import (
    MultiStatusResponseError,
    Response,
    parse_multistatus_response,
    prepare_propfind_request_data,
)
from .stream import IterStream
from .urls import URL, join_url

if TYPE_CHECKING:
    from datetime import datetime
    from os import PathLike
    from typing import AnyStr

    from .multistatus import DAVProperties, MultiStatusResponse
    from .types import AuthTypes, HeaderTypes, HTTPResponse, URLTypes


class ClientError(Exception):
    """Custom exception thrown by the Client."""

    def __init__(self, msg: str) -> None:
        """Instantiate exception with a msg."""
        self.msg: str = msg
        super().__init__(msg)

    def __str__(self) -> str:
        """Provide str repr of the msg."""
        return self.msg


class ResourceConflict(ClientError):
    """Raised when there was conflict during the operation (got 409)."""


class ForbiddenOperation(ClientError):
    """Raised when the operation was forbidden (got 403)."""


class ResourceAlreadyExists(ClientError):
    """Error returned if the resource already exists."""

    def __init__(self, path: str) -> None:
        """Instantiate exception with the path that already exists."""
        self.path = path
        super().__init__(f"The resource {path} already exists")


class InsufficientStorage(ClientError):
    """Error when the resource does not exist on the server."""

    def __init__(self, path: str) -> None:
        """Instantiate exception with the path for which the request failed."""
        self.path = path
        super().__init__("Insufficient Storage on the server")


class BadGatewayError(ClientError):
    """Error when bad gateway error is thrown."""

    def __init__(self) -> None:
        """Raised when 502 status code is raised by the server."""
        msg = "The destination server may have refused to accept the resource"
        super().__init__(msg)


class ResourceLocked(ClientError):
    """Error raised when the resource is locked."""


class ResourceNotFound(ClientError):
    """Error when the resource does not exist on the server."""

    def __init__(self, path: str) -> None:
        """Instantiate exception with path that does not exist."""
        self.path = path
        super().__init__(
            f"The resource {path} could not be found in the server"
        )


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


class MultiStatusError(ClientError):
    """Wrapping MultiStatusResponseError with ClientError."""


class Client:
    """Provides higher level APIs for interacting with Webdav server."""

    def __init__(
        self,
        base_url: "URLTypes",
        auth: "AuthTypes" = None,
        http_client: "HTTPClient" = None,
        **client_opts: Any,
    ) -> None:
        """Instantiate client for webdav.

        Examples:
            >>> client = Client("https://webdav.example.org")
            >>> client.ls("/")

        Args:
            base_url: base url of the Webdav server
            auth: Auth for the webdav.
                Auth can be any of the following:

                - a tuple of (user, password)
                - None if no auth is required.

                Refer to `Customizing Authentication \
                <https://www.python-httpx.org/advanced/ \
                #customizing-authentication>`_
                for more options.

            http_client: http client to use instead, useful in mocking
                (when extending, it is expected to have implemented additional
                verbs from webdav)

        All of the following keyword arguments are passed along to the
        `httpx <https://www.python-httpx.org/api/#client>`_, the http library
        this client is built on.

        Keyword Args:
            headers: Dict. of HTTP headers to include when sending requests
            cookies: Dict. of Cookie items to include when sending requests
            verify: SSL certificates used to verify the identity of requested
                hosts. Can be any of:

                - True (uses default CA bundle),
                - a path to an SSL certificate file,
                - False (disable verfication), or
                - a :py:class:`ssl.SSLContext`

            cert: An SSL certificate used by the requested host to
                authenticate the client.
                Either a path to an SSL certificate file,
                or two-tuple of (certificate file, key file),
                or a three-tuple of (certificate file, key file, password).
            proxies: A dictionary mapping proxy keys to proxy URLs
            timeout: The timeout configuration to use when sending requests
            limits: The limits configuration to use
            max_redirects: The maximum number of redirect responses that
                should be followed
            trust_env: Enables or disables usage of environment variables
                for configuration
        """
        client_opts.update({"base_url": base_url, "auth": auth})
        self.http: HTTPClient = http_client or HTTPClient(**client_opts)
        self.base_url = URL(base_url)

    def options(self, path: str = "") -> Set[str]:
        """Returns features detected in the webdav server."""
        resp = self.http.options(path)
        dav_header = resp.headers.get("dav", "")
        return {f.strip() for f in dav_header.split(",")}

    def join_url(self, path: str) -> URL:
        """Join resource path with base url of the webdav server."""
        return join_url(self.base_url, path)

    def propfind(
        self, path: str, data: str = None, headers: "HeaderTypes" = None
    ) -> "MultiStatusResponse":
        """Returns properties of the specific resource by propfind request."""
        http_resp = self._request(
            HTTPMethod.PROPFIND, path, content=data, headers=headers
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
        result = self.propfind(path, headers=headers, data=data)
        response = result.get_response_for_path(self.base_url.path, path)
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
        if http_resp.status_code == 502:
            raise BadGatewayError

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
            result = parse_multistatus_response(http_resp)
            try:
                result.raise_for_status()
            except MultiStatusResponseError as exc:
                raise MultiStatusError(exc.msg) from exc

        return http_resp

    def move(
        self, from_path: str, to_path: str, overwrite: bool = False
    ) -> None:
        """Move resource to a new destination (with or without overwriting)."""
        return self._transfer(
            HTTPMethod.MOVE, from_path, to_path, overwrite=overwrite
        )

    def _transfer(
        self,
        operation: str,
        from_path: str,
        to_path: str,
        overwrite: bool,
        depth: Union[int, str] = "infinity",
    ) -> None:
        """Transfer a resource by copying/moving from a path to the other."""
        assert operation in {HTTPMethod.MOVE, HTTPMethod.COPY}

        to_url = self.join_url(to_path)
        headers = {
            "Destination": str(to_url),
            "Overwrite": "T" if overwrite else "F",
            "Depth": str(depth),
        }

        try:
            self.request(operation, from_path, headers=headers)
        except HTTPError as exc:
            if exc.status_code == 403:
                msg = "the source and the destination could be the same"
                raise ForbiddenOperation(msg) from exc
            if exc.status_code == 409:
                msg = (
                    "there was a conflict when trying to "
                    f"{operation.lower()} the resource"
                )
                raise ResourceConflict(msg) from exc
            if exc.status_code == 412:
                raise ResourceAlreadyExists(to_path) from exc
            if exc.status_code == 423:
                msg = "the source or the destination resource is locked"
                raise ResourceLocked(msg) from exc

            raise

    def copy(
        self,
        from_path: str,
        to_path: str,
        depth: Union[int, str] = "infinity",
        overwrite: bool = False,
    ) -> None:
        """Copy resource."""
        return self._transfer(
            HTTPMethod.COPY,
            from_path,
            to_path,
            depth=depth,
            overwrite=overwrite,
        )

    def mkdir(self, path: str, exist_ok: bool = False) -> None:
        """Create a collection."""
        try:
            http_resp = self.request(HTTPMethod.MKCOL, path)
        except HTTPError as exc:
            if exc.status_code == 405:
                if exist_ok:
                    return
                raise ResourceAlreadyExists(path) from exc
            if exc.status_code == 403:
                msg = (
                    "the server does not allow creation in the namespace"
                    "or cannot accept members"
                )
                raise ForbiddenOperation(msg) from exc
            if exc.status_code == 409:
                msg = "parent of the collection does not exist"
                raise ResourceConflict(msg) from exc

            raise

        assert http_resp.status_code in (200, 201)

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
        try:
            self.request(HTTPMethod.DELETE, path)
        except HTTPError as exc:
            if exc.status_code == 423:
                raise ResourceLocked("the resource is locked") from exc
            raise

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

        result = self.propfind(path, headers={"Depth": "1"})
        responses = result.responses

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
            raise ValueError("Cannot open a collection")

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
        to_path: "PathLike[AnyStr]",
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Download file from remote path to local path."""
        with open(to_path, mode="wb") as fobj:
            self.download_fileobj(from_path, fobj, callback=callback)

    def upload_file(
        self,
        from_path: "PathLike[AnyStr]",
        to_path: str,
        overwrite: bool = False,
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
        overwrite: bool = False,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Upload file from file object to given path."""
        length = -1
        with suppress(TypeError, AttributeError, UnsupportedOperation):
            length = peek_filelike_length(file_obj)

        headers = {"Content-Length": str(length)} if length >= 0 else None

        if not overwrite and self.exists(to_path):
            raise ResourceAlreadyExists(to_path)

        wrapped = wrap_file_like(file_obj, callback)
        http_resp = self.request(
            HTTPMethod.PUT, to_path, content=wrapped, headers=headers
        )
        http_resp.raise_for_status()
