"""Client for the webdav."""
import locale
import shutil
import threading
from contextlib import contextmanager, suppress
from http import HTTPStatus
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
    TypeVar,
    Union,
    cast,
)

from .callback import wrap_file_like
from .fs_utils import peek_filelike_length
from .func_utils import wrap_fn
from .http import Client as HTTPClient
from .http import HTTPStatusError
from .http import Method as HTTPMethod
from .multistatus import (
    MultiStatusResponseError,
    Response,
    parse_multistatus_response,
    prepare_propfind_request_data,
)
from .retry import retry as _retry
from .stream import IterStream, read_chunks
from .urls import URL, join_url

if TYPE_CHECKING:
    from datetime import datetime
    from os import PathLike
    from typing import AnyStr

    from .multistatus import DAVProperties, MultiStatusResponse
    from .types import AuthTypes, HeaderTypes, HTTPResponse, URLTypes

_T = TypeVar("_T")


DEFAULT_CHUNK_SIZE = 2 ** 22


def _prepare_result_info(
    response: Response, base_url: URL, detail: bool = True
) -> Union[str, Dict[str, Any]]:
    """Transform response to a dictionary/str for info/ls."""
    rel = response.path_relative_to(base_url)
    if not detail:
        return rel
    return {
        "name": rel,
        "href": response.href,
        **response.properties.as_dict(),
    }


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


class IsAResourceError(ClientError):
    """Exception thrown when the path is a resource.

    Could be thrown when the collection is expected.
    """

    def __init__(self, path: str, msg: str = "") -> None:
        """Initialize with the path and the appropriate message."""
        self.path: str = path
        super().__init__(f"{path} is a collection. {msg}")


class IsACollectionError(ClientError):
    """Exception thrown when the path is a collection.

    Could be thrown when the resource/non-collection is expected.
    """

    def __init__(self, path: str, msg: str = "") -> None:
        """Initialize with the path and the appropriate message."""
        self.path: str = path
        super().__init__(f"{path} is a collection. {msg}")


class MultiStatusError(ClientError):
    """Wrapping MultiStatusResponseError with ClientError."""


class FeatureDetection:
    """Detect features in the webdav resources.

    Mostly used for detecting support for Accept-Ranges as ownCloud/NextCloud
    don't advertise support for it in GET requests.
    """

    supports_ranges: bool
    dav_compliances: Set[str]

    def __init__(self, options_response: "HTTPResponse" = None) -> None:
        """Initialize with the given response."""
        dav_compliances = set()
        supports_ranges = False
        if options_response:
            dav_header = options_response.headers.get("dav", "")
            dav_compliances = {f.strip() for f in dav_header.split(",")}
            supports_ranges = (
                options_response.headers.get("accept-ranges") == "bytes"
            )

        self.dav_compliances = dav_compliances
        self.supports_ranges = supports_ranges


class Client:
    """Provides higher level APIs for interacting with Webdav server."""

    def __init__(
        self,
        base_url: "URLTypes",
        auth: "AuthTypes" = None,
        http_client: "HTTPClient" = None,
        retry: Union[Callable[[Callable[[], _T]], _T], bool] = True,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
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
            retry: disable or enable retry on client. Can also pass a callable
                to handle it there. Some well-known errors are handled and
                retried a few times with the backoff.

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
        self.with_retry = retry if callable(retry) else _retry(retry)
        self._detected_features: Optional[FeatureDetection] = None
        self._detect_feature_lock = threading.RLock()
        self.chunk_size = chunk_size

    @property
    def detected_features(self) -> FeatureDetection:
        """Feature detection for the server."""
        if not self._detected_features:
            with self._detect_feature_lock:
                # a lot of threads might be stuck on thread lock
                # and if one is done with it, it means we already
                # have it set, so we should look for it rather than
                # sending out a request.
                if self._detected_features:  # pragma: no cover
                    return self._detected_features

                resp = None
                with suppress(Exception):
                    resp = self.http.options(self.base_url)
                self._detected_features = FeatureDetection(resp)
        return self._detected_features

    def options(self, path: str = "") -> Set[str]:
        """Returns features detected in the webdav server."""
        resp = self.http.options(path)
        detected_features = FeatureDetection(resp)
        return detected_features.dav_compliances

    def join_url(self, path: str) -> URL:
        """Join resource path with base url of the webdav server."""
        return join_url(self.base_url, path)

    def propfind(
        self, path: str, data: str = None, headers: "HeaderTypes" = None
    ) -> "MultiStatusResponse":
        """Returns properties of the specific resource by propfind request."""
        call = wrap_fn(
            self._request,
            HTTPMethod.PROPFIND,
            path,
            content=data,
            headers=headers,
        )
        http_resp = self.with_retry(call)
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

        if http_resp.status_code == HTTPStatus.NOT_FOUND:
            raise ResourceNotFound(path)
        if http_resp.status_code == HTTPStatus.INSUFFICIENT_STORAGE:
            raise InsufficientStorage(path)
        if http_resp.status_code == HTTPStatus.BAD_GATEWAY:
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
        if http_resp.status_code == HTTPStatus.MULTI_STATUS:
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

        call = wrap_fn(self.request, operation, from_path, headers=headers)
        try:
            self.with_retry(call)
        except HTTPError as exc:
            if exc.status_code == HTTPStatus.FORBIDDEN:
                msg = "the source and the destination could be the same"
                raise ForbiddenOperation(msg) from exc
            if exc.status_code == HTTPStatus.CONFLICT:
                msg = (
                    "there was a conflict when trying to "
                    f"{operation.lower()} the resource"
                )
                raise ResourceConflict(msg) from exc
            if exc.status_code == HTTPStatus.PRECONDITION_FAILED:
                raise ResourceAlreadyExists(to_path) from exc
            if exc.status_code == HTTPStatus.LOCKED:
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

    def mkdir(self, path: str) -> None:
        """Create a collection."""
        call = wrap_fn(self.request, HTTPMethod.MKCOL, path)
        try:
            http_resp = self.with_retry(call)
        except HTTPError as exc:
            if exc.status_code == HTTPStatus.METHOD_NOT_ALLOWED:
                raise ResourceAlreadyExists(path) from exc
            if exc.status_code == HTTPStatus.FORBIDDEN:
                msg = (
                    "the server does not allow creation in the namespace"
                    "or cannot accept members"
                )
                raise ForbiddenOperation(msg) from exc
            if exc.status_code == HTTPStatus.CONFLICT:
                msg = "parent of the collection does not exist"
                raise ResourceConflict(msg) from exc

            raise

        assert http_resp.status_code in (HTTPStatus.OK, HTTPStatus.CREATED)

    def remove(self, path: str) -> None:
        """Remove a resource."""
        call = wrap_fn(self.request, HTTPMethod.DELETE, path)
        try:
            self.with_retry(call)
        except HTTPError as exc:
            if exc.status_code == HTTPStatus.LOCKED:
                raise ResourceLocked("the resource is locked") from exc
            raise

    def ls(  # pylint: disable=invalid-name
        self,
        path: str,
        detail: bool = True,
        allow_listing_resource: bool = True,
    ) -> List[Union[str, Dict[str, Any]]]:
        """List items in a resource/collection.

        Args:
            path: Path to the resource
            detail: If detail=True, additional information is returned
                in a dictionary
            allow_listing_resource: If True and path is a resource
                (non-collection), ls will return the file entry/details.
                Otherwise, it will raise an error.
        """
        result = self.propfind(path, headers={"Depth": "1"})
        responses = result.responses

        url = self.join_url(path)
        response = responses.get(url.path)

        if response:
            typ = response.properties.resource_type
            if typ == "file" and not allow_listing_resource:
                raise IsAResourceError(
                    path, "cannot list from a resource itself"
                )
            if typ == "directory":
                responses.pop(url.path)
        else:  # pragma: no cover
            assert not response  # response should always be there

        return [
            _prepare_result_info(resp, self.base_url, detail)
            for resp in responses.values()
        ]

    def info(self, path: str) -> Dict[str, Any]:
        """Returns information about the path itself."""
        result = self.propfind(path, headers={"Depth": "1"})
        responses = result.responses

        url = self.join_url(path)
        details = _prepare_result_info(
            responses[url.path], self.base_url, detail=True
        )
        assert not isinstance(details, str)
        return details

    def exists(self, path: str) -> bool:
        """Checks whether the resource with the given path exists or not."""
        try:
            self.propfind(path)
        except ResourceNotFound:
            return False

        return True

    def isdir(self, path: str) -> bool:
        """Checks whether the resource with the given path is a directory."""
        return bool(self.get_props(path).collection)

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
        chunk_size: int = None,
    ) -> Iterator[Union[TextIO, BinaryIO]]:
        """Returns file-like object to a resource."""
        if self.isdir(path):
            raise IsACollectionError(path, "Cannot open a collection")
        assert mode in {"r", "rt", "rb"}

        with IterStream(
            self,
            self.join_url(path),
            chunk_size=chunk_size or self.chunk_size,
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
        chunk_size: int = None,
    ) -> None:
        """Write stream from path to given file object."""
        with self.open(
            from_path, mode="rb", chunk_size=chunk_size
        ) as remote_obj:
            # TODO: fix typings for open to always return BinaryIO on mode=rb
            remote_obj = cast(BinaryIO, remote_obj)
            wrapped = wrap_file_like(file_obj, callback, method="write")
            shutil.copyfileobj(remote_obj, wrapped)

    def download_file(
        self,
        from_path: str,
        to_path: "PathLike[AnyStr]",
        chunk_size: int = None,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Download file from remote path to local path."""
        with open(to_path, mode="wb") as fobj:
            self.download_fileobj(
                from_path, fobj, callback=callback, chunk_size=chunk_size
            )

    def upload_file(
        self,
        from_path: "PathLike[AnyStr]",
        to_path: str,
        overwrite: bool = False,
        chunk_size: int = None,
        callback: Callable[[int], Any] = None,
    ) -> None:
        """Upload file from local path to a given remote path."""
        with open(from_path, mode="rb") as fobj:
            self.upload_fileobj(
                fobj,
                to_path,
                overwrite=overwrite,
                chunk_size=chunk_size,
                callback=callback,
            )

    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        to_path: str,
        overwrite: bool = False,
        callback: Callable[[int], Any] = None,
        chunk_size: int = None,
        size: int = None,
    ) -> None:
        """Upload file from file object to given path."""
        # we try to avoid chunked transfer as much as possible
        # so we try to use size as a hint if provided.
        # else, we will try to find that out from the file object
        # if we are not successfull in that, we gracefully fallback
        # to the chunked encoding.
        if size is None:
            with suppress(TypeError, AttributeError, UnsupportedOperation):
                size = peek_filelike_length(file_obj)

        headers = {"Content-Length": str(size)} if size is not None else None
        if not overwrite and self.exists(to_path):
            raise ResourceAlreadyExists(to_path)

        wrapped = wrap_file_like(file_obj, callback)
        content = read_chunks(
            wrapped, chunk_size=chunk_size or self.chunk_size
        )

        http_resp = self.request(
            HTTPMethod.PUT, to_path, content=content, headers=headers
        )
        http_resp.raise_for_status()
