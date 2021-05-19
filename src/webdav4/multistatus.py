"""Parsing propfind response."""

from http.client import responses
from typing import TYPE_CHECKING, Any, Dict, Optional, Union
from xml.etree.ElementTree import Element, ElementTree, SubElement
from xml.etree.ElementTree import fromstring as str2xml
from xml.etree.ElementTree import tostring as xml2string

from .date_utils import from_rfc1123, fromisoformat
from .urls import URL, join_url_path, relative_url_to, strip_leading_slash

if TYPE_CHECKING:
    from httpx import Response as HTTPResponse


# Map name used in library with actual prop name
MAPPING_PROPS: Dict[str, str] = {
    "content_length": "getcontentlength",
    "etag": "getetag",
    "created": "creationdate",
    "modified": "getlastmodified",
    "content_language": "getcontentlanguage",
    "content_type": "getcontenttype",
    "display_name": "displayname",
}


def prop(
    node: Union[Element, ElementTree], name: str, relative: bool = False
) -> Optional[str]:
    """Returns text of the property if it exists under DAV namespace."""
    namespace = "{DAV:}"
    selector = ".//" if relative else ""
    xpath = f"{selector}{namespace}{name}"
    return node.findtext(xpath)


class DAVProperties:
    """Parses <d:propstat> data into certain properties.

    Only supports a certain set of properties to extract. Others are ignored.
    """

    def __init__(self, response_xml: Element = None):
        """Parses props to certain attributes.

        Args:
             response_xml: <d:propstat> element
        """
        self.response_xml: Optional[Element] = response_xml
        self.raw: Dict[str, Any] = {}

        def extract_text(prop_name: str) -> Optional[str]:
            text = (
                prop(response_xml, MAPPING_PROPS[prop_name], relative=True)
                if response_xml
                else None
            )
            self.raw[prop_name] = text
            return text

        created = extract_text("created")
        self.created = fromisoformat(created) if created else None

        modified = extract_text("modified")
        self.modified = from_rfc1123(modified) if modified else None

        self.etag = extract_text("etag") or None
        self.content_type = extract_text("content_type")

        content_length = extract_text("content_length")
        self.content_length = int(content_length) if content_length else None
        self.content_language = extract_text("content_language")

        collection: Optional[bool] = None
        resource_type: Optional[str] = None
        resource_xml: Optional[Element] = None

        if response_xml is not None:
            resource_xml = response_xml.find(".//{DAV:}resourcetype")

        if resource_xml is not None:
            collection = resource_xml.find(".//{DAV:}collection") is not None
            resource_type = "directory" if collection else "file"

        self.collection = collection
        self.resource_type = resource_type

        self.display_name = extract_text("display_name")

    def as_dict(self, raw: bool = False) -> Dict[str, Any]:
        """Returns all properties that it supports parsing.

        Args:
            raw: Provides raw data instead.
        """
        if raw:
            return self.raw

        return {
            "content_length": self.content_length,
            "created": self.created,
            "modified": self.modified,
            "content_language": self.content_language,
            "content_type": self.content_type,
            "etag": self.etag,
            "type": self.resource_type,
            "display_name": self.display_name,
        }


class Response:
    """Individual response from multistatus propfind response."""

    def __init__(self, response_xml: Element) -> None:
        """Parses xml from each responses to an easier format.

        Args:
            response_xml: <d:response> element

        Note: we do parse <d:propstat> to figure out status,
        but we leave <d:prop> to ResourceProps to figure out.
        """
        self.response_xml = response_xml
        href = prop(response_xml, "href")
        assert href

        parsed = URL(href)

        # href is what was received
        self.href = href
        self.is_href_absolute = parsed.is_absolute_url

        # path is absolute path from the href
        # collection might contain `/` at the end
        self.path = parsed.path

        # path_norm is the path-absolute without differentiating
        # `/` at the end. Used as a key for the responses.
        # but does have a slash in front.
        self.path_norm = strip_leading_slash(self.path)

        status_line = prop(response_xml, "status")
        code = None
        if status_line:
            _, code_str, *_ = status_line.split()
            code = int(code_str)

        self.status_code = code
        self.reason_phrase = (
            responses[self.status_code] if self.status_code else None
        )

        self.response_description: Optional[str] = prop(
            response_xml, "responsedescription"
        )
        self.error = prop(response_xml, "error")
        self.location = prop(response_xml, "location")

        self.has_propstat = response_xml.find("{DAV:}propstat") is not None
        self.properties = DAVProperties(response_xml)

    def __str__(self) -> str:
        """User representation for the response."""
        return f"Response: {self.path_norm}"

    def __repr__(self) -> str:
        """Repr for the response."""
        return f"Response: {self.path}"

    def path_relative_to(self, base_url: URL) -> str:
        """Relative path of the resource from the response."""
        return relative_url_to(base_url, self.path_norm)


class MultiStatusResponseError(Exception):
    """Raised when multistatus response has failures in it."""

    def __init__(self, statuses: Dict[str, str]) -> None:
        """Pass multiple statuses, which is displayed when error is raised."""
        self.statuses = statuses

        assert self.statuses
        if len(self.statuses) > 1:
            msg = "multiple errors received: " + str(statuses)
        else:
            path, status = list(statuses.items())[0]
            msg = f"The resource {path} is {status.lower()}"

        self.msg = msg
        super().__init__(msg)


class MultiStatusResponse:
    """Parse multistatus responses from the received http content.

    Propfind response can contain multiple responses for multiple resources.
    The format is in xml, so we try to parse it into an easier format, and
    provide an easier way to access a response for one particular resource.

    Also note that propfind response could be partial, in that those
    properties may not exist if we are doing propfind with named properties.
    """

    def __init__(self, content: str) -> None:
        """Parse the http content from propfind request.

        Args:
             content: body received from PROPFIND call
        """
        self.content = content
        self.tree = tree = str2xml(content)

        self.response_description: Optional[str] = prop(
            tree, "responsedescription"
        )

        self.responses: Dict[str, Response] = {}
        for resp in tree.findall(".//{DAV:}response"):
            r_obj = Response(resp)
            self.responses[r_obj.path_norm] = r_obj

    def get_response_for_path(self, hostname: str, path: str) -> Response:
        """Provides response for the resource with the specific href/path.

        Args:
            hostname: Hostname path.
            path: Propfind response could have multiple responses inside
                for multiple resources (could be recursive based on the `Depth`
                as well). We use `href` to match the proper response for that
                resource.
        """
        return self.responses[join_url_path(hostname, path)]

    def raise_for_status(self) -> None:
        """Raise error if the responses in the multistatus resp. has errors."""
        statuses = {
            resp.href: resp.reason_phrase
            for resp in self.responses.values()
            if resp.reason_phrase
            and resp.status_code
            and 400 <= resp.status_code <= 599
        }
        statuses = dict(sorted(statuses.items()))
        if statuses:
            raise MultiStatusResponseError(statuses)


# TODO: support `allprop`?
def prepare_propfind_request_data(
    name: str = None, namespace: str = None
) -> Optional[str]:
    """Prepares propfind request data from specified name.

    In this case, when sent to the server, the `<prop> will only contain the
    `name` property
    """
    if not name:
        return None
    name = MAPPING_PROPS.get(name) or name
    root = Element("propfind", xmlns="DAV:")
    SubElement(
        SubElement(root, "prop"), "{DAV:}" + name, xmlns=namespace or ""
    )
    return xml2string(root, encoding="unicode")


def parse_multistatus_response(
    http_response: "HTTPResponse",
) -> MultiStatusResponse:
    """Parses multistatus response from the given http response."""
    if http_response.status_code != 207:
        raise ValueError("http response is not a multistatus response")
    return MultiStatusResponse(http_response.text)
