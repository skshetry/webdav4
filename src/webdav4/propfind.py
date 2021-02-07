"""Parsing propfind response."""

from http.client import responses
from typing import TYPE_CHECKING, Dict, Optional, Union
from xml.etree.ElementTree import Element, ElementTree, SubElement
from xml.etree.ElementTree import fromstring as str2xml
from xml.etree.ElementTree import tostring as xml2string

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


def prop(node: Union[Element, ElementTree], name: str) -> Optional[str]:
    """Returns text of the property if it exists under DAV namespace."""
    xpath = "{DAV:}" + name
    return node.findtext(xpath)


class ResourceProps:
    """Parses <d:prop> data into certain properties.

    Only supports a certain set of properties to extract. Others are ignored.
    """

    def __init__(self, props: Element = None):
        """Parses props to certain attributes.

        Args:
             props: <d:prop> element
        """
        self.props: Union[ElementTree, Element] = (
            ElementTree() if props is None else props
        )

    @property
    def modified(self) -> Optional[str]:
        """Returns last modified data from the propfind response."""
        return prop(self.props, MAPPING_PROPS["modified"])

    @property
    def collection(self) -> bool:
        """Figures out if the resource is a collection from the response."""
        return (
            self.props.find("{DAV:}resourcetype/{DAV:}collection") is not None
        )

    @property
    def resource_type(self) -> str:
        """Tells whether the resource is directory or a file.

        It might not be any of those, but this is mostly used
        in relation to files.
        """
        return "directory" if self.collection else "file"

    @property
    def etag(self) -> Optional[str]:
        """Returns etag from the propfind response."""
        return prop(self.props, MAPPING_PROPS["etag"])

    @property
    def content_type(self) -> Optional[str]:
        """Returns content type from the propfind response."""
        return prop(self.props, MAPPING_PROPS["content_type"])

    @property
    def content_length(self) -> Optional[int]:
        """Returns content length of the resource from the response."""
        length = prop(self.props, MAPPING_PROPS["content_length"])
        return int(length) if length else None

    @property
    def content_language(self) -> Optional[str]:
        """Returns content language from the propfind response if exists."""
        return prop(self.props, MAPPING_PROPS["content_language"])

    @property
    def created(self) -> Optional[str]:
        """Returns creation date from the propfind response."""
        return prop(self.props, MAPPING_PROPS["created"])

    @property
    def display_name(self) -> Optional[str]:
        """Returns display name of the reource from the propfind response."""
        return prop(self.props, MAPPING_PROPS["display_name"])


class Response:
    """Individual response from multistatus propfind response."""

    def __init__(self, xml_resp: Element) -> None:
        """Parses xml from each responses to an easier format.

        Args:
            xml_resp: <d:response> element

        Note: we do parse <d:propstat> to figure out status,
        but we leave <d:prop> to ResourceProps to figure out.
        """
        self.xml_resp = xml_resp

    @property
    def href(self) -> str:
        """Returns href of the resource."""
        path = prop(self.xml_resp, "href")
        assert path
        return path

    @property
    def status_code(self) -> Optional[int]:
        """Returns status code from multistatus response."""
        status_line = self.xml_resp.findtext("{DAV:}status")
        if not status_line:
            return None

        _, code, *_ = status_line.split()
        assert code
        return int(code)

    @property
    def reason_phrase(self) -> Optional[str]:
        """Reason phrase from the status code."""
        if not self.status_code:
            return None
        return responses[self.status_code]

    @property
    def props(self) -> ResourceProps:
        """Extract properties of the resource wrapped w/ ResourceProps."""
        return ResourceProps(self.xml_resp.find("{DAV:}propstat/{DAV:}prop"))


class PropfindData:
    """Parse propfind data from the response.

    Propfind response can contain multiple responses for multiple resources.
    The format is in xml, so we try to parse it into an easier format, and
    provide an easier way to access a response for one particular resource.

    Also note that propfind response could be partial, in that those
    properties may not exist if we are doing propfind with named properties.
    """

    def __init__(self, response: "HTTPResponse") -> None:
        """Parse the http response from propfind request.

        Args:
             response: response received from PROPFIND call
        """
        self.responses: Dict[str, Response] = {}

        tree = str2xml(response.text)
        for resp in tree.findall(".//{DAV:}response"):
            r_obj = Response(resp)
            self.responses[r_obj.href] = r_obj

    def get_response_for_path(self, path: str) -> Response:
        """Provides response for the resource with the specific href/path.

        Args:
            path: Propfind response could have multiple responses inside
                for multiple resources (could be recursive based on the `Depth`
                as well). We use `href` to match the proper response for that
                resource.
        """
        return self.responses[path]


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
