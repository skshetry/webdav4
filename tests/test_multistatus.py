"""Testing multistatus responses."""
from datetime import datetime
from typing import Optional, Tuple
from xml.etree.ElementTree import Element, fromstring

import pytest
from dateutil.tz import tzutc
from httpx import Response as HTTPResponse

from webdav4.multistatus import (
    DAVProperties,
    MultiStatusResponse,
    MultiStatusResponseError,
    Response,
    parse_multistatus_response,
    prepare_propfind_request_data,
    prop,
)
from webdav4.urls import URL


def test_prop():
    """Test getting text from xml element."""
    status_line = "<d:status>HTTP/1.1 200 OK</d:status>"
    content = f'<d:multistatus xmlns:d="DAV:">{status_line}</d:multistatus>'
    elem = fromstring(content)
    assert prop(elem, "status") == "HTTP/1.1 200 OK"


def test_prop_relative():
    """Test getting text anywhere from xml element."""
    status_line = "<d:status>HTTP/1.1 200 OK</d:status>"
    response = f"<d:response>{status_line}</d:response>"
    content = f'<d:multistatus xmlns:d="DAV:">{response}</d:multistatus>'
    elem = fromstring(content)
    assert prop(elem, "status") is None
    assert prop(elem, "status", relative=True) == "HTTP/1.1 200 OK"


@pytest.mark.parametrize(
    "args",
    [
        (fromstring('<d:propstat xmlns:d="DAV:"></d:propstat>')),
        (),
    ],
)
def test_dav_properties_empty(args: Tuple[Element]):
    """Test dav properties when it's empty."""
    props = DAVProperties(*args)
    assert (
        props.raw
        == {
            "modified": None,
            "created": None,
            "content_length": None,
            "etag": None,
            "content_type": None,
            "content_language": None,
            "display_name": None,
        }
        == props.as_dict(raw=True)
    )

    assert props.as_dict() == {**props.raw, "type": None}
    assert props.modified is None
    assert props.created is None
    assert props.content_length is None
    assert props.content_language is None
    assert props.etag is None
    assert props.content_type is None
    assert props.display_name is None
    assert props.collection is None
    assert props.resource_type is None


def test_dav_properties():
    """Test simple dav properties extraction."""
    content = """\
    <d:response xmlns:d="DAV:">
      <d:propstat>
        <d:prop>
          <d:getlastmodified>Fri, 10 Feb 2020 10:10:10 GMT</d:getlastmodified>
          <d:creationdate>2020-1-02T03:04:05Z</d:creationdate>
          <d:getcontentlength>136314880</d:getcontentlength>
          <d:getcontentlanguage>en-US</d:getcontentlanguage>
          <d:displayname>foo</d:displayname>
          <d:resourcetype/>
          <d:getetag>"8db748065bfed5c0731e9c7ee5f9bf4c"</d:getetag>
          <d:getcontenttype>text/plain</d:getcontenttype>
        </d:prop>
      </d:propstat>
    </d:response>"""
    elem = fromstring(content)

    props = DAVProperties(elem)
    assert props.response_xml == elem
    assert (
        props.raw
        == {
            "modified": "Fri, 10 Feb 2020 10:10:10 GMT",
            "created": "2020-1-02T03:04:05Z",
            "content_length": "136314880",
            "etag": '"8db748065bfed5c0731e9c7ee5f9bf4c"',
            "content_type": "text/plain",
            "content_language": "en-US",
            "display_name": "foo",
        }
        == props.as_dict(raw=True)
    )
    assert props.created == datetime(2020, 1, 2, 3, 4, 5, tzinfo=tzutc())
    assert props.modified == datetime(2020, 2, 10, 10, 10, 10, tzinfo=tzutc())
    assert props.content_length == 136314880
    assert props.etag == '"8db748065bfed5c0731e9c7ee5f9bf4c"'
    assert props.content_type == "text/plain"
    assert props.content_language == "en-US"
    assert props.display_name == "foo"
    assert props.collection is False
    assert props.resource_type == "file"

    assert props.as_dict() == {
        "created": datetime(2020, 1, 2, 3, 4, 5, tzinfo=tzutc()),
        "modified": datetime(2020, 2, 10, 10, 10, 10, tzinfo=tzutc()),
        "content_length": 136314880,
        "etag": '"8db748065bfed5c0731e9c7ee5f9bf4c"',
        "content_type": "text/plain",
        "content_language": "en-US",
        "display_name": "foo",
        "type": "file",
    }


def test_dav_properties_partial():
    """Test that it is still readable if there's partial propfind response."""
    content = """\
    <d:propstat xmlns:d="DAV:">
      <d:prop>
        <d:displayname>foo</d:displayname>
        <d:resourcetype><d:collection/></d:resourcetype>
      </d:prop>
    </d:propstat>"""
    elem = fromstring(content)

    props = DAVProperties(elem)
    assert (
        props.raw
        == {
            "modified": None,
            "created": None,
            "content_length": None,
            "etag": None,
            "content_type": None,
            "content_language": None,
            "display_name": "foo",
        }
        == props.as_dict(raw=True)
    )

    assert props.as_dict() == {**props.raw, "type": "directory"}
    assert props.modified is None
    assert props.created is None
    assert props.content_length is None
    assert props.content_language is None
    assert props.etag is None
    assert props.content_type is None
    assert props.display_name == "foo"
    assert props.collection is True
    assert props.resource_type == "directory"


@pytest.mark.parametrize(
    "href, absolute",
    [
        ("/remote.php/dav/files/admin/dir/", False),
        ("https://example.org/remote.php/dav/files/admin/dir/", True),
    ],
)
def test_response(href: str, absolute: bool):
    """Test common scenarios for Response object."""
    content = f"""
    <d:response xmlns:d="DAV:">
        <d:href>{href}</d:href>
        <d:status>HTTP/1.1 423 Locked</d:status>
        <d:error><d:lock-token-submitted/></d:error>
    </d:response>
    """
    elem = fromstring(content)

    response = Response(elem)
    assert response.response_xml == elem
    assert response.href == href
    assert response.is_href_absolute == absolute
    assert response.path == "/remote.php/dav/files/admin/dir/"
    assert response.path_norm == "/remote.php/dav/files/admin/dir"
    assert response.error == ""
    assert response.has_propstat is False

    assert isinstance(response.properties, DAVProperties)
    assert response.response_description is None
    assert response.properties.response_xml is elem
    assert response.status_code == 423
    assert response.reason_phrase == "Locked"
    assert response.location is None

    assert str(response) == f"Response: {response.path_norm}"
    assert repr(response) == f"Response: {response.path}"


@pytest.mark.parametrize(
    "href",
    [
        "/remote.php/dav/files/admin/dir/",
        "/remote.php/dav/files/admin/dir",
        "https://example.org/remote.php/dav/files/admin/dir/",
        "https://example.org/remote.php/dav/files/admin/dir",
    ],
)
@pytest.mark.parametrize(
    "base_url, expected",
    [
        ("https://example.org", "remote.php/dav/files/admin/dir"),
        ("https://example.org/", "remote.php/dav/files/admin/dir"),
        ("https://example.org/remote.php", "dav/files/admin/dir"),
        ("https://example.org/remote.php/", "dav/files/admin/dir"),
        ("https://example.org/remote.php/dav", "files/admin/dir"),
        ("https://example.org/remote.php/dav/", "files/admin/dir"),
        ("https://example.org/remote.php/dav/files/admin/dir", "/"),
        ("https://example.org/remote.php/dav/files/admin/dir", "/"),
    ],
)
def test_path_relative_to(href: str, base_url: str, expected: str):
    """Test path relative to calculation from Response of the resource."""
    content = f"""
    <d:response xmlns:d="DAV:">
        <d:href>{href}</d:href>
        <d:status>HTTP/1.1 423 Locked</d:status>
        <d:error><d:lock-token-submitted/></d:error>
    </d:response>
    """
    elem = fromstring(content)

    response = Response(elem)
    assert response.href == href
    assert response.path_relative_to(URL(base_url)) == expected


def test_get_response_for_path():
    """Test getting response for appropriate path."""
    content = """
    <d:multistatus xmlns:d="DAV:">
        <d:response>
            <d:href>/remote.php/dav/files/admin/sample1.txt</d:href>
            <d:propstat>
                <d:prop><d:resourcetype/></d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/remote.php/dav/files/admin/sample2.txt</d:href>
            <d:propstat>
                <d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
    </d:multistatus>
    """

    response = MultiStatusResponse(content)
    response.raise_for_status()
    assert response.content == content

    expected_keys = [
        "/remote.php/dav/files/admin/sample1.txt",
        "/remote.php/dav/files/admin/sample2.txt",
    ]

    assert list(response.responses.keys()) == expected_keys
    assert response.response_description is None

    prop1 = response.responses[expected_keys[0]].properties
    assert prop1.resource_type == "file"

    prop2 = response.responses[expected_keys[1]].properties
    assert prop2.resource_type == "directory"

    assert (
        response.get_response_for_path(
            "/remote.php/dav/files", "admin/sample1.txt"
        )
        == response.responses[expected_keys[0]]
    )
    assert (
        response.get_response_for_path(
            "/remote.php/dav/files/admin", "sample2.txt"
        )
        == response.responses[expected_keys[1]]
    )


def test_raise_for_status():
    """Test raising exception from multistatus response."""
    content = """\
    <d:multistatus xmlns:d='DAV:'>
        <d:response>
            <d:href>/othercontainer/C2/</d:href>
            <d:status>HTTP/1.1 423 Locked</d:status>
            <d:error><d:lock-token-submitted/></d:error>
        </d:response>
        <d:response>
            <d:href>/othercontainer/C3/</d:href>
            <d:status>HTTP/1.1 423 Locked</d:status>
            <d:error><d:lock-token-submitted/></d:error>
        </d:response>
    </d:multistatus>
    """
    response = MultiStatusResponse(content)
    with pytest.raises(MultiStatusResponseError) as exc_info:
        response.raise_for_status()

    expected = {
        "/othercontainer/C2/": "Locked",
        "/othercontainer/C3/": "Locked",
    }
    assert str(exc_info.value) == f"multiple errors received: {str(expected)}"
    assert exc_info.value.statuses == expected


def test_prepare_propfind_data_empty():
    """Test preparing propfind data, without specifying any parameters.

    Should return empty in this case.
    """
    return prepare_propfind_request_data() is None


@pytest.mark.parametrize(
    ["name", "namespace", "expected_inner_element"],
    [
        ("created", None, '<prop><ns0:creationdate xmlns="" />'),
        ("modified", "newDAV", '<prop><ns0:getlastmodified xmlns="newDAV" />'),
        ("something", None, '<prop><ns0:something xmlns="" />'),
    ],
    # unmapped as in, we haven't mapped this property internally
    # as we do with `created -> creationdate` and `modified -> getlastmodified`
    # it should be sent as-is.
    ids=["simple", "namespaced", "unmapped_property"],
)
def test_prepare_propfind_data(
    name: str, namespace: Optional[str], expected_inner_element: str
):
    """Test preparing propfind xml string data."""
    body = prepare_propfind_request_data(name, namespace=namespace)
    expected = '<propfind xmlns:ns0="DAV:" xmlns="DAV:">{0}</prop></propfind>'
    assert body == expected.format(expected_inner_element)


def test_try_parse_multistatus_response_for_not_a_207_response():
    """Test trying to parse with a multistatus response that's not 207."""
    with pytest.raises(ValueError) as exc_info:
        parse_multistatus_response(HTTPResponse(status_code=404))

    assert str(exc_info.value) == "http response is not a multistatus response"


def test_parse_multistatus_response():
    """Test trying to parse multistatus response."""
    res = parse_multistatus_response(
        HTTPResponse(status_code=207, text="<d></d>")
    )
    assert isinstance(res, MultiStatusResponse)
    assert res.responses == {}
    assert res.response_description is None
    assert res.content == "<d></d>"

    res.raise_for_status()


def test_check_multistatus():
    """Test MultiStatusError string representation."""
    statuses = {"/data/bar": "Locked"}
    error = MultiStatusResponseError(statuses)
    assert str(error) == "The resource /data/bar is locked"

    statuses = {"/data/bar": "Locked", "http://example.org": "Bad Gateway"}
    error = MultiStatusResponseError(statuses)
    assert str(error) == "multiple errors received: " + str(statuses)
