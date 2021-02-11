"""Testing url parsing and processing logic."""

import pytest

from webdav4.urls import URL, join_url, relative_url_to


@pytest.mark.parametrize(
    "base_url, path, expected",
    [
        ("https://example.org", "/", "https://example.org/"),
        ("https://example.org/", "/", "https://example.org/"),
        ("https://example.org", "", "https://example.org/"),
        ("https://example.org/", "", "https://example.org/"),
        ("https://example.org", "path", "https://example.org/path"),
        ("https://example.org/", "path", "https://example.org/path"),
        ("https://example.org/", "/path", "https://example.org/path"),
        ("https://example.org/", "/path/", "https://example.org/path"),
        ("https://example.org/", "path/", "https://example.org/path"),
        ("https://example.org/foo", "bar", "https://example.org/foo/bar"),
        ("https://example.org/foo/", "bar", "https://example.org/foo/bar"),
        ("https://example.org/foo/", "/bar", "https://example.org/foo/bar"),
        ("https://example.org/foo", "/bar", "https://example.org/foo/bar"),
        ("https://example.org/foo/", "/bar/", "https://example.org/foo/bar"),
    ],
)
def test_join_url(base_url: str, path: str, expected: str):
    """Testing join_url operation in the client."""
    assert join_url(URL(base_url), path) == expected


@pytest.mark.parametrize(
    "base, rel, expected",
    [
        ("https://example.org/foo", "foo", "/"),
        ("https://example.org/foo", "/foo", "/"),
        ("https://example.org/foo", "foo/", "/"),
        ("https://example.org/foo", "/foo/", "/"),
        ("https://example.org/foo/", "foo", "/"),
        ("https://example.org/foo/", "/foo", "/"),
        ("https://example.org/foo/", "foo/", "/"),
        ("https://example.org/foo/", "/foo/", "/"),
        ("https://example.org", "/", "/"),
        ("https://example.org/", "/", "/"),
        ("https://example.org", "", "/"),
        ("https://example.org/", "", "/"),
        ("https://example.org", "data", "data"),
        ("https://example.org", "/data", "data"),
        ("https://example.org", "data/", "data"),
        ("https://example.org", "/data/", "data"),
        ("https://example.org/", "data", "data"),
        ("https://example.org/", "/data", "data"),
        ("https://example.org/", "data/", "data"),
        ("https://example.org/", "/data/", "data"),
        ("https://example.org/foo", "foo/bar", "bar"),
        ("https://example.org/foo", "/foo/bar", "bar"),
        ("https://example.org/foo", "foo/bar/", "bar"),
        ("https://example.org/foo", "/foo/bar/", "bar"),
        ("https://example.org/foo/", "foo/bar", "bar"),
        ("https://example.org/foo/", "/foo/bar", "bar"),
        ("https://example.org/foo/", "foo/bar/", "bar"),
        ("https://example.org/foo/", "/foo/bar/", "bar"),
    ],
)
def test_path_relative_to(base: str, rel: str, expected: str):
    """Test relative path calculation."""
    assert relative_url_to(URL(base), rel) == expected
