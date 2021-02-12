"""Testing url parsing and processing logic."""

import pytest

from webdav4.urls import (
    URL,
    join_url,
    join_url_path,
    normalize_path,
    relative_url_to,
    strip_leading_slash,
)


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


@pytest.mark.parametrize(
    "base_path, path, expected",
    [
        ("", "", "/"),
        ("", "/", "/"),
        ("/", "", "/"),
        ("/", "/", "/"),
        ("", "foo", "/foo"),
        ("", "/foo", "/foo"),
        ("", "foo/", "/foo"),
        ("", "/foo/", "/foo"),
        ("/", "foo", "/foo"),
        ("/", "/foo", "/foo"),
        ("/", "foo/", "/foo"),
        ("/", "/foo/", "/foo"),
        ("/foo", "bar", "/foo/bar"),
        ("/foo", "/bar", "/foo/bar"),
        ("/foo", "bar/", "/foo/bar"),
        ("/foo", "/bar/", "/foo/bar"),
        ("foo/", "bar", "/foo/bar"),
        ("foo/", "/bar", "/foo/bar"),
        ("foo/", "bar/", "/foo/bar"),
        ("foo/", "/bar/", "/foo/bar"),
        ("foo", "bar", "/foo/bar"),
        ("foo", "/bar", "/foo/bar"),
        ("foo", "bar/", "/foo/bar"),
        ("foo", "/bar/", "/foo/bar"),
        ("/foo/", "bar", "/foo/bar"),
        ("/foo/", "/bar", "/foo/bar"),
        ("/foo/", "bar/", "/foo/bar"),
        ("/foo/", "/bar/", "/foo/bar"),
        ("/foo/bar", "foobar", "/foo/bar/foobar"),
        ("/foo/bar/", "foobar", "/foo/bar/foobar"),
        ("/foo/bar", "foobar/", "/foo/bar/foobar"),
        ("/foo/bar", "foobar/foobar", "/foo/bar/foobar/foobar"),
    ],
)
def test_join_url_path(base_path: str, path: str, expected: str):
    """Test joining base path and path together, while normalizing the url."""
    assert join_url_path(base_path, path) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/", "/"),
        ("foo", "foo"),
        ("/foo", "/foo"),
        ("/foo/bar/", "/foo/bar"),
        ("/foo//bar//", "/foo/bar"),
        ("/////foo////bar////", "/foo/bar"),
    ],
)
def test_normalize_url(path: str, expected: str):
    """Test that it normalizes urls removing too many "/" and leading slash."""
    assert normalize_path(path) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ("", ""),
        ("/", "/"),
        ("foo", "foo"),
        ("foo/bar/", "foo/bar"),
        ("/foo/bar/", "/foo/bar"),
    ],
)
def test_strip_leading_slash(path: str, expected: str):
    """Test that it strips leading slash, except the "/" only urls."""
    assert strip_leading_slash(path) == expected
