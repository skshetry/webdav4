"""Testing date parsing logic here."""

from datetime import datetime

from dateutil.tz import tzutc

from webdav4.date_utils import from_rfc1123, fromisoformat


def test_iso8601_parsing():
    """Test parsing iso8601 format.

    creationdate is in this format.
    """
    dt = datetime(2020, 1, 2, 3, 4, 5, tzinfo=tzutc())
    datestring = "2020-1-02T03:04:05Z"
    assert fromisoformat(datestring) == dt


def test_rfc1123_parsing():
    """Test parsing rfc1123 format datetime.

    getlastmodified is in this format.
    """
    dt = datetime(2020, 1, 2, 3, 4, 5, tzinfo=tzutc())
    datestring = "Fri, 02 Jan 2020 03:04:05 GMT"
    assert from_rfc1123(datestring) == dt
