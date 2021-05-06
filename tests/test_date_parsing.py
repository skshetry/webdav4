"""Testing date parsing logic here."""

from datetime import datetime

import pytest as pytest
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


@pytest.mark.parametrize(
    "datestring",
    [
        "Fri, 2 Jan 2020 03:04:05 GMT",  # non-zero format in the day
        "Fri, 02 Jan 2020 03:04:05 +0000",  # rfc1123z
        "Fri Jan 02 03:04:05 UTC 2020",  # unix date
        "2020-01-02T03:04:05+0000",  # rfc3339
    ],
)
def test_non_standard_datetimes(datestring):
    """Test that we can still parse non-standard datetimes."""
    dt = datetime(2020, 1, 2, 3, 4, 5, tzinfo=tzutc())
    assert fromisoformat(datestring) == dt
    assert from_rfc1123(datestring) == dt
