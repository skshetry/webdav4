"""Testing common functional utilities."""

import inspect
from unittest import mock

import pytest

from webdav4.func_utils import repeat_func, retry, wrap_fn


def test_repeat_func():
    """Test repeat_func that it gets repeatedly called over n times(or inf)."""

    def always_true():
        return True

    assert list(repeat_func(always_true, times=10)) == [True] * 10
    # infinite generator
    it = repeat_func(always_true)
    assert next(it)
    assert next(it)


def test_wrap_fn():
    """Test wrap_fn which reduces arity to zero."""
    func = wrap_fn(sum, [1, 2, 3])

    params = inspect.getfullargspec(func)
    assert not any(params)
    assert func() == 6


def test_retry():
    """Test retry."""
    value = 3

    func = mock.MagicMock(side_effect=[KeyError, IndexError, value])
    wrapped = retry(3, (KeyError, IndexError), timeout=0.01)

    assert wrapped(func) == value
    assert func.call_count == 3


def test_retry_all_attempts_fail():
    """Test retry but all attempts fail."""
    value = 3

    func = mock.MagicMock(side_effect=[KeyError, IndexError, ValueError])
    wrapped = retry(3, (KeyError, IndexError, ValueError), timeout=0.01)

    with pytest.raises(ValueError):
        assert wrapped(func) == value
    assert func.call_count == 3


def test_retry_unhandled_errors():
    """Test retry for unhandled errors."""
    value = 3

    func = mock.MagicMock(side_effect=[KeyError, IndexError])
    wrapped = retry(3, [KeyError], timeout=0.01)

    with pytest.raises(IndexError):
        assert wrapped(func) == value
    assert func.call_count == 2


def test_retry_once():
    """Test retry once, i.e. no retry at all."""
    value = 3

    func = mock.MagicMock(side_effect=[KeyError])
    wrapped = retry(1, [KeyError], timeout=0.01)

    with pytest.raises(KeyError):
        assert wrapped(func) == value
    assert func.call_count == 1


def test_retry_filter_errors():
    """Test retry with extra filter for errors."""
    value = 3
    _filter = mock.MagicMock(return_value=True)

    excs = [KeyError, IndexError, ValueError]
    func = mock.MagicMock(side_effect=excs)
    wrapped = retry(3, excs, timeout=0.01, filter_errors=_filter)

    with pytest.raises(ValueError):
        assert wrapped(func) == value

    assert all(
        isinstance(val[0][0], typ)
        for val, typ in zip(_filter.call_args_list, excs)
    )

    assert func.call_count == 3


def test_retry_filter_errors_raises_false():
    """Test retry with extra filter that returns false (asking to raise)."""
    value = 3
    _filter = mock.MagicMock(return_value=False)

    excs = [KeyError, IndexError, ValueError]
    func = mock.MagicMock(side_effect=excs)
    wrapped = retry(3, excs, timeout=0.01, filter_errors=_filter)

    with pytest.raises(KeyError):
        assert wrapped(func) == value

    assert func.call_count == 1
