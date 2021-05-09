"""Testing common functional utilities."""

from webdav4.func_utils import repeat_func


def test_repeat_func():
    """Test repeat_func that it gets repeatedly called over n times(or inf)."""

    def always_true():
        return True

    assert list(repeat_func(always_true, times=10)) == [True] * 10
    # infinite generator
    it = repeat_func(always_true)
    assert next(it)
    assert next(it)
