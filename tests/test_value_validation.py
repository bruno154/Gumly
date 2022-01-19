from mlutils.value_validation import assert_check_number, check_number

import pytest


def test_argument_validation_check_number():

    assert True == check_number(1, 0, 100)
    assert False == check_number(-1, 0, 100)
    assert True == check_number(0.25, 0, 1)
    assert False == check_number("string")
    assert False == check_number(list(), 0, 1)
    assert False == check_number(dict())


def test_argument_validation_check():

    lower_percentil = 1

    assert_check_number(lower_percentil, 0, 1.0, "lower_percentil")


def test_argument_validation_error():
    with pytest.raises(AssertionError):

        lower_percentil = -1

        assert_check_number(lower_percentil, 0, 1.0, "lower_percentil")
