import pytest
from datetime import date

from openaq_api.v3.models.utils import fix_date


def test_infinity_date():
    d = fix_date("infinity")
    assert d == None


def test_string_date():
    d = fix_date("2024-01-01")
    assert isinstance(d, date)
