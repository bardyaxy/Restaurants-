import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pytest
import math
from utils import normalize_hours, haversine_miles


def test_normalize_hours_basic():
    hours = {
        "Monday": "9 AM - 5 pm",
        "Tuesday": "10-11 pm",
        "Wednesday": "",
    }
    expected = {
        "Mon": "9 AM – 5 PM",
        "Tue": "10 PM – 11 PM",
    }
    assert normalize_hours(hours) == expected


def test_haversine_zero_distance():
    assert haversine_miles(47.0, -122.0, 47.0, -122.0) == pytest.approx(0.0)


def test_haversine_one_degree_latitude():
    dist = haversine_miles(47.0, -122.0, 48.0, -122.0)
    assert dist == pytest.approx(69.09, rel=1e-2)


def test_haversine_one_degree_longitude():
    dist = haversine_miles(47.0, -122.0, 47.0, -123.0)
    assert dist == pytest.approx(47.12, rel=1e-2)


def test_haversine_invalid_none():
    assert haversine_miles(None, -122.0, 47.0, -123.0) is None
    assert haversine_miles(47.0, None, 47.0, -123.0) is None


def test_haversine_invalid_nan():
    nan = math.nan
    assert haversine_miles(nan, -122.0, 47.0, -123.0) is None
    assert haversine_miles(47.0, nan, 47.0, -123.0) is None
