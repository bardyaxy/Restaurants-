import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pytest
from utils import normalize_hours


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
