# tests/test_pipeline.py
import pytest
from datetime import datetime
from your_module import validate_dates, prepare_hours

def test_validate_dates_valid():
    start, end = validate_dates("01/01/2024", "02/01/2024")
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert end > start

def test_validate_dates_invalid():
    with pytest.raises(ValueError):
        validate_dates("02/01/2024", "01/01/2024")  # end before start

def test_prepare_hours_valid():
    hours = prepare_hours(["00", "01", "23"])
    assert all(h in hours for h in ["00", "01", "23"])

def test_prepare_hours_invalid():
    with pytest.raises(ValueError):
        prepare_hours(["24"])  # invalid hour
