# tests/test_pipeline.py
import pytest
from datetime import datetime
from your_module import validate_dates, prepare_hours
from freezegun import freeze_time  # For time-sensitive tests

# Parameterized test data for date validation
DATE_FORMAT_CASES = [
    # (start_date, end_date, valid, description)
    ("01/01/2024", "02/01/2024", True, "valid ascending dates"),
    ("31/12/2023", "01/01/2024", True, "cross-year dates"),
    ("28/02/2024", "01/03/2024", True, "leap year dates"),
    ("15/06/2024", "15/06/2024", True, "same dates"),
    ("01/01/2024", "01/01/2023", False, "end before start"),
    ("30/02/2024", "01/03/2024", False, "invalid start date"),
    ("01/13/2024", "02/01/2024", False, "invalid month"),
    ("2024-01-01", "2024-01-02", False, "wrong date format"),
    ("invalid", "01/01/2024", False, "invalid start format"),
    ("01/01/2024", "invalid", False, "invalid end format"),
]

HOUR_CASES = [
    # (input_hours, expected_output, should_raise, description)
    (["00", "12", "23"], {"00", "12", "23"}, False, "valid hours"),
    (["0", "1"], None, True, "single-digit format"),
    (["24", "25"], None, True, "out-of-range hours"),
    (["1A", "B2"], None, True, "non-numeric characters"),
    ([" 08 ", "13 "], {"08", "13"}, False, "whitespace padding"),
    ([], None, True, "empty list"),
    (["12", "12"], {"12"}, False, "duplicate hours"),
    (["00", "23", "00"], {"00", "23"}, False, "multiple duplicates"),
]

@pytest.mark.parametrize("start,end,valid,desc", DATE_FORMAT_CASES)
def test_validate_dates(start, end, valid, desc):
    """Test date validation with various scenarios."""
    if valid:
        start_dt, end_dt = validate_dates(start, end)
        assert isinstance(start_dt, datetime)
        assert isinstance(end_dt, datetime)
        assert end_dt >= start_dt
    else:
        with pytest.raises(ValueError) as excinfo:
            validate_dates(start, end)
        
        if "invalid" in desc:
            assert "Invalid date format" in str(excinfo.value)
        elif "before" in desc:
            assert "End date must be after start date" in str(excinfo.value)

@freeze_time("2024-01-15")
def test_future_date_validation():
    """Test validation against future dates."""
    with pytest.raises(ValueError) as excinfo:
        validate_dates("01/02/2024", "01/03/2024")
    assert "Future dates not allowed" in str(excinfo.value)

@pytest.mark.parametrize("hours,expected,should_raise,desc", HOUR_CASES)
def test_prepare_hours(hours, expected, should_raise, desc):
    """Test hour preparation with various input scenarios."""
    if should_raise:
        with pytest.raises(ValueError) as excinfo:
            prepare_hours(hours)
        
        if "empty" in desc:
            assert "At least one hour required" in str(excinfo.value)
        elif any(c.isalpha() for h in hours for c in h):
            assert "Invalid hour format" in str(excinfo.value)
        else:
            assert "Hour must be between 00-23" in str(excinfo.value)
    else:
        result = prepare_hours(hours)
        assert isinstance(result, set)
        assert result == expected

def test_hour_case_insensitivity():
    """Test mixed case hour formatting."""
    result = prepare_hours(["00", "12", "23"])
    assert result == {"00", "12", "23"}

def test_hour_leading_zero_handling():
    """Test numeric conversion handling."""
    result = prepare_hours(["00", "01", "1"])
    assert "01" in result
    assert "1" not in result  # Should normalize to 2-digit format

def test_hour_type_validation():
    """Test non-list input handling."""
    with pytest.raises(TypeError):
        prepare_hours("12")  # String instead of list
        
    with pytest.raises(TypeError):
        prepare_hours({"12"})  # Set instead of list

def test_locale_specific_dates():
    """Test locale-specific date validation."""
    with pytest.raises(ValueError):
        # Test MM/DD format confusion
        validate_dates("12/31/2023", "01/01/2024")  # If expecting DD/MM format