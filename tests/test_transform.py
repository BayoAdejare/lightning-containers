#!/usr/bin/env python
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from lightning_streams.assets.etl import transform
import logging

# Configure logging
logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def raw_data():
    """Sample raw data fixture with edge cases"""
    return pd.DataFrame({
        'timestamp': [
            '2023-01-01 12:00', 
            'invalid_date', 
            '2023-01-01 18:30',
            None
        ],
        'value': ['100.5', 'missing', '200.75', '300.0'],
        'category': ['A', 'B', 'C', None],
        'is_active': ['true', 'false', 'invalid', '1']
    })

@pytest.fixture
def cleaned_data():
    """Sample cleaned data for transformation validation"""
    return pd.DataFrame({
        'event_time': pd.to_datetime([
            '2023-01-01 12:00', 
            '2023-01-01 18:30'
        ]),
        'measurement': [100.5, 200.75],
        'group': ['A', 'C'],
        'status': [True, False]
    })

def test_clean_timestamp(raw_data, caplog):
    """Test datetime parsing with error handling"""
    caplog.set_level(logging.WARNING)
    
    result = transform.clean_timestamp(raw_data.copy())
    
    # Validate datetimes
    assert pd.api.types.is_datetime64_any_dtype(result['timestamp'])
    
    # Check invalid date handling
    assert result['timestamp'].isna().sum() == 2
    assert "Failed to parse timestamp" in caplog.text
    
    # Check valid parses
    valid_dates = result['timestamp'].dropna()
    assert valid_dates.dt.date.unique()[0] == pd.Timestamp('2023-01-01').date()

def test_convert_numeric(raw_data, caplog):
    """Test numeric conversion with error logging"""
    caplog.set_level(logging.WARNING)
    
    result = transform.convert_numeric(raw_data.copy(), 'value')
    
    # Validate numeric type
    assert pd.api.types.is_float_dtype(result['value'])
    
    # Check conversion results
    assert result['value'].tolist() == [100.5, np.nan, 200.75, 300.0]
    assert "Invalid numeric value" in caplog.text

def test_encode_boolean(raw_data):
    """Test boolean encoding from various string inputs"""
    result = transform.encode_boolean(raw_data.copy(), 'is_active')
    
    # Validate boolean type
    assert pd.api.types.is_bool_dtype(result['is_active'])
    
    # Check encoding results
    assert result['is_active'].tolist() == [True, False, False, True]

def test_validate_schema(cleaned_data):
    """Validate transformed data schema"""
    required_columns = {
        'event_time': 'datetime64[ns]',
        'measurement': 'float64',
        'group': 'object',
        'status': 'bool'
    }
    
    # Check column presence
    assert set(cleaned_data.columns) == set(required_columns.keys())
    
    # Check data types
    for col, dtype in required_columns.items():
        assert str(cleaned_data[col].dtype) == dtype

def test_handle_nulls(raw_data):
    """Test null handling strategies"""
    # Test different null handling methods
    result_drop = transform.handle_nulls(raw_data.copy(), strategy='drop')
    assert len(result_drop) == 2
    
    result_fill = transform.handle_nulls(
        raw_data.copy(), 
        strategy='fill',
        fill_values={'category': 'Unknown'}
    )
    assert result_fill['category'].isna().sum() == 0
    assert 'Unknown' in result_fill['category'].values

def test_aggregation_transforms(cleaned_data):
    """Test aggregation and feature engineering"""
    result = transform.add_aggregate_features(cleaned_data.copy())
    
    # Check new features
    assert 'hour_of_day' in result.columns
    assert 'measurement_zscore' in result.columns
    
    # Validate z-score calculation
    zscores = result['measurement_zscore'].dropna()
    assert np.allclose(zscores.mean(), 0, atol=1e-8)
    assert np.allclose(zscores.std(), 1, atol=1e-8)

def test_error_handling(raw_data):
    """Test error conditions and exception raising"""
    # Test invalid strategy
    with pytest.raises(ValueError):
        transform.handle_nulls(raw_data.copy(), strategy='invalid')
    
    # Test missing column
    with pytest.raises(KeyError):
        transform.convert_numeric(raw_data.copy(), 'missing_column')

def test_idempotency(cleaned_data):
    """Test transform is idempotent when reapplied"""
    first_pass = transform.apply_full_pipeline(cleaned_data.copy())
    second_pass = transform.apply_full_pipeline(first_pass)
    
    pd.testing.assert_frame_equal(first_pass, second_pass)

@pytest.mark.parametrize("input_val,expected", [
    ("100K", 100_000),
    ("1.5M", 1_500_000),
    ("invalid", np.nan),
    (None, np.nan)
])
def test_custom_parser(input_val, expected, caplog):
    """Test custom value parser with parameterized inputs"""
    caplog.set_level(logging.DEBUG)
    result = transform.parse_custom_value(input_val)
    
    if np.isnan(expected):
        assert np.isnan(result)
    else:
        assert result == expected
    
    if input_val == "invalid":
        assert "Failed to parse value" in caplog.text

if __name__ == "__main__":
    pytest.main(["-v", __file__])