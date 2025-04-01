#!/usr/bin/env python
import pytest
from unittest.mock import patch, MagicMock
from src.tasks import extract
import pandas as pd
import boto3
from moto import mock_s3
import logging

# Configure test logging
logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def s3_test_setup():
    """Mock S3 setup with test data"""
    with mock_s3():
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        prefix = "test-prefix/2023/001/00/"
        
        # Create test bucket
        s3.create_bucket(Bucket=bucket_name)
        
        # Add test files
        for i in range(1, 6):
            s3.put_object(
                Bucket=bucket_name,
                Key=f"{prefix}file_{i}.csv",
                Body=f"test content {i}"
            )
        
        yield {
            "bucket": bucket_name,
            "prefix": prefix,
            "expected_count": 5,
            "expected_columns": ["key", "size", "last_modified"]
        }

def test_extract_s3_returns_expected_file_count(s3_test_setup, caplog):
    """Test S3 extraction returns correct number of files"""
    caplog.set_level(logging.INFO)
    
    result = extract.extract_s3(
        bucket=s3_test_setup["bucket"],
        prefix=s3_test_setup["prefix"]
    )
    
    # Basic validation
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == s3_test_setup["expected_count"]
    
    # Log validation
    assert f"Scanning {s3_test_setup['bucket']}/{s3_test_setup['prefix']}" in caplog.text
    assert f"Found {s3_test_setup['expected_count']} files" in caplog.text

def test_extract_s3_dataframe_structure(s3_test_setup):
    """Test returned dataframe has expected structure"""
    result = extract.extract_s3(
        bucket=s3_test_setup["bucket"],
        prefix=s3_test_setup["prefix"]
    )
    
    # Column check
    assert all(col in result.columns for col in s3_test_setup["expected_columns"])
    
    # Type check
    assert pd.api.types.is_datetime64_any_dtype(result["last_modified"])
    assert pd.api.types.is_integer_dtype(result["size"])

@patch('boto3.client')
def test_extract_s3_handles_large_datasets(mock_boto, s3_test_setup):
    """Test pagination handling for large result sets"""
    mock_client = MagicMock()
    mock_boto.return_value = mock_client
    
    # Mock paginated response
    mock_client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": f"file_{i}", "Size": 100, "LastModified": "2023-01-01"} 
                      for i in range(1, 1001)]},
        {"Contents": [{"Key": f"file_{i}", "Size": 100, "LastModified": "2023-01-01"} 
                      for i in range(1001, 1501)]}
    ]
    
    result = extract.extract_s3(
        bucket="large-bucket",
        prefix="massive-dataset/"
    )
    
    assert result.shape[0] == 1500

def test_extract_s3_empty_prefix(s3_test_setup):
    """Test handling of empty prefix results"""
    with pytest.raises(ValueError) as excinfo:
        extract.extract_s3(
            bucket=s3_test_setup["bucket"],
            prefix="non-existent-prefix/"
        )
    
    assert "No files found" in str(excinfo.value)

def test_extract_s3_error_handling(s3_test_setup):
    """Test error handling for invalid buckets"""
    with pytest.raises(Exception) as excinfo:
        extract.extract_s3(
            bucket="invalid-bucket",
            prefix=s3_test_setup["prefix"]
        )
    
    assert "Access Denied" in str(excinfo.value) or "Not Found" in str(excinfo.value)

@pytest.mark.integration
def test_real_s3_extraction():
    """Integration test with real S3 (opt-in with pytest -m integration)"""
    result = extract.extract_s3(
        bucket="noaa-goes18",
        prefix="GLM-L2-LCFA/2023/048/21/"
    )
    
    assert not result.empty
    assert "GLM-L2-LCFA" in result["key"].iloc[0]

if __name__ == "__main__":
    pytest.main(["-v", __file__])