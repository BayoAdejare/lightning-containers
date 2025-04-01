#!/usr/bin/env python
import pytest
from unittest.mock import patch
from src.flows import extract
import pandas as pd
import boto3
from moto import mock_s3
import logging
from botocore.exceptions import ClientError

# Configure test logging
logger = logging.getLogger(__name__)

@pytest.fixture(scope="function")  # Changed to function scope for test isolation
def s3_test_setup():
    """Mock S3 setup with test data"""
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')  # Explicit region
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
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == s3_test_setup["expected_count"]
    assert all(result["key"].str.startswith(s3_test_setup["prefix"]))
    
    # Verify logging contains expected messages
    assert f"Scanning {s3_test_setup['bucket']}/{s3_test_setup['prefix']}" in caplog.text
    assert f"Found {s3_test_setup['expected_count']} files" in caplog.text

def test_extract_s3_dataframe_structure(s3_test_setup):
    """Test returned dataframe has expected structure"""
    result = extract.extract_s3(
        bucket=s3_test_setup["bucket"],
        prefix=s3_test_setup["prefix"]
    )
    
    # Validate column presence and data types
    assert set(result.columns) == set(s3_test_setup["expected_columns"])
    assert pd.api.types.is_datetime64_any_dtype(result["last_modified"])
    assert pd.api.types.is_integer_dtype(result["size"])

def test_extract_s3_handles_large_datasets(s3_test_setup):
    """Test pagination handling for large result sets using moto"""
    s3 = boto3.client('s3', region_name='us-east-1')
    
    # Add 1500 test objects
    for i in range(1, 1501):
        s3.put_object(
            Bucket=s3_test_setup["bucket"],
            Key=f"{s3_test_setup['prefix']}large_file_{i}.csv",
            Body=f"large content {i}"
        )
    
    result = extract.extract_s3(
        bucket=s3_test_setup["bucket"],
        prefix=s3_test_setup["prefix"]
    )
    
    assert result.shape[0] == 1500

def test_extract_s3_empty_prefix(s3_test_setup):
    """Test handling of empty results"""
    with pytest.raises(ValueError, match="No files found"):
        extract.extract_s3(
            bucket=s3_test_setup["bucket"],
            prefix="non-existent-prefix/"
        )

def test_extract_s3_error_handling():
    """Test error handling for invalid buckets"""
    with mock_s3():
        with pytest.raises(ClientError) as excinfo:
            extract.extract_s3(
                bucket="invalid-bucket",
                prefix="any-prefix/"
            )
        
        assert excinfo.value.response["Error"]["Code"] in ["404", "403"]

@pytest.mark.integration
@patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"})
def test_real_s3_extraction():
    """Integration test with real S3 (requires credentials)"""
    pytest.importorskip("botocore")  # Ensure boto3 is available
    
    try:
        result = extract.extract_s3(
            bucket="noaa-goes18",
            prefix="GLM-L2-LCFA/2023/048/21/"
        )
        assert not result.empty
        assert "GLM-L2-LCFA" in result["key"].iloc[0]
    except ClientError as e:
        if "InvalidAccessKeyId" in str(e):
            pytest.skip("Valid AWS credentials required for integration test")
        raise

if __name__ == "__main__":
    pytest.main(["-v", __file__])