#!/usr/bin/env python
import pytest
from unittest.mock import patch, MagicMock
from src.tasks import sink
import pandas as pd
import boto3
from moto import mock_s3
import os
import tempfile
import shutil

# Configure logging
import logging
logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def s3_test_env():
    """Mock S3 environment for load testing"""
    with mock_s3():
        s3 = boto3.client('s3')
        bucket_name = "test-load-bucket"
        s3.create_bucket(Bucket=bucket_name)
        
        yield {
            "bucket": bucket_name,
            "test_df": pd.DataFrame({
                'id': [1, 2, 3],
                'data': ['A', 'B', 'C']
            }),
            "test_path": "processed/data_20230101.parquet"
        }

@pytest.fixture
def local_fs_env():
    """Local filesystem test environment"""
    temp_dir = tempfile.mkdtemp()
    yield {
        "base_path": temp_dir,
        "test_data": pd.DataFrame({
            'timestamp': pd.date_range('2023-01-01', periods=3),
            'value': [10.5, 11.2, 9.8]
        })
    }
    shutil.rmtree(temp_dir)

def test_load_s3_success(s3_test_env, caplog):
    """Test successful S3 load operation"""
    caplog.set_level(logging.INFO)
    
    result = sink.load_to_s3(
        data=s3_test_env["test_df"],
        bucket=s3_test_env["bucket"],
        s3_key=s3_test_env["test_path"],
        file_format="parquet"
    )
    
    # Verify return value
    assert result == f"s3://{s3_test_env['bucket']}/{s3_test_env['test_path']}"
    
    # Verify object exists
    s3 = boto3.client('s3')
    response = s3.head_object(
        Bucket=s3_test_env["bucket"],
        Key=s3_test_env["test_path"]
    )
    assert response['ContentLength'] > 0
    
    # Verify logs
    assert "Successfully uploaded" in caplog.text
    assert "parquet" in caplog.text

def test_load_local_filesystem(local_fs_env):
    """Test local filesystem load"""
    test_path = os.path.join(
        local_fs_env["base_path"],
        "output/data_20230101.csv"
    )
    
    sink.load_to_fs(
        data=local_fs_env["test_data"],
        path=test_path,
        format="csv"
    )
    
    # Verify file existence
    assert os.path.exists(test_path)
    
    # Verify content
    loaded_df = pd.read_csv(test_path)
    pd.testing.assert_frame_equal(loaded_df, local_fs_env["test_data"])

def test_load_s3_invalid_credentials(s3_test_env):
    """Test error handling for invalid credentials"""
    with patch('boto3.client') as mock_client:
        mock_client.side_effect = Exception("AWS Credentials Error")
        
        with pytest.raises(Exception) as excinfo:
            sink.load_to_s3(
                data=s3_test_env["test_df"],
                bucket=s3_test_env["bucket"],
                s3_key="invalid/path.parquet"
            )
            
    assert "AWS Credentials" in str(excinfo.value)

def test_load_file_format_validation(s3_test_env):
    """Test unsupported file format handling"""
    with pytest.raises(ValueError) as excinfo:
        sink.load_to_s3(
            data=s3_test_env["test_df"],
            bucket=s3_test_env["bucket"],
            s3_key="data.unsupported",
            file_format="unsupported"
        )
        
    assert "Unsupported file format" in str(excinfo.value)

def test_load_data_integrity(s3_test_env):
    """Test data integrity after load"""
    test_key = "integrity_check.parquet"
    
    # Load and retrieve data
    sink.load_to_s3(
        data=s3_test_env["test_df"],
        bucket=s3_test_env["bucket"],
        s3_key=test_key
    )
    
    s3 = boto3.client('s3')
    obj = s3.get_object(
        Bucket=s3_test_env["bucket"],
        Key=test_key
    )
    loaded_df = pd.read_parquet(obj['Body'])
    
    pd.testing.assert_frame_equal(
        loaded_df.reset_index(drop=True),
        s3_test_env["test_df"].reset_index(drop=True)
    )

def test_load_overwrite_protection(s3_test_env, caplog):
    """Test overwrite protection mechanism"""
    # Initial load
    sink.load_to_s3(
        data=s3_test_env["test_df"],
        bucket=s3_test_env["bucket"],
        s3_key="existing_file.parquet"
    )
    
    # Attempt overwrite without permission
    with pytest.raises(FileExistsError):
        sink.load_to_s3(
            data=s3_test_env["test_df"],
            bucket=s3_test_env["bucket"],
            s3_key="existing_file.parquet",
            overwrite=False
        )
    
    # Verify no duplicate writes
    s3 = boto3.resource('s3')
    versions = s3.Bucket(s3_test_env["bucket"]).object_versions.filter(
        Prefix="existing_file.parquet"
    )
    assert len(list(versions)) == 1

@pytest.mark.integration
def test_production_s3_load():
    """Integration test with real S3 (run with pytest -m integration)"""
    test_df = pd.DataFrame({"test": [1, 2, 3]})
    
    result = sink.load_to_s3(
        data=test_df,
        bucket="production-bucket",
        s3_key="integration_test.parquet",
        overwrite=True
    )
    
    assert "s3://production-bucket/integration_test.parquet" == result

if __name__ == "__main__":
    pytest.main(["-v", __file__])