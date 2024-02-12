#!/usr/bin/env python

from lightning_streams.assets.etl import extract
import logging

# Testing fixtures
example_bucket_name = "noaa-goes18"  # Mock s3 bucket
example_prefix = "GLM-L2-LCFA/2023/048/21/"  # Mock s3 directory


def test_extract_full_sync_count():
    """
    Test extract full sync count for bucket hour.
    """
    logging.info(f"Testing file extract for: {example_prefix}")

    assert (
        extract.extract_s3(bucket=example_bucket_name, prefix=example_prefix).shape[0]
        == 180
    )
