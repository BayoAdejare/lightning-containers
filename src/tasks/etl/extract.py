#!/usr/bin/env python

import os
from typing import List, Dict, Any, Union

import pandas as pd
from prefect import get_run_logger

from botocore.client import Config
from botocore import UNSIGNED, exceptions
from boto3 import client


def extract_s3(bucket: str, prefix: str, filename: str, filepath: str) -> bool:
    """
    Downloads GOES netCDF files from s3 buckets
    
    Args:
        bucket (str): The S3 bucket name
        prefix (str): The directory prefix in S3
        filename (str): The filename to save locally
        filepath (str): Full path in S3 including filename
        
    Returns:
        bool: True if download successful, False otherwise
        
    Note:
        The expected S3 path format is:
        s3://<weather_satellite>/<product_line>/<year>/<day_of_year>/<hour>/<OR_...*.nc>
    """
    logger = get_run_logger()
    s3 = client("s3", config=Config(signature_version=UNSIGNED))
    
    try:
        logger.info(f"Downloading {filename} from {bucket}/{filepath}")
        s3.download_file(Bucket=bucket, Filename=filename, Key=filepath)
        
        # Check if file was actually downloaded
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            logger.info(f"Successfully downloaded {filename}")
            return True
        else:
            logger.warning(f"File {filename} downloaded but appears empty")
            return False
            
    except exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            logger.error(f"{filename} cannot be located in {bucket}/{filepath}")
        else:
            logger.error(f"Error downloading {filename}: {str(err)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading {filename}: {str(e)}")
        return False