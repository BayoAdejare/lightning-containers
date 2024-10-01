#!/usr/bin/env python

import os

import pandas as pd

from botocore.client import Config
from botocore import UNSIGNED, exceptions
from boto3 import client


def extract_s3(bucket: str, prefix: str, filename: str, filepath: str) -> pd.DataFrame:
    """
    Downloads GOES netCDF files from s3 buckets
    prefix = s3://<weather_satellite>/<product_line>/<year>/<day_of_year>/<hour>/<OR_...*.nc>
    """
    s3 = client("s3", config=Config(signature_version=UNSIGNED))
    try:
        s3.download_file(Bucket=bucket, Filename=filename, Key=filepath)
    except exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            print(f"{filename} cannot be located.")
        else:
            raise
    # List files downloaded
    df_extract = pd.DataFrame(os.listdir())
    return df_extract
