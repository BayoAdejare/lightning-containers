import os

import pandas as pd

from tasks.etl import source, transformations, sink


def ingestion_config():
    # Optional ETL parameters:
    year = os.getenv("GOES_YEAR")
    day_of_year = os.getenv("GOES_DOY")
    hour = os.getenv("GOES_HOUR")
    # Required parameters:
    bucket_name = os.getenv("S3_BUCKET")  # Satellite i.e. GOES-18
    product_line = os.getenv("PRODUCT")  # Product line id i.e. ABI...
    prefix = f"{product_line}/{year}/{day_of_year}/{hour}/"

    return prefix, bucket_name


def ingestion(start_date: str, end_date: str, hours: [str]):
    """Collects the data"""

    for single_date in pd.date_range(start_date, end_date, freq="D"):
        for single_hour in hours:
            single_hour = single_hour.rjust(2, "0")
            print(
                f"Start date: {start_date}; End date: {end_date}; Hour: {single_hour}"
            )
            # Export env vars
            os.environ["GOES_YEAR"] = single_date.strftime("%Y")
            os.environ["GOES_DOY"] = single_date.strftime("%j")
            os.environ["GOES_HOUR"] = str(single_hour)
            # config file string
            prefix, bucket_name = ingestion_config()
            print(f"Prefix: {prefix}; Bucket: {bucket_name}")
            try:
                # ETL tasks
                s = source()
                t = transformations(s)
                s = sink(t)
            except Exception as e:
                print(f"Error loading files from {prefix}")
