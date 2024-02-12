import os
import shutil
import warnings

from prefect import task
from datetime import datetime, timedelta
from .extract import extract_s3
from .transform import transform_file
from .load import load_tbl

from botocore import UNSIGNED, exceptions
from botocore.client import Config
from boto3 import client
from pathlib import Path
from tqdm import tqdm

warnings.simplefilter("ignore")


def etl_config(process: str):
    # optional ETL parameters:
    dt = datetime.utcnow() - timedelta(hours=1)
    year = os.getenv("GOES_YEAR", dt.strftime("%Y"))
    day_of_year = os.getenv("GOES_DOY", dt.strftime("%j"))
    hour = os.getenv("GOES_HOUR", dt.strftime("%H"))
    # required parameters:
    bucket_name = os.getenv("S3_BUCKET", "noaa-goes18")  # Satellite i.e. GOES-18
    product_line = os.getenv("PRODUCT", "GLM-L2-LCFA")  # Product line id i.e. ABI...
    prefix = f"{product_line}/{year}/{day_of_year}/{hour}/"
    basepath = Path(__file__).resolve().parent.parent.parent.parent
    ext_path = Path("data/Extract")
    trf_path = Path("data/transform")
    lod_path = Path("data/Load")
    if process == "extract":
        src_folder = prefix
        dest_folder = os.path.join(basepath, ext_path)
        # create folder if not existing
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
    elif process == "transform":
        src_folder = os.path.join(basepath, ext_path)
        dest_folder = os.path.join(basepath, trf_path)
        # create folder if not existing
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
    elif process == "load":
        src_folder = os.path.join(basepath, trf_path)
        dest_folder = os.path.join(basepath, lod_path)
        # create folder if not existing
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
    else:
        print(f"Process {process} not found!")

    return src_folder, bucket_name, dest_folder


@task(
    name="Source extract",
    description="Extract GOES netCDF files from s3 bucket.",
    retries=2,
    retry_delay_seconds=3,
)
def source():
    # config file string
    prefix, bucket_name, extract_folder = etl_config(process="extract")
    # navigate to folder
    os.chdir(extract_folder)
    results = []
    print(f"Starting file extracts for: {prefix}")
    # configure s3 no sign in credential
    s3 = client("s3", config=Config(signature_version=UNSIGNED))
    # list existing files in buckets
    for files in tqdm(
        s3.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"],
        total=180,
        ascii=" >=",
        desc=f"Extract {prefix}",
    ):
        # download file from list
        filepath = files["Key"]
        path, filename = os.path.split(filepath)
        print(f"Dowloading {filename} to {os.path.join(extract_folder, filename)}")
        s3_extract = extract_s3(bucket_name, prefix, filename, filepath)
        results.append(s3_extract)
    # context.add_output_metadata({s3_extract -> results})
    return results


@task(
    name="Transform data",
    description="Convert GOES netCDF files into csv files.",
    retries=2,
    retry_delay_seconds=3,
)
def transformations(source):
    # config file string
    extract_folder, bucket_name, transform_folder = etl_config(process="transform")
    glm_files = [f for f in os.listdir(extract_folder) if f.endswith(".nc")]
    # exit if source folder not existing
    if not os.path.exists(extract_folder):
        pass
    # empty sink folder or recreate
    if not os.path.exists(transform_folder):
        os.makedirs(transform_folder)
    else:
        filelist = [f for f in os.listdir(transform_folder) if f.endswith(".csv")]
        for f in filelist:
            os.remove(os.path.join(transform_folder, f))
    results = []
    print(f"Starting file conversions for: {extract_folder}")
    # Convert glm files into one time series dataframe
    for filename in tqdm(glm_files, desc=f"transform {extract_folder}"):
        print(f"Converting {filename} to csv")
        csv_transform = transform_file(extract_folder, transform_folder, filename)
        results.append(csv_transform)
    # csv_transform -> results
    return results


@task(
    name="Sink load",
    description="Load GOES csv files into storage.",
    retries=2,
    retry_delay_seconds=3,
)
def sink(transformations):
    # config file string
    transform_folder, bucket_name, load_folder = etl_config(process="load")
    glm_files = [f for f in os.listdir(transform_folder) if f.endswith(".csv")]
    print(f"Starting files load for: {transform_folder}")
    # navigate to folder
    os.chdir(transform_folder)
    for filename in glm_files:
        try:
            # copy to folder
            shutil.copy(filename, load_folder)
        # if source and sink are same
        except shutil.SameFileError:
            print("Source and sink represents the same file.")
        # if there is any permission issue
        except PermissionError:
            print("Permission denied.")
        # for other errors
        except:
            print(f"Error copying {filename} to {load_folder}.")
        try:
            # rename files
            os.rename(filename, f"{filename}.trm")
        # file already exists
        except FileExistsError:
            print(f"Error renaming {filename}; file exists.")
    results = []
    print(f"Load {load_folder} files to sink.")
    # navigate to folder
    os.chdir(load_folder)
    db_load = load_tbl(load_folder)
    results.append(db_load)
    return results
