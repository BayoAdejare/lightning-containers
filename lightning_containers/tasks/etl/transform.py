#!/usr/bin/env python

import os
import shutil

import netCDF4 as nc
import pandas as pd

from pathlib import Path


def transform_file(
    extract_folder: str, transform_folder: str, filename: str
) -> pd.DataFrame:
    """
    Convert GOES netCDF files into csv
    """
    file_conn = Path(os.path.join(extract_folder, filename))
    event_file = file_conn.with_suffix("").with_suffix(
        ".event.csv"
    )  # radiant event file
    # create dataset
    glm = nc.Dataset(file_conn, mode="r")
    # variable definition
    event_lat = glm.variables["event_lat"][:]
    event_lon = glm.variables["event_lon"][:]
    event_time = glm.variables["event_time_offset"]
    event_energy = glm.variables["event_energy"][:]
    dtime = nc.num2date(event_time[:], event_time.units)
    # flatten multi-dimensional data into series
    event_energy_ts = pd.Series(event_energy, index=dtime)
    event_lat_ts = pd.Series(event_lat, index=dtime)
    event_lon_ts = pd.Series(event_lon, index=dtime)
    # combine series to dataframe
    event_df = pd.concat(
        [event_lat_ts, event_lon_ts, event_energy_ts], axis=1
    ).reset_index()
    # headers
    with open(event_file, "w") as glm_file:
        glm_file.write("id,ts_date,latitude,longitude,energy\n")
    # write to csv
    event_df.to_csv(event_file, index=True, header=False, mode="a")
    # move files
    shutil.move(event_file, transform_folder)
    # list converted files
    df_transform = pd.DataFrame(os.listdir())
    return df_transform
