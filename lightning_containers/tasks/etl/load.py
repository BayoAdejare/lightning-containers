#!/usr/bin/env python

import os
import shutil
import glob
import sqlite3 as db

import pandas as pd


def load_tbl(load_folder: str) -> pd.DataFrame:
    """
    Load data into sink.
    """
    print("Loading geo data!")
    os.chdir(load_folder)
    glm_files = glob.glob(os.path.join(load_folder, "*event.csv"))
    li = []
    for filename in glm_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    df["date_time"] = pd.to_datetime(df["ts_date"])
    df["date"] = df["date_time"].dt.date
    df["h_time"] = df["date_time"].dt.time

    conn = db.connect(f"{load_folder}/glmFlash.db")

    try:
        # create the table "tbl_flash" from the csv files
        df.to_sql("tbl_flash", conn, if_exists="append", index=False)
    except Exception as db_err:
        # table likely exist try insert
        print("DB error!")

    conn.execute(
        f"""
    CREATE VIEW IF NOT EXISTS vw_flash
    AS
    SELECT *, 
        CASE WHEN h_time >= 9 AND h_time <= 17 
                then 'Day'
             WHEN h_time > 17 AND h_time <= 20 THEN 'Evening'
            ELSE 'Night' -- catch all
        END AS time_period,
        9 as cluster,
        "Default" as state
    FROM tbl_flash;
    """
    )

    # conn.enable_load_extension(True)

    # try:
    #     conn.execute("SELECT load_extension('mod_spatialite');")
    # except db.OperationalError:
    #     conn.load_extension("libspatialite.so")

    conn.close()

    os.makedirs("loaded")
    try:
        for filename in glm_files:
            # Move loaded files
            shutil.move(filename, "loaded")
    except Exception as e:
        print("Exception errors received.")
    # cleanup
    shutil.rmtree("loaded")

    return df
