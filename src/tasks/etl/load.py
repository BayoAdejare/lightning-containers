#!/usr/bin/env python
import logging
from pathlib import Path
import sqlite3 as db
from typing import Optional
import pandas as pd
import shutil

from prefect import get_run_logger


def load_tbl(load_folder: str) -> Optional[pd.DataFrame]:
    """
    Load geo data into SQLite database and manage processed files.
    
    Args:
        load_folder (str): Path to directory containing source CSV files
        
    Returns:
        pd.DataFrame: Combined DataFrame of loaded data or None if error occurs
    """
    logger = get_run_logger()
    dfs = []
    load_path = Path(load_folder)
    db_path = load_path / "glmFlash.db"
    loaded_dir = load_path / "loaded"

    try:
        # Validate environment
        if not load_path.is_dir():
            raise NotADirectoryError(f"Invalid load directory: {load_folder}")
        loaded_dir.mkdir(exist_ok=True)

        # Process files
        for filename in load_path.glob("*event.csv"):
            try:
                # 1. Column validation first
                with open(filename) as f:
                    header = f.readline().strip().lower()
                    if 'timestamp' not in header:
                        raise ValueError(f"Missing timestamp column in {filename.name}")

                # 2. Read with dtype specification
                df = pd.read_csv(
                    filename,
                    dtype={
                        'latitude': 'float32',
                        'longitude': 'float32',
                        'energy': 'float32'
                    },
                    parse_dates=['timestamp'],
                    infer_datetime_format=True
                )

                # 3. Validate timestamp conversion
                if df['timestamp'].isnull().any():
                    bad_count = df['timestamp'].isnull().sum()
                    logger.warning(f"{bad_count} invalid timestamps in {filename.name}")
                    df = df.dropna(subset=['timestamp'])

                dfs.append(df)
                _archive_file(filename, loaded_dir)

            except Exception as e:
                logger.error("Failed to process %s: %s", filename.name, str(e))
                _move_to_quarantine(filename)
                continue

        if not dfs:
            logger.warning("No valid data files processed")
            return None

        # 4. Combine and optimize data
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.convert_dtypes()

        # 5. Database operations
        with db.connect(str(db_path)) as conn:
            # Load in chunks
            full_df.to_sql(
                "tbl_flash",
                conn,
                if_exists="append",
                index=False,
                chunksize=10000
            )
            
            # Maintain database
            conn.execute("VACUUM")
            conn.execute("""
                CREATE VIEW IF NOT EXISTS vw_flash AS
                SELECT *,
                    CAST(strftime('%Y', timestamp) AS INTEGER) AS year,
                    CAST(strftime('%m', timestamp) AS INTEGER) AS month,
                    CAST(strftime('%d', timestamp) AS INTEGER) AS day,
                    null AS cluster,       
                    null AS state,
                    null AS time_period  
                FROM tbl_flash;
            """)

        return full_df

    except Exception as e:
        logger.critical("Load failure: %s", str(e), exc_info=True)
        return None

def _archive_file(src: Path, dest_dir: Path) -> None:
    """Safe file movement with versioning"""
    dest = dest_dir / src.name
    if dest.exists():
        version = 1
        while (dest.with_name(f"{src.stem}_v{version}{src.suffix}")).exists():
            version += 1
        dest = dest.with_name(f"{src.stem}_v{version}{src.suffix}")
    shutil.move(str(src), str(dest))

def _move_to_quarantine(filename: Path) -> None:
    """Handle problematic files"""
    logger = get_run_logger()
    quarantine_dir = filename.parent / "quarantine"
    quarantine_dir.mkdir(exist_ok=True)
    try:
        shutil.move(str(filename), str(quarantine_dir / filename.name))
    except Exception as e:
        logger.error("Quarantine failed for %s: %s", filename.name, str(e))