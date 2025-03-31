#!/usr/bin/env python

import os
import gc
import shutil
from pathlib import Path
from typing import Optional
from prefect import get_run_logger

import contextlib
import numpy as np
import pandas as pd
import netCDF4 as nc


def transform_file(
    extract_file: Path,
    transform_folder: Path,
    filename: str,
    chunk_size: Optional[int] = None
) -> Optional[Path]:
    """
    Transform GOES netCDF files into structured CSV with memory optimization
    
    Args:
        extract_file: Path to input NetCDF file
        transform_folder: Output directory for transformed CSV
        filename: Base name for output files
        chunk_size: Number of rows to process at once (None for single pass)
    
    Returns:
        Path to created CSV file or None if failed
    """
    logger = get_run_logger()
    # Validate inputs
    if not extract_file.exists():
        logger.error(f"Input file not found: {extract_file}")
        return None

    transform_folder.mkdir(parents=True, exist_ok=True)
    output_path = transform_folder / f"{filename}.event.csv"
    temp_path = output_path.with_suffix(".tmp")

    try:
        with contextlib.ExitStack() as stack:
            # Open NetCDF file with context manager
            ds = stack.enter_context(nc.Dataset(extract_file, 'r'))
            
            # Validate required variables
            required_vars = {'event_lat', 'event_lon', 'event_time_offset', 'event_energy'}
            missing_vars = required_vars - set(ds.variables)
            if missing_vars:
                logger.error(f"Missing required variables: {missing_vars}")
                return None

            # Access variables once
            time_var = ds.variables['event_time_offset']
            lat = ds.variables['event_lat'][:]
            lon = ds.variables['event_lon'][:]
            energy = ds.variables['event_energy'][:]

            # Convert time using vectorized operations
            dtime = nc.num2date(time_var[:], time_var.units, only_use_cftime_datetimes=False)
            timestamps = np.array([pd.Timestamp(t) for t in dtime])

            # Create DataFrame in chunks if specified
            def process_chunk(start, end):
                return pd.DataFrame({
                    'timestamp': timestamps[start:end],
                    'latitude': lat[start:end],
                    'longitude': lon[start:end],
                    'energy': energy[start:end]
                })

            # Write header once
            header = "timestamp,latitude,longitude,energy\n"
            with open(temp_path, 'w') as f:
                f.write(header)

            # Process data in chunks
            total_rows = len(timestamps)
            chunk_size = chunk_size or total_rows
            
            for start in range(0, total_rows, chunk_size):
                end = min(start + chunk_size, total_rows)
                chunk_df = process_chunk(start, end)
                
                # Append chunk to file
                chunk_df.to_csv(
                    temp_path,
                    mode='a',
                    header=False,
                    index=False,
                    date_format='%Y-%m-%d %H:%M:%S.%f'
                )
                
                # Explicit memory cleanup for chunk
                del chunk_df
                gc.collect()

        # Atomic move when complete
        shutil.move(temp_path, output_path)
        logger.info(f"Successfully processed: {output_path}")
        return output_path

    except (IOError, OSError, RuntimeError) as e:
        logger.error(f"Processing failed for {extract_file}: {str(e)}")
        if temp_path.exists():
            temp_path.unlink()
        return None
    finally:
        # Cleanup NetCDF references
        if 'ds' in locals():
            ds.close()
        gc.collect()