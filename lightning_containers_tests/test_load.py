#!/usr/bin/env python

import logging
import pytest


from pyspark.testing.utils import (
    assertSchemaEqual,
    assertDataFrameEqual,
)  # pyspark 3.5.0
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType

from ..lightning_streams.assets.etl import load

# Testing fixtures


def test_load_energy_sp_schema():
    """
    Test load energy stream schema.
    """
    logging.info(f"Testing file stream schema for: energy stream")

    s1 = StructType().add("ts_date", "timestamp").add("energy", "double")
    s2 = {}

    assertSchemaEqual(s1, s2)


def test_load_geospatial_sp_dataframe():
    """
    Test load geospatial data frame.
    """
    logging.info(f"Testing file load data frame for: geospatial load")
    sd1 = {}
    sd2 = {}
    assertDataFrameEqual(sd1, sd2)
