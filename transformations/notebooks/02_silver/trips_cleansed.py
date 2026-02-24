# Databricks notebook source
# MAGIC %md
# MAGIC # Cleansed Trips

# COMMAND ----------

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import lit, add_months
from abc import ABC, abstractmethod
from typing import Callable
from pyspark.sql.functions import when, col

import sys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Component Location

# COMMAND ----------

def register_component_location(location: str):
    sys.path.append(location) 

register_component_location("/Volume/Shared/nyc_project/transformations")

from utils.app_entities import MonthsAgo
from io.contracts import InputTrips 
from io.trips import MonthlyTrips

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

trips_raw_table = "nyctaxi.bronze.yellow_trips_raw"
cleansed_trips_table = "nyctaxi.silver.yellow_trips_cleansed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Data



# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic

# COMMAND ----------

def mapping_columns(df: DataFrame):
    vendor_col = \
        when(df["VendorID"] == 1, "Creative Mobile Technologies, LLC").\
        when(df["VendorID"] == 2, "Curb Mobility, LLC").\
        when(df["VendorID"] == 6, "Myle Technologies Inc").\
        when(df["VendorID"] == 7, "Helix").\
        otherwise("Unknown").alias("vendor")

    rate_type_col = \
        when(df["RatecodeID"] == 1, "Standard Rate").\
        when(df["RatecodeID"] == 2, "JFK").\
        when(df["RatecodeID"] == 3, "Newark").\
        when(df["RatecodeID"] == 4, "Nassau or Westchester").\
        when(df["RatecodeID"] == 5, "Negotiated fare").\
        when(df["RatecodeID"] == 6, "Group ride").\
        otherwise("Unknown").alias("rate_type")

    payment_type_col = \
        when(df["payment_type"] == 0, "Flex Fare trip").\
        when(df["payment_type"] == 1, "Credit Card").\
        when(df["payment_type"] == 2, "Cash").\
        when(df["payment_type"] == 3, "No charge").\
        when(df["payment_type"] == 4, "Dispute").\
        when(df["payment_type"] == 5, "Unknown").\
        when(df["payment_type"] == 6, "Voided trip").\
        otherwise("Unknown").alias("payment_type")

    return {
        'VendorID': vendor_col,
        'RatecodeID': rate_type_col,
        'payment_type': payment_type_col,
        'PULocationID': df['PULocationID'].alias('pu_location_id'),
        'DOLocationID': df['DOLocationID'].alias('do_location_id'),
        'Airport_fee': df['Airport_fee'].alias('airport_fee')
    }

def with_cleansed_cols(df: DataFrame):
    cleansed_cols = mapping_columns(df)

    sorted_cols = [
        cleansed_cols[col] if col in cleansed_cols.keys() else df[col]
        for col in df.columns
        ]
    
    return df.select(*sorted_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Data

# COMMAND ----------

def save_cleansed_trips(df: DataFrame, cleansed_loc: str):
    df.write.mode("append").saveAsTable(cleansed_loc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Steps

# COMMAND ----------

def cleansed_trips():
  raw_trips = MonthlyTrips.from_table_and_date(
    trips_raw_table,
    MonthsAgo.from_current_month_first_day().date(), 
    ).data()

  cleansed_trips = with_cleansed_cols(raw_trips)
  save_cleansed_trips(cleansed_trips, cleansed_trips_table)

cleansed_trips()