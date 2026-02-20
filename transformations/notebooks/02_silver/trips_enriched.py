# Databricks notebook source
from typing import Callable
from abc import ABC, abstractmethod
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, timestamp_diff, make_date, lit, add_months

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# One time input parameters
cleansed_location = "nyctaxi.silver.yellow_trips_cleansed"
taxi_zone_location = "nyctaxi.silver.taxi_zone_lookup"
yellow_trips_enriched_location = "nyctaxi.silver.yellow_trips_enriched"

# COMMAND ----------

## By job execution
def processing_date():
    return date.today().replace(day=1) - relativedelta(months=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Data

# COMMAND ----------

def load_cleansed_trips(cleansed_loc: str) -> DataFrame:
    return spark.read.table(cleansed_loc)

def _processing_date(year: int, month: int) -> Column:
    return lit(f"{year}-{month:02d}-01").cast("date")


class InputTrips(ABC):

    @abstractmethod
    def dataframe(self) -> DataFrame:
        pass

    def __call__(self):
        return self.dataframe()

class MonthlyTrips(InputTrips):
    def __init__(self, cleansed: Callable[[], DataFrame], processing_date: Column):
        self.cleansed = cleansed
        self.processing_date = processing_date

    def dataframe(self) -> DataFrame: 
        monthly_predicate = \
            (col("tpep_pickup_datetime").cast("date") >= self.processing_date) & \
            (col("tpep_dropoff_datetime").cast("date") < add_months(self.processing_date, 1))

        return self.cleansed().filter(monthly_predicate)
    
    def from_table_and_date(date: date, cleansed_loc: str):
        return MonthlyTrips(
            cleansed = lambda: load_cleansed_trips(cleansed_loc),
            processing_date = _processing_date(date.year, date.month)
        )

# COMMAND ----------

def load_taxi_zone(zone_loc: str) -> DataFrame:
    return spark.read.table(zone_loc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic

# COMMAND ----------

def enriched_trips(cleansed: InputTrips, zone: DataFrame) -> DataFrame:
    # with_enriched_locations
    def with_enriched_locations(cleansed: DataFrame, zone: DataFrame) -> DataFrame:
        pu = zone.alias('pu')
        do = zone.alias('do')

        location_mapping = {
            'pu_location_id': [ pu['borough'].alias('pu_borough'), do['borough' ].alias('do_borough') ],
            'do_location_id': [ pu['zone'].alias('pu_zone'), do['zone'].alias('do_zone') ]
        }

        # column order
        sorted_cols = [
            col_val
            for col in cleansed.columns
            for col_val in ([cleansed[col]] if col not in location_mapping.keys() else location_mapping[col])
        ]

        with_locations = cleansed\
            .join(pu, cleansed['pu_location_id'] == pu['location_id'], "left")\
            .join(do, cleansed['do_location_id'] == do['location_id'], "left")\
            .select(*sorted_cols)
        
        return with_locations
    
    def with_trip_duration(cleansed: DataFrame) -> DataFrame:
        trip_duration = \
            timestamp_diff('minute', cleansed['tpep_pickup_datetime'], cleansed['tpep_dropoff_datetime'])\
            .alias("trip_duration_mins")

        # column order
        sorted_cols = [
            col_val
            for col in cleansed.columns
            for col_val in ([cleansed[col]] if col != 'tpep_dropoff_datetime' else [cleansed[col], trip_duration])
        ]

        return cleansed.select(*sorted_cols)
    

    return with_trip_duration(
        with_enriched_locations(cleansed(), zone)
    )

# COMMAND ----------

def save_enriched_trips(enriched: DataFrame, enriched_loc: str):
    enriched.write.mode("append").saveAsTable(enriched_loc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Steps

# COMMAND ----------

save_enriched_trips(
    enriched =  enriched_trips(
        cleansed = MonthlyTrips.from_table_and_date(processing_date(), cleansed_location),
        zone = load_taxi_zone(taxi_zone_location)
    ),
    enriched_loc = yellow_trips_enriched_location
)