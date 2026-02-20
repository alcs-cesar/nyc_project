# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Trip Summary

# COMMAND ----------

from typing import Callable
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, Column
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, lit, to_date, count, avg, max, min, sum, add_months

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

trips_enriched_loc = "nyctaxi.silver.yellow_trips_enriched"
daily_summary_loc = "nyctaxi.gold.daily_trip_summary"

# COMMAND ----------

def processing_date():
    return date.today().replace(day=1) - relativedelta(months=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Data

# COMMAND ----------

def load_trips_enriched(trips_enriched_loc):
  return spark.read.table(trips_enriched_loc)

def _processing_date_column(year: int, month: int) -> Column:
    return lit(f"{year}-{month:02d}-01").cast("date")


class InputTrips(ABC):

    @abstractmethod
    def dataframe(self) -> DataFrame:
        pass

    def __call__(self):
        return self.dataframe()

class MonthlyTrips(InputTrips):
    def __init__(self, all_trips: Callable[[], DataFrame], processing_date: Column):
        self.all_trips = all_trips
        self.processing_date = processing_date

    def dataframe(self) -> DataFrame: 
        monthly_predicate = \
            (col("tpep_pickup_datetime").cast("date") >= self.processing_date) & \
            (col("tpep_dropoff_datetime").cast("date") < add_months(self.processing_date, 1))

        return self.all_trips().filter(monthly_predicate)
    
    def from_table_and_date(date: date, all_trips_loc: str):
        return MonthlyTrips(
            all_trips = lambda: load_trips_enriched(all_trips_loc),
            processing_date = _processing_date_column(date.year, date.month)
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic

# COMMAND ----------

def daily_trip_summary(trips_enriched: InputTrips) -> DataFrame:
    return trips_enriched().withColumn("pickup_date", to_date("tpep_pickup_datetime"))\
        .groupBy("pickup_date")\
        .agg(
            count("*").alias("total_trips"),
            avg("passenger_count").alias("avg_passengers_per_trip"),
            avg("trip_distance").alias("avg_distance_per_trip"),
            avg("fare_amount").alias("avg_fare_per_trip"),
            max("fare_amount").alias("max_fare"),
            min("fare_amount").alias("min_fare"),
            sum("total_amount").alias("total_revenue")
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Output

# COMMAND ----------

def save_daily_summary(daily_summary: DataFrame, output_path: str):
    daily_summary.write.mode("append").saveAsTable(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Steps

# COMMAND ----------

save_daily_summary(
    daily_summary=daily_trip_summary(MonthlyTrips.from_table_and_date(processing_date(), trips_enriched_loc)),
    output_path=daily_summary_loc
)