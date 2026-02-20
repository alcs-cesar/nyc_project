# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, current_timestamp, lit, timestamp_diff

# COMMAND ----------

taxi_zone_landing_loc = "/Volumes/nyctaxi/landing/data_sources/lookup/taxi_zone_lookup.csv"

zone_raw_df = spark.read.format("csv").options(header=True, inferSchema=True).load(taxi_zone_landing_loc)

def with_formatted_cols(df: DataFrame):
  return df.select(
    df.LocationID.cast("int").alias("location_id"),
    df.Borough.alias("borough"),
    df.Zone.alias("zone"),
    df.service_zone,
    current_timestamp().alias("effective_date"),
    lit(None).cast("timestamp").alias("end_date")
  )

enriched_zone = with_formatted_cols(zone_raw_df)

enriched_zone.display()

# COMMAND ----------

enriched_zone_table = "nyctaxi.silver.taxi_zone_lookup" 
enriched_zone.write.mode("overwrite").saveAsTable(enriched_zone_table)

# COMMAND ----------

spark.read.table(enriched_zone_table).display()