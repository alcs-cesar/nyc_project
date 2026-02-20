# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, current_timestamp, lit, timestamp_diff

# COMMAND ----------

cleansed_df = spark.read.table("nyctaxi.silver.yellow_trips_cleansed") 
enriched_zone = spark.read.table("nyctaxi.silver.taxi_zone_lookup")

# COMMAND ----------

def with_enriched_cols(trips_df: DataFrame, enriched_zone: DataFrame):
    trip_duration_col = timestamp_diff('minute', trips_df.tpep_pickup_datetime, trips_df.tpep_dropoff_datetime).alias("trip_duration_mins")

    trips = trips_df.alias("t")
    pu_df = enriched_zone.alias("pu")
    do_df = enriched_zone.alias("do")

    location_mapped_cols = {
        'pu_location_id': [ pu_df.borough.alias("pu_borough"), do_df.borough.alias("do_borough") ],
        'do_location_id': [ pu_df.zone.alias("pu_zone"), do_df.zone.alias("do_zone") ]
    }

    pu_do_trips = trips.join(pu_df, trips.pu_location_id == pu_df.location_id, "left")\
        .join(do_df, trips.do_location_id == do_df.location_id, "left")

#    flatten_location_cols = lambda col_list: 

    result_cols = [
        col_val
        for col in trips_df.columns
        for col_val in (location_mapped_cols[col] if col in location_mapped_cols.keys() else [trips_df[col]])
        ]
    
    result_cols.insert(3, trip_duration_col)

    return pu_do_trips.select(*result_cols)

# COMMAND ----------

enriched_trips = with_enriched_cols(cleansed_df, enriched_zone)
enriched_trips.display()

# COMMAND ----------

enriched_trips_table = "nyctaxi.silver.yellow_trips_enriched"
enriched_trips.write.mode("overwrite").saveAsTable(enriched_trips_table)
spark.read.table(enriched_trips_table).display()