# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading enriched trips

# COMMAND ----------

from pyspark.sql.functions import to_date, col, lit, to_timestamp, count, avg, max, min, sum, round

# COMMAND ----------

enriched_trips_table = "nyctaxi.silver.yellow_trips_enriched"
enriched_trips = spark.read.table(enriched_trips_table)
enriched_trips.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping by picku date

# COMMAND ----------

daily_sum = \
    enriched_trips\
        .groupBy(col("tpep_pickup_datetime").cast("date").alias("pickup_date"))\
        .agg(
            count("*").alias("total_trips"),
            round(avg("passenger_count"), 1).alias("avg_passenger_per_trip"),
            round(avg("trip_distance"), 1).alias("avg_distance_per_trip"),
            round(avg("fare_amount"), 2).alias("avg_fare_per_trip"),
            round(max("fare_amount"), 2).alias("max_fare"),
            round(min("fare_amount"), 2).alias("min_fare"),
            round(sum("total_amount"), 2).alias("total_revenue")
        )

daily_sum.display()

# COMMAND ----------

daily_sum_table = "nyctaxi.gold.daily_trip_summary"
daily_sum.write.mode("overwrite").saveAsTable(daily_sum_table)
spark.read.table(daily_sum_table).display()