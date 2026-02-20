# Databricks notebook source
# MAGIC %md
# MAGIC ## Which vendor makes the most revenue

# COMMAND ----------

from pyspark.sql.functions import col, sum, round, max, count

# COMMAND ----------

silver_layer = "nyctaxi.silver"

trips_enriched = spark.read.table(f"{silver_layer}.yellow_trips_enriched")

rv = trips_enriched.groupBy("vendor")\
    .agg( round(sum("total_amount"), 2).alias("revenue") )

rv.groupBy()\
    .agg(max("revenue"))\
    .display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the most popular pickup borough?

# COMMAND ----------

trips_enriched.groupBy("pu_borough")\
    .agg(count("*").alias("pick_ups"))\
    .sort("pick_ups", ascending=False)\
    .limit(5)\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the most common journey(starting_place to destination)?

# COMMAND ----------

trips_enriched.groupBy(["pu_borough", "do_borough"])\
    .agg(count("*").alias("total_trips"))\
    .sort("total_trips", ascending=False)\
    .limit(10)\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a time series chart shwoing the number of trips and total revenue per day

# COMMAND ----------

ds = spark.read.table("nyctaxi.gold.daily_trip_summary")

display(ds.select("pickup_date", "total_trips", "total_revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC