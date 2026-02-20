# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load all monthly trips in one dataframe

# COMMAND ----------

trips_base_loc = "/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/*"

trips_raw_df = spark.read.format("parquet").load(trips_base_loc)
trips_raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Ingested Data

# COMMAND ----------

with_ts = trips_raw_df.withColumn("processed_timestamp", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Raw Data

# COMMAND ----------

cleaned_trips_table = "nyctaxi.bronze.yellow_trips_raw"
with_ts.write.mode("overwrite").saveAsTable(cleaned_trips_table)

# COMMAND ----------

spark.read.table(cleaned_trips_table).display()