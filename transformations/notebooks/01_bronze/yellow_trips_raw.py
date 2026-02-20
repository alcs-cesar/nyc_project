# Databricks notebook source
# MAGIC %md
# MAGIC ## Read the ingested monthly data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

ingested_trips_loc = dbutils.jobs.taskValues.get("00_ingest_yellow_trips", "ingested_trips_loc")
historic_raw_trips_loc = "nyctaxi.bronze.yellow_trips_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Data

# COMMAND ----------

trips_raw_df = spark.read.format("parquet").load(ingested_trips_loc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic and Output Data

# COMMAND ----------

trips_raw_df.withColumn("processed_timestamp", current_timestamp()) \
  .write.mode("append").saveAsTable(historic_raw_trips_loc)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Task Values

# COMMAND ----------

dbutils.jobs.taskValues.set("trips_raw_loc", historic_raw_trips_loc )