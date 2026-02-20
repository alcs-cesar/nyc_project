# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, current_timestamp, lit, timestamp_diff

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw trips

# COMMAND ----------

raw_trips_table = "nyctaxi.bronze.yellow_trips_raw"
raw_df = spark.read.table(raw_trips_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Only keep the trips in the processing interval

# COMMAND ----------

raw_df.filter("tpep_pickup_datetime < '2025-05-01'  OR tpep_dropoff_datetime >= '2025-11-01'").display()

# COMMAND ----------

trips_in_range = raw_df.filter("tpep_pickup_datetime >= '2025-05-01' AND tpep_dropoff_datetime < '2025-11-01'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleansed Trips

# COMMAND ----------

def with_mapped_columns(df: DataFrame):
  vendor_col = \
    when(df.VendorID == 1, 'Creative Mobile Technologies, LLC')\
      .when(df.VendorID == 2, 'Curb Mobility, LLC')\
      .when(df.VendorID == 6, 'Myle Technologies Inc')\
      .when(df.VendorID == 7, 'Helix')\
      .otherwise('Unknown')\
      .alias("vendor")

  rate_type_col = \
    when(df.RatecodeID == 1, 'Standard Rate')\
      .when(df.RatecodeID == 2, 'JFK')\
      .when(df.RatecodeID == 3, 'Newark')\
      .when(df.RatecodeID == 4, 'Nassau or Westchester')\
      .when(df.RatecodeID == 5, 'Negotiated Rate')\
      .when(df.RatecodeID == 6, 'Group')\
      .otherwise('Unknown')\
      .alias("rate_type")

  payment_type_col = \
    when(df.payment_type == 0, 'Flex Fare Trip')\
      .when(df.payment_type == 1, 'Credit Card')\
      .when(df.payment_type == 2, 'Cash')\
      .when(df.payment_type == 3, 'No Charge')\
      .when(df.payment_type == 4, 'Dispute')\
      .when(df.payment_type == 5, 'Unknown')\
      .when(df.payment_type == 6, 'Voided Trip')\
      .alias("payment_type")

  mapped_cols = {
    'VendorID': vendor_col,
    'RatecodeID': rate_type_col,
    'PULocationID': df.PULocationID.alias('pu_location_id'),
    'DOLocationID': df.DOLocationID.alias('do_location_id'),
    'payment_type': payment_type_col,
    'Airport_fee': df.Airport_fee.alias('airport_fee')
  }

  all_columns = [
    mapped_cols[col] if col in mapped_cols.keys() else df[col]
    for col in df.columns 
  ]

  return df.select(*all_columns)

# COMMAND ----------

#print(raw_df.columns)

cleansed_df = with_mapped_columns(trips_in_range)
cleansed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Cleansed Trips

# COMMAND ----------

cleansed_table = "nyctaxi.silver.yellow_trips_cleansed"
cleansed_df.write.mode("overwrite").saveAsTable(cleansed_table)