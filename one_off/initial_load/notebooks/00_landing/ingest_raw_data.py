# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Raw Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Taxi Trips

# COMMAND ----------

import os
import urllib.request
from string import Template 

# COMMAND ----------

landing_volume = "/Volumes/nyctaxi/landing/data_sources"
taxi_trips_url_template = Template("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-$month.parquet")

# COMMAND ----------

def ingest_taxi_trips(url_template: Template, year: str, month_range):
    for month in month_range:
        formatted_month = f"{month:02d}"
        month_url = url_template.substitute(**{'year': year, 'month': formatted_month })

        ## load the web resource && save to fs
        month_out_dir = f"{landing_volume}/nyctaxi_yellow/{year}-{formatted_month}"
        month_out_file = f"yellow_tripdata_{year}-{formatted_month}.parquet"

        os.makedirs(month_out_dir, exist_ok=True)
        month_out_path = f"{month_out_dir}/{month_out_file}"

        with urllib.request.urlopen(month_url) as response:
            data = response.read()
            print(f"Downloaded {month_url}")

            with open(month_out_path, 'wb') as f:
                f.write(data)
                print(f"Saved {month_out_path}")

ingest_taxi_trips(taxi_trips_url_template, '2025', range(5,11))

        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taxi Zone Lookup 

# COMMAND ----------

taxi_zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

lookup_out_dir = f"{landing_volume}/lookup"
lookup_out_filepath = f"{lookup_out_dir}/taxi_zone_lookup.csv" 
os.makedirs(lookup_out_dir, exist_ok=True)

# load the web resource and save to fs
with urllib.request.urlopen(taxi_zone_url) as response:
    data = response.read()

    with open(f"{lookup_out_filepath}", 'wb') as f:
        f.write(data)

display(dbutils.fs.ls(f"{lookup_out_dir}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read the ingested assets

# COMMAND ----------

spark.read.format("parquet").load(f"{landing_volume}/nyctaxi_yellow/*").display()

# COMMAND ----------

spark.read.format("csv").options(header=True, inferSchema=True).load(f"{landing_volume}/lookup/taxi_zone_lookup.csv").display()