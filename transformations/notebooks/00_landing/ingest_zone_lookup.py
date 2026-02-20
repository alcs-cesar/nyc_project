# Databricks notebook source
import os
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod  # Added import statements
from collections.abc import Callable
from pathlib import Path
import urllib.request

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

zone_lookup_url = dbutils.widgets.get("taxi_zone_url")
zone_out_dir = dbutils.widgets.get("zone_out_dir")
zone_filename = dbutils.widgets.get("zone_out_filename")

# COMMAND ----------

zone_lookup_path = f"{zone_out_dir}/{zone_filename}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Data

# COMMAND ----------

def zone_resource(zone_url) -> bytes:
    with urllib.request.urlopen(zone_url) as response:
        return response.read()
    


# COMMAND ----------

# MAGIC %md
# MAGIC #### EO Design

# COMMAND ----------

class ZoneWebResource:
    def __init__(self, zone_url):
        self.zone_url = zone_url

    def bytes(self) -> bytes:
        with urllib.request.urlopen(self.zone_url) as response:
            return response.read()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Data

# COMMAND ----------

def save_zone(zone_bytes: bytes, zone_location: str):
    with open(zone_location, "wb") as f:
        f.write(zone_bytes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EO Design

# COMMAND ----------

class PersistableLocation:
    def __init__(self, qualified_location: str):
        self._location = qualified_location

    def setup(self):
        # create dir
        os.path.dirname(self._location).mkdir(parents=True, exist_ok=True)

    def location(self) -> str:
        return self._location

    def __call__(self):
        self.setup()
        return self
    
    @classmethod
    def from_dir_and_filename(cls, dir: str, filename: str):
        return cls(f"{dir}/{filename}")

# COMMAND ----------

class ZoneBinary:
    def __init__(self, zone_bytes: Callable[[], bytes]):
        self.zone_bytes = zone_bytes

    def save(self, persistable_loc: PersistableLocation):
        with open(persistable_loc().location(), "wb") as f:
            f.write(self.zone_bytes())



# COMMAND ----------

# ZoneBinary(lambda: zone_resource(zone_lookup_url)) \
#    .save( PersistableLocation.from_dir_and_filename(landing_base_path, zone_filename) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Steps

# COMMAND ----------

def ingest_zone_lookup(zone_url, zone_lookup_path):
    try:
        zone_bytes = zone_resource(zone_url)
        save_zone(zone_bytes, zone_lookup_path)
        return "yes"
    except Exception as e:
        print(f"Error: {e}")
        return "no"


continues_downstream = ingest_zone_lookup(zone_lookup_url, zone_lookup_path)
dbutils.jobs.taskValues.set("continue_downstream", continues_downstream)
