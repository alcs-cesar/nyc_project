# Databricks notebook source
from string import Template
from typing import Optional, Callable
from abc import ABC, abstractmethod
import urllib.request, os
import sys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Component Location

# COMMAND ----------

def register_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

    if project_root not in sys.path:
        sys.path.append(project_root) 

register_project_root()

from modules.utils.app_entities import MonthsAgo


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# One Time Inputs
trips_base_path = dbutils.widgets.get("trips_out_base_dir")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Data (Parameters + RemoteResource(resource_location))
# MAGIC Parameters(DynamicParams, StaticParams)

# COMMAND ----------

monthly_templates = {
    'trips_in_url': dbutils.widgets.get("trips_in_url"),
    'trips_out_filename': dbutils.widgets.get("trips_out_filename"),
    'trips_out_dir': dbutils.widgets.get("trips_out_dir")
}

def monthly_param(template_param, year, month):
    return Template(template_param).substitute(year=year, month=f"{month:02d}") 

def monthly_params(year, month):
    params = {
        key: monthly_param(value, year, month)
        for key, value in monthly_templates.items()
    }

    params['trips_out_filepath'] = f"{params['trips_out_dir']}/{params['trips_out_filename']}"

    print(f"momthly params: {params}")
    return params

# COMMAND ----------

def trips_resource(url_trips: str) -> bytes:
    with urllib.request.urlopen(url_trips) as response:
        return response.read()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Data -> BinaryTrips(trips).save(output_loc)

# COMMAND ----------

def save_monthly_trips(trips, m_params):
    # Create monthly directory
    monthly_dir = m_params['trips_out_dir'] 
    os.makedirs(monthly_dir, exist_ok=True)
    # Save trips data
    trips_filepath = f"{monthly_dir}/{m_params['trips_out_filename']}"
    with open(trips_filepath, "wb") as file:
        file.write(trips)



# COMMAND ----------

class QualifiedLocation:
    def __init__(self, abs_dir: str, filename: str):
        self.abs_dir = abs_dir
        self.filename = filename

    def location(self) -> str:
        return f"{self.abs_dir}/{self.filename}"

    def __str__(self):
        return self.location()

    def __call__(self):
        return self.location()
    
    @classmethod
    def from_dict(cls, params: dict):
        return cls(params['trips_out_dir'], params['trips_out_filename'])

class SaveTrips:
    def __init__(self, save_loc: QualifiedLocation):    
        self.save_loc = save_loc
        self.successful = False

    def save(self, trips):
        try:
            with open(self.save_loc(), "wb") as file:
                file.write(trips)

            self.successful = True
        except Exception as e:
            print(f"Error saving trips: {e}")
            self.successful = False
    
    def success(self):
        return self.success

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic

# COMMAND ----------

def already_ingested(filepath):
    try:
        dbutils.fs.ls(filepath)
        return True
    except Exception as e:
        return False

# load and save the trips data
def ingest_monthly_trips(m_params):
    if already_ingested(m_params['trips_out_filepath']):
        print('File already ingested')
        return "no"
    
    try:
        # Load Trips
        trips = trips_resource(m_params['trips_in_url'])
        print(f"Downloaded trips from {m_params['trips_in_url']}")

        # Save Trips
        save_monthly_trips(trips, m_params)
        print('File successfully uploaded in the current run')
        return "yes"
    except Exception as e:
        print(f"Error loading trips: {e}")
        return "no"


# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Steps

# COMMAND ----------
processing_date = MonthsAgo.from_today().date()
print(f"processing_date: {processing_date}")
_monthly_params = monthly_params(processing_date.year, processing_date.month)
continue_downstream = ingest_monthly_trips(_monthly_params)

# Next Task Params
dbutils.jobs.taskValues.set("continue_downstream", continue_downstream)
dbutils.jobs.taskValues.set("ingested_trips_loc", _monthly_params['trips_out_filepath'])