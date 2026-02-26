# Databricks notebook source
from typing import Callable
from abc import ABC, abstractmethod
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, timestamp_diff, make_date, lit, add_months
import os
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

from modules.layers.enriched_operations import Enriched, TripDuration, DetailedLocations, NoEnriched
from modules.io.contracts import InputTrips, SaveTrips
from modules.io.trips import MonthlyTrips
from modules.io.unity_entities import UnityTable

#############################################################
################ COMPOSABLE ENRICHED OPERATIONS #############
#############################################################

class EnrichedTrips(Enriched):
    def __init__(self, enriched: Enriched):
        self.enriched = enriched

    def data(self) -> DataFrame:
        return self.enriched.data()

    def save(self, save_trips: SaveTrips):
        save_trips.save(self.data())

    @classmethod
    def from_trip_duration_and_zones(cls, raw_trips: InputTrips, taxi_zones: DataFrame):
        return cls(
            TripDuration(
                DetailedLocations( 
                    NoEnriched(raw_trips), taxi_zones 
                )
            )
        )


#############################################################
################ MODULE CONFIGURATION ##################
#############################################################

# class EnrichedModule (declarative setup & method to trigger execution)
# - EnrichedTrips & SaveTrips
# I/O Parameters
trips_cleansed_table = "nyctaxi.silver.yellow_trips_cleansed"
zone_table = "nyctaxi.silver.taxi_zone_lookup"
trips_enriched_table = "nyctaxi.silver.yellow_trips_enriched"


class EnrichedModule:
    def __init__(self, enriched_trips: Enriched, save_trips: SaveTrips):
        self.enriched_trips = enriched_trips
        self.save_trips = save_trips

    def run(self):
        self.enriched_trips.save(self.save_trips)

    @classmethod
    def from_io_tables(cls, cleansed_table: str, zone_table: str, enriched_table: str):
        monthly_raw_trips = MonthlyTrips.from_table_and_date(cleansed_table) 
        zone_table = UnityTable(zone_table)

        return cls(
            enriched_trips = EnrichedTrips.from_trip_duration_and_zones(monthly_raw_trips, zone_table.data()),
            save_trips = UnityTable(enriched_table)
        )

EnrichedModule.from_io_tables(trips_cleansed_table, zone_table, trips_enriched_table).run()
