from abc import ABC, abstractmethod
from typing import Callable
from functools import cached_property
from pyspark.sql import DataFrame

import sys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Component Location

# COMMAND ----------

def register_component_location(location: str):
    sys.path.append(location) 

register_component_location("/Volume/Shared/nyc_project/transformations")

from utils.app_entities import MonthsAgo
from io.contracts import InputTrips, SaveTrips
from io.unity_entities import UnityTable
from io.trips import MonthlyTrips



#############################################################
################ BUSINESS LOGIC ##################
#############################################################

# TripDuration( DetailedLocations( NoEnriched( lambda: UnityTable(table_name()) ) ) )

class Enriched(ABC):

    @abstractmethod
    def data(self) -> DataFrame:
        pass

class NoEnriched(Enriched):
    def __init__(self, raw_trips: InputTrips):
        self.raw_trips = raw_trips

    def data(self) -> DataFrame:
        return self.raw_trips.data()


class DetailedLocations(Enriched):
    def __init__(self, enriched: Enriched, zone: DataFrame):
        self.enriched = enriched
        self.zone = zone

    @cached_property
    def data(self) -> DataFrame:
        pu = self.zone.alias('pu')
        do = self.zone.alias('do')
        trips = self.enriched.data()

        location_mapping = {
            'pu_location_id': [ pu['borough'].alias('pu_borough'), do['borough' ].alias('do_borough') ],
            'do_location_id': [ pu['zone'].alias('pu_zone'), do['zone'].alias('do_zone') ]
        }

        # column order
        sorted_cols = [
            col_val
            for col in trips.columns
            for col_val in ([trips[col]] if col not in location_mapping.keys() else location_mapping[col])
        ]

        with_locations = trips\
            .join(pu, trips['pu_location_id'] == pu['location_id'], "left")\
            .join(do, trips['do_location_id'] == do['location_id'], "left")\
            .select(*sorted_cols)
        
        return with_locations

class TripDuration(Enriched):
    def __init__(self, enriched: Enriched):
        self.enriched = enriched

    @cached_property
    def data(self):
        trips = self.enriched.data()

        trip_duration = \
            timestamp_diff('minute', trips['tpep_pickup_datetime'], trips['tpep_dropoff_datetime'])\
            .alias("trip_duration_mins")

        # column order
        sorted_cols = [
            col_val
            for col in trips.columns
            for col_val in ([trips[col]] if col != 'tpep_dropoff_datetime' else [trips[col], trip_duration])
        ]

        return trips.select(*sorted_cols)
