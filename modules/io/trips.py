from typing import Callable
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, add_months, lit
from datetime import date, datetime
import sys
import os

################ Import App Dependencies ################

def register_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

    if project_root not in sys.path:
        sys.path.append(project_root) 

register_project_root()

from modules.io.contracts import InputTrips
from modules.io.unity_entities import UnityTable
from modules.utils.app_entities import MonthsAgo
from modules.utils.columns import DateAsColumn

#########################################################

### FilteredTrips(trips, predicate)


class MonthlyTrips(InputTrips):
    def __init__(self, trips: InputTrips, processing_date: Column):
        self.trips = trips
        self.processing_date = processing_date

    def data(self) -> DataFrame: 
        monthly_predicate = \
            (col("tpep_pickup_datetime").cast("date") >= self.processing_date) & \
            (col("tpep_dropoff_datetime").cast("date") < add_months(self.processing_date, 1))

        return self.trips.data().filter(monthly_predicate)

    @classmethod 
    def from_table_and_date(cls, trips_table: str, date: datetime | date = None):
        months_ago =  MonthsAgo.from_current_month_first_day() if date is None else date

        return cls(
            trips = UnityTable(trips_table),
            processing_date = DateAsColumn(months_ago).column()
        )
