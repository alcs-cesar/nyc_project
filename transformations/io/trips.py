from typing import Callable
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, add_months, lit

################ Import App Dependencies ################

def register_component_location(location: str):
    sys.path.append(location) 

register_component_location("/Volume/Shared/nyc_project/transformations")

from io.contracts import InputTrips
from io.unity_entities import UnityTable
from utils.app_entities import MonthsAgo
from utils.columns import DateAsColumn

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
        return cls(
            trips = UnityTable(trips_table),
            processing_date = DateAsColumn(MonthsAgo.from_current_month_first_day()).column()
        )


def _month_first_day(date: datetime | date) -> Column:
    return lit(f"{date.year}-{date.month:02d}-01").cast("date")
