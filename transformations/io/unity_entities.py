from typing import Callable
from functools import cached_property
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, add_months, lit

################ Import App Dependencies ################

def register_component_location(location: str):
    sys.path.append(location) 

register_component_location("/Volume/Shared/nyc_project/transformations")

from io.contracts import InputTrips, SaveTrips

#########################################################

################ RESOURCES ##############################

class UnityTable(InputTrips, SaveTrips):
    def __init__(self, table_name: str):
        self.table_name = table_name

    @cached_property
    def data(self) -> DataFrame:
        return spark.read.table(self.table_name)

    def save(self, trips: DataFrame, mode: str = "append"):
        trips.write.mode(mode).saveAsTable(self.table_name)

class SchemedCsv(InputTrips, SaveTrips):
    def __init__(self, qualified_name: str):
        self.qualified_name = qualified_name

    @cached_property
    def data(self) -> DataFrame:
        return spark.read.format("csv").options(header= True, inferSchema = True).load(self.qualified_name)

    def save(self, trips: DataFrame, mode: str = "append"):
        trips.write.mode(mode).format("csv").save(self.qualified_name)


class ParquetFile(InputTrips, SaveTrips):
    def __init__(self, qualified_name: str):
        self.qualified_name = qualified_name

    @cached_property
    def data(self) -> DataFrame:
        return spark.read.format("parquet").load(self.qualified_name)

    def save(self, trips: DataFrame, mode: str = "append"):
        trips.write.mode(mode).format("parquet").save(self.qualified_name)