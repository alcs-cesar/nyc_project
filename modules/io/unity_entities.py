# from databricks.sdk.runtime import *
from typing import Callable
from functools import cached_property
from pyspark.sql import DataFrame, Column, SparkSession, DataFrameWriter
from pyspark.sql.functions import col, add_months, lit
import sys, os


################ Import App Dependencies ################

def register_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

    if project_root not in sys.path:
        sys.path.append(project_root) 

register_project_root()

from modules.io.contracts import InputTrips, SaveRawTrips

#########################################################

################ RESOURCES ##############################

class UnityTable(InputTrips, SaveRawTrips):
    def __init__(self, table_name: str, spark: SparkSession = None):
        self.table_name = table_name
        self.spark = SparkSession.builder.getOrCreate()

    def data(self) -> DataFrame:
        return self.spark.read.table(self.table_name)

    def save(self, trips: DataFrameWriter):
        trips.mode("append").saveAsTable(self.table_name)

class SchemedCsv(InputTrips, SaveRawTrips):
    def __init__(self, qualified_name: str, spark: SparkSession = None):
        self.qualified_name = qualified_name
        self.spark = SparkSession.builder.getOrCreate()

    def data(self) -> DataFrame:
        return self.spark.read.format("csv").options(header= True, inferSchema = True).load(self.qualified_name)

    def save(self, trips: DataFrameWriter):
        trips.write.mode("append").format("csv").save(self.qualified_name)


class ParquetFile(InputTrips, SaveRawTrips):
    def __init__(self, qualified_name: str):
        self.qualified_name = qualified_name

    @cached_property
    def data(self) -> DataFrame:
        return spark.read.format("parquet").load(self.qualified_name)

    def save(self, trips: DataFrameWriter):
        trips.write.mode("append").format("parquet").save(self.qualified_name)