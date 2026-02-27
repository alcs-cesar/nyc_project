from typing import List
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter

import sys
import os

################ Import App Dependencies ################

def register_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

    if project_root not in sys.path:
        sys.path.append(project_root) 

register_project_root()

from modules.io.contracts import InputTrips, SaveRawTrips

#########################################################


class ExternalTable(SaveRawTrips):
    def __init__(self, table_name: str, external_location: str, spark = None):
        self.table_name = table_name
        self.external_location = external_location
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

    def save(self, writer: DataFrameWriter): 
        writer.option("path", self.external_location).saveAsTable(self.table_name)

    def data(self):
        self.spark.catalog.createTable(
            self.table_name, self.external_location
        )


