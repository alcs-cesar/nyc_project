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

from modules.io.contracts import InputTrips
from modules.io.configurated_save import ConfiguratedSave

#########################################################


class ExternalTable:
    def __init__(self, table_name: str, external_location: str):
        self.table_name = table_name
        self.external_location = external_location

    def save(self, writer: Callable[[], DataFrameWriter]): 
        writer().saveAsTable(self.table_name)

    def data(self):
        self.spark.catalog.createTable(
            self.table_name, self.external_location
        )


