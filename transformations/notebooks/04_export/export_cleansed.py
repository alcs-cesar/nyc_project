from pyspark.sql import DataFrame
from typing import List

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

from modules.io.contracts import InputTrips, SaveRawTrips
from modules.io.external_table import ExternalTable
from modules.io.trips import MonthlyTrips
from modules.io.contracts import ConfigurableSave
from modules.utils.columns import with_year_month


# Output Parameters
external_location = "abfss://nyctaxi-yellow@nyctaxistoragemxcentral.dfs.core.windows.net/"
resource_output_dir = "nyctaxi_yellow_export/"
ext_resource_loc = external_location + resource_output_dir

export_table = "nyctaxi.export.nyctaxi_yellow_trips_export"

# Input Parameters
enriched_table = "nyctaxi.silver.yellow_trips_enriched"


class ExportCleansed:
    def __init__(self, save_trips: ConfigurableSave ):
        self.save_trips = save_trips

    def export(self):
        self.save_trips.save()


    @classmethod    
    def from_configurated_save(cls, trips: InputTrips, raw_save: SaveRawTrips):
        final_trips = \
            lambda: with_year_month(trips.data(), "tpep_pickup_timestamp", "YYYY-MM", "year_month")

        return cls(
            ConfigurableSave(
                raw_save = raw_save,
                writer =  lambda: final_trips().write.format("json").mode("append").partitionBy(["vendor", "year_month"])
            )
        )

    @classmethod
    def monthly(cls, enriched_table: str, export_table: str, external_resource_loc: str):
        return cls.from_configurated_save(
            trips = MonthlyTrips.from_table_and_date(enriched_table),
            raw_save = ExternalTable(export_table, ext_resource_loc)
        )

    @classmethod
    def batch(cls, enriched_table: str, export_table: str, external_resource_loc: str):
        return cls.from_configurated_save(
            trips = UnityTable(enriched_table),
            raw_save = ExternalTable(export_table, ext_resource_loc)
        )



ExportCleansed.monthly(enriched_table, export_table, ext_resource_loc).export()






