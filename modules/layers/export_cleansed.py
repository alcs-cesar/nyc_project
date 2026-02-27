from pyspark.sql import DataFrame, DataFrameWriter
from typing import List, Callable

import os
import sys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Component Location

# COMMAND ----------

def register_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

    if project_root not in sys.path:
        sys.path.append(project_root) 

register_project_root()

from modules.io.contracts import InputTrips, SaveRawTrips
from modules.io.external_table import ExternalTable
from modules.io.trips import MonthlyTrips
from modules.io.configurated_save import ConfigurableSave
from modules.utils.columns import with_year_month


class ExportCleansed:
    def __init__(self, save_trips: ConfigurableSave ):
        self.save_trips = save_trips

    def export(self):
        self.save_trips.save()


    @classmethod    
    def from_configurated_save(
        cls, trips: InputTrips, raw_save: SaveRawTrips, final_writer: Callable[[DataFrameWriter], DataFrameWriter]
    ):
        final_trips = \
            lambda t: with_year_month(t.data(), "tpep_pickup_timestamp", "YYYY-MM", "year_month")

        common_writer = \
            lambda t: t.write.format("json").partitionBy(["vendor", "year_month"])

        return cls(
            ConfigurableSave(
                raw_save = raw_save,
                writer = lambda: final_writer(common_writer(final_trips(trips)))
            )
        )

    @classmethod
    def monthly(cls, enriched_table: str, export_table: str, external_resource_loc: str):
        return cls.from_configurated_save(
            trips = MonthlyTrips.from_table_and_date(enriched_table),
            raw_save = ExternalTable(export_table, ext_resource_loc),
            final_writer = lambda w: w.mode("append")
        )

    @classmethod
    def batch(cls, enriched_table: str, export_table: str, external_resource_loc: str):
        return cls.from_configurated_save(
            trips = UnityTable(enriched_table),
            raw_save = ExternalTable(export_table, ext_resource_loc),
            final_writer = lambda w: w.mode("overwrite")
        )


####################################################################################################################


# Output Parameters
external_location = "abfss://nyctaxi-yellow@nyctaxistorage42.dfs.core.windows.net/"
resource_output_dir = "nyctaxi_yellow_export/"
ext_resource_loc = external_location + resource_output_dir

export_table = "nyctaxi.export.nyctaxi_yellow_trips_export"

# Input Parameters
enriched_table = "nyctaxi.silver.yellow_trips_enriched"

def export_monthly():
    return ExportCleansed.monthly(enriched_table, export_table, ext_resource_loc).export()

def export_batch():
    return ExportCleansed.batch(enriched_table, export_table, ext_resource_loc).export()




