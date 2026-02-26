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

from modules.io.contracts import InputTrips, SaveTrips
from modules.io.external_table import ExternalTable
from modules.io.trips import MonthlyTrips
from modules.utils.columns import with_year_month


# Output Parameters
external_location = "abfss://nyctaxi-yellow@nyctaxistoragemxcentral.dfs.core.windows.net/"
resource_output_dir = "nyctaxi_yellow_export/"
ext_resource_loc = external_location + resource_output_dir

export_table = "nyctaxi.export.nyctaxi_yellow_trips_export"

# Input Parameters
enriched_table = "nyctaxi.silver.yellow_trips_enriched"

def input_trips(enriched_table: str):
    return with_year_month(
        df = MonthlyTrips.from_table_and_date(enriched_table).data(),
        timestamp_col = "tpep_pickup_timestamp",
        fmt = "YYYY-MM",
        formatted_column = "year_month"
    )


def export_as_json(enriched_table: str, export_table: str, external_resource_loc: str):
    _input_trips = input_trips(enriched_table)

    configurated_save = \
        lambda: _input_trips.write.format("json").mode("append").partitionBy(["vendor", "year_month"])

    ExternalTable(export_table, external_resource_loc).save(configurated_save)



export_as_json(enriched_table, export_table, ext_resource_loc)






