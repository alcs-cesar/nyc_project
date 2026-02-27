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

from modules.layers.export_enriched import export_monthly

####################################################################################

export_monthly()