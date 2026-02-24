from pyspark.sql import Column
from pyspark.sql.functions import lit
import sys

################ Import App Dependencies ################

def register_component_location(location: str):
    sys.path.append(location) 

register_component_location("/Volume/Shared/nyc_project/transformations")

from utils.app_entities import Date

#########################################################

class IColumn(ABC):

    @abstractmethod
    def column(self) -> Column:
        pass

class ProcessingDate(IColumn):
    def __init__(self, processing_date: Date):
        self._date = processing_date

    def column(self) -> Column:
        return lit(f"{date.date()}").cast("date")

class DateAsColumn(IColumn):
    def __init__(self, date_value: Date):
        self._date = date_value

    def column(self) -> Column:
        return lit(f"{date.date()}").cast("date")