from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import lit, date_format
from abc import ABC, abstractmethod
import sys
import os

################ Import App Dependencies ################

def register_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

    if project_root not in sys.path:
        sys.path.append(project_root) 

register_project_root()

from modules.utils.app_entities import Date

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
        return lit(f"{self._date.date()}").cast("date")

def with_year_month(df: DataFrame, timestamp_col: str, fmt: str, formatted_column: str = "year_month"): 
    return df.withColumn( formatted_column, date_format(df[timestamp_col], fmt) )