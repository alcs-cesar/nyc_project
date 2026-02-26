from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, DataFrameWriter

################ INPUT ##########################

class InputTrips(ABC):

    @abstractmethod
    def data(self) -> DataFrame:
        pass

################ OUTPUT ##########################

class SaveRawTrips(ABC):
    @abstractmethod
    def save(self, trips: DataFrameWriter):
        pass
