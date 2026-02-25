from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

################ INPUT ##########################

class InputTrips(ABC):

    @abstractmethod
    def data(self) -> DataFrame:
        pass

################ OUTPUT ##########################

class SaveTrips(ABC):
    @abstractmethod
    def save(self, trips: DataFrame, mode: str):
        pass