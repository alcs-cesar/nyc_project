from typing import Callable
from abc import ABC, abstractmethod
from pyspark.sql import DataFrameWriter

from modules.io.contracts import SaveRawTrips

class ConfigurableSave:
    def __init__(self, raw_save: SaveRawTrips, writer: Callable[[], DataFrameWriter]):
        self.raw_save = raw_save 
        self.writer = writer

    def save(self):
        self.raw_save.save(self.writer())