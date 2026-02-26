from typing import Callable
from abc import ABC, abstractmethod
from pyspark.sql import DataFrameWriter

def configurated_save(writer: DataFrameWriter) -> DataFrameWriter:

    def write_mode(mode: str):
        def partitioned(cols: List[str]):
            def formatted(file_format: str):
                return writer.mode(mode).partitionBy(cols).format(file_format)
            return formatted
        return partitioned

    return write_mode

# configurated(w)("append")(["mode", "month"])("json")
## class ConfiguratedSave | FeaturedSave
## Formatted( Partitioned( Appendable( data ), cols ), "json" )

class AttributedSave(ABC):
    @abstractmethod
    def writer(self) -> DataFrameWriter:
        pass

class NoAttribute(AttributedSave):
    def __init__(self, writer: Callable[[], DataFrameWriter]):
        self.writer = writer

    def writer(self) -> DataFrameWriter:
        self.writer()

class Formatted(AttributedSave):
    def __init__(self, fmt: str, config_save):
        self.config_save = config_save 
        self.format = fmt

    def writer(self) -> DataFrameWriter:
        return self.config_save.writer().format(self.format)

class Partitioned(AttributedSave):
    def __init__(self, partition_cols: List[str], config_save):
        self.config_save = config_save 
        self.partition_cols = partition_cols

    def writer(self) -> DataFrameWriter:
        return self.config_save.writer().partition_cols(self.partition_cols)

class WriteMode(AttributedSave):
    def __init__(self, write_mode, config_save):
        self.config_save = config_save 
        self.write_mode = write_mode

    def writer(self) -> DataFrameWriter:
        return self.config_save.writer().mode(self.write_mode)

class ConfiguratedSave(AttributedSave):
    def __init__(self, attributed_save: AttributedSave):
        self.attributed_save = attributed_save

    def writer(self) -> AttributedSave:
        return self.attributed_save.writer()

    @classmethod
    def from_format_partitions_write_mode(cls, trips: InputTrips, file_format: str, partition_cols: List[str], write_mode: str):
        attributed = Formatted( 
            file_format,
            Partitioned(
                partition_cols,
                WriteMode(
                    write_mode,
                    NoAttribute( lambda: trips.data().write )
                )
            ) 
        )

        return cls( attributed )