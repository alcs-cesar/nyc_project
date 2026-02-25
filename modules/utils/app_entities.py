from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from abc import ABC, abstractmethod

# processing_date = execution_date - months

class Date(ABC):

    @abstractmethod
    def date(self) -> date:
        pass

class MonthsAgo(Date):
    def __init__(self, reference_date: date, months_ago: int = 3):
        self._reference_date = reference_date 
        self._months_ago = months_ago

    def date(self):
        return self._reference_date - relativedelta(months=self._months_ago)

    @classmethod
    def from_today(cls, months_ago: int = None):
        return cls(date.today(), month_ago)

    @classmethod
    def from_beginning_of_month(cls, ref_date: date, months_ago: int = None):
        return cls(ref_date.replace(day=1), months_ago)

    @classmethod
    def from_current_month_first_day(cls, months_ago: int = None):
        return cls.from_beginning_of_month(date.today(), months_ago)