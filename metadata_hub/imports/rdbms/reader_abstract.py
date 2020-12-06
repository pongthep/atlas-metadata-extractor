from abc import ABC, abstractmethod


class RDBMSReader(ABC):
    @abstractmethod
    def read_table_meta(self):
        pass
