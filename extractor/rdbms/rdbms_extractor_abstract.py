from abc import ABC, abstractmethod
from psycopg2.extras import DictCursor


class RDBMSExtractor(ABC):
    @abstractmethod
    def get_table_list(self, cursor: DictCursor, table_schema: str):
        pass

    @abstractmethod
    def extract_table(self, cursor: DictCursor, table_schema: str, table_name: str):
        pass

    @abstractmethod
    def extract_column(self, cursor: DictCursor, table_schema: str, table_name: str):
        pass
