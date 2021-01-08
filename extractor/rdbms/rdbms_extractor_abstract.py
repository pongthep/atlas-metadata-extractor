from abc import ABC, abstractmethod


class RDBMSExtractorAbstract(ABC):
    @abstractmethod
    def get_table_list(self, cursor: None, db_schema: str = ''):
        pass

    @abstractmethod
    def extract_table(self, cursor: None, db_schema: str = '', table_name: str = ''):
        pass

    @abstractmethod
    def extract_column(self, cursor: None, db_schema: str = '', table_name: str = ''):
        pass
