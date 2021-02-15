from abc import ABC, abstractmethod
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.models.rdbms.database_schema import DatabaseSchema
from metadata_extractor.models.rdbms.column_info import Column
from typing import List, Dict


class RDBMSExtractor(ABC):
    @abstractmethod
    def get_table_list(self, conn: RDBMSConnection = None, db_schema: str = ''):
        pass

    @abstractmethod
    def extract_db_schema(self, conn: RDBMSConnection = None, builder: RDBMSBuilder = None,
                          db_schema: DatabaseSchema = None) -> Dict[str, List[Column]]:
        pass

    @abstractmethod
    def extract_column(self, conn: RDBMSConnection = None, db_schema: str = '', table_name: str = ''):
        pass
