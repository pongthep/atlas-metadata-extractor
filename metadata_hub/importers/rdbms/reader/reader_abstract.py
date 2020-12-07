from abc import ABC, abstractmethod
from metadata_hub.importers.rdbms.connection.connection_abstract import DBConnection


class RDBMSReader(ABC):
    @abstractmethod
    def read_table_meta(self, db_conn: DBConnection, table_name: str = ""):
        pass
