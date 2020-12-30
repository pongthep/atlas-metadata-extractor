from abc import ABC, abstractmethod
from metadata_hub.connection.connection_abstract import RDBMSConnection
from metadata_hub.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_hub.models.rdbms.database_info import Database
from metadata_hub.models.rdbms.table_info import Table
from metadata_hub.models.rdbms.column_info import Column


class RDBMSBuilder(ABC):
    @abstractmethod
    def build_instance(self, conn: RDBMSConnection) -> RDBMSInstance:
        pass

    @abstractmethod
    def build_database(self, name: str, instance: RDBMSInstance) -> Database:
        pass

    @abstractmethod
    def build_table(self, table_name: str = '', table_schema: str = 'public', db: Database = None) -> Table:
        pass

    @abstractmethod
    def build_column(self, table_name: str, datatype: str, length: int, table: Table) -> Column:
        pass
