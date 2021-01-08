from abc import ABC, abstractmethod
from connection.connection_abstract import RDBMSConnectionAbstract
from models.server_instance.rdbms_instance import RDBMSInstance
from models.rdbms.database_info import Database
from models.rdbms.table_info import Table
from models.rdbms.column_info import Column


class RDBMSBuilderAbstract(ABC):
    def build_instance(self, conn: RDBMSConnectionAbstract) -> RDBMSInstance:
        return RDBMSInstance(host=conn.host,
                             port=conn.port,
                             rdbms_type=self.rdbms_type)

    @staticmethod
    def build_database(name: str, instance: RDBMSInstance) -> Database:
        return Database(name, instance)

    @staticmethod
    def build_table(table_name: str = '', table_schema: str = 'public', db: Database = None) -> Table:
        return Table(table_name, table_schema, db)

    @staticmethod
    def build_column(table_name: str, datatype: str, length: int, table: Table) -> Column:
        return Column(table_name, datatype, length, table)
