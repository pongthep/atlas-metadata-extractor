from abc import ABC
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_extractor.models.rdbms.database_info import Database
from metadata_extractor.models.rdbms.database_schema import DatabaseSchema
from metadata_extractor.models.rdbms.table_info import Table
from metadata_extractor.models.rdbms.column_info import Column


class RDBMSBuilder(ABC):
    def __init__(self, rdbms_type: str = 'undefined '):
        self.rdbms_type = rdbms_type

    def build_instance(self, conn: RDBMSConnection = None) -> RDBMSInstance:
        return RDBMSInstance(host=conn.host,
                             port=conn.port,
                             rdbms_type=self.rdbms_type)

    @staticmethod
    def build_database(name: str = '', instance: RDBMSInstance = None) -> Database:
        return Database(name, instance)

    @staticmethod
    def build_database_schema(name: str = 'public', db: Database = None) -> DatabaseSchema:
        return DatabaseSchema(name, db)

    @staticmethod
    def build_table(table_name: str = '', desc: str = '', db_schema: DatabaseSchema = None) -> Table:
        return Table(name=table_name, db_schema=db_schema, desc=desc)

    @staticmethod
    def build_column(column_name: str = '', data_type: str = '', length: str = '', desc: str = '',
                     table: Table = None) -> Column:
        return Column(column_name, data_type, length, desc, table)
