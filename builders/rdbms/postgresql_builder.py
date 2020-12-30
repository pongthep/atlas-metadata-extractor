from metadata_hub.connection.connection_abstract import RDBMSConnection
from metadata_hub.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_hub.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_hub.models.rdbms.database_info import Database
from metadata_hub.models.rdbms.table_info import Table
from metadata_hub.models.rdbms.column_info import Column


class PostgresqlBuilder(RDBMSBuilder):
    def __init__(self):
        self.rdbms_type = 'postgresql'

    def build_instance(self, conn: RDBMSConnection) -> RDBMSInstance:
        return RDBMSInstance(host=conn.host,
                             port=conn.port,
                             rdbms_type=self.rdbms_type)

    def build_database(self, name: str, instance: RDBMSInstance) -> Database:
        return Database(name, instance)

    def build_table(self, table_name: str = '', table_schema: str = 'public', db: Database = None) -> Table:
        return Table(table_name, table_schema, db)

    def build_column(self, table_name: str, datatype: str, length: int, table: Table) -> Column:
        return Column(table_name, datatype, length, table)
