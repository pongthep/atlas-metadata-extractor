from connection.connection_abstract import RDBMSConnection
from builders.rdbms.postgresql_builder import PostgresqlBuilder
from models.server_instance.rdbms_instance import RDBMSInstance
from models.rdbms.database_info import Database
from models.rdbms.table_info import Table
from models.rdbms.column_info import Column


class RedshiftBuilder(PostgresqlBuilder):
    def __init__(self):
        self.rdbms_type = 'redshift'
