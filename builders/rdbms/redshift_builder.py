from metadata_hub.connection.connection_abstract import RDBMSConnection
from metadata_hub.builders.rdbms.postgresql_builder import PostgresqlBuilder
from metadata_hub.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_hub.models.rdbms.database_info import Database
from metadata_hub.models.rdbms.table_info import Table
from metadata_hub.models.rdbms.column_info import Column


class RedshiftBuilder(PostgresqlBuilder):
    def __init__(self):
        self.rdbms_type = 'redshift'
