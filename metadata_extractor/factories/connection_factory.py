from metadata_extractor.connection.rdbms.postgresql_connection import PostgresqlConnection
from metadata_extractor.connection.rdbms.mysql_connection import MysqlConnection
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.models.enum.connection_enum import RDBMSConnectionName


class RDBMSConnectionFactory:
    @staticmethod
    def create(image_name: str, host: str = '', port: int = 1111, db_name: str = '', user: str = '',
               password: str = '', **kwargs: {}) -> RDBMSConnection:
        switcher = {
            RDBMSConnectionName.postgresql.name: PostgresqlConnection(),
            RDBMSConnectionName.mysql.name: MysqlConnection()
        }

        builder_obj = switcher.get(image_name, None)

        if builder_obj is not None:
            builder_obj.create_conn(host, port, db_name, user, password, **kwargs)
            return builder_obj
        else:
            raise ValueError('Not found connection named \'%s\'' % image_name)
