from connection.rdbms.postgresql_connection import PostgreSQLConnection
from connection.connection_abstract import RDBMSConnectionAbstract
from models.enum.connection_enum import RDBMSConnectionName


class RDBMSConnectionFactory:
    @staticmethod
    def create(name: str) -> RDBMSConnectionAbstract:
        switcher = {
            RDBMSConnectionName.postgresql.name: PostgreSQLConnection()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found connection named \'%s\'' % name)
