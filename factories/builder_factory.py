from builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from models.enum.builder_enum import RDBMSBuilderName
from builders.rdbms.postgresql_builder import PostgresqlBuilder
from builders.rdbms.mysql_builder import MysqlBuilder


class RDBMSBuilderFactory:
    @staticmethod
    def create(name: str) -> RDBMSBuilder:
        switcher = {
            RDBMSBuilderName.postgresql.name: PostgresqlBuilder(),
            RDBMSBuilderName.mysql.name: MysqlBuilder()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found builder named \'%s\'' % name)
