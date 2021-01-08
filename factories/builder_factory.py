from builders.rdbms.rdbms_builder_abstract import RDBMSBuilderAbstract
from models.enum.builder_enum import RDBMSBuilderName
from builders.rdbms.postgresql_builder import PostgresqlBuilder


class RDBMSBuilderFactory:
    @staticmethod
    def create(name: str) -> RDBMSBuilderAbstract:
        switcher = {
            RDBMSBuilderName.postgresql.name: PostgresqlBuilder()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found builder named \'%s\'' % name)
