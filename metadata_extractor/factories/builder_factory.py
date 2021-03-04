from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.models.enum.db_engine_enum import DBEngine
from metadata_extractor.builders.rdbms.postgresql_builder import PostgresqlBuilder
from metadata_extractor.builders.rdbms.mysql_builder import MysqlBuilder


class RDBMSBuilderFactory:
    @staticmethod
    def create(name: str) -> RDBMSBuilder:
        switcher = {
            DBEngine.postgresql.name: PostgresqlBuilder(),
            DBEngine.mysql.name: MysqlBuilder()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found builder named \'%s\'' % name)
