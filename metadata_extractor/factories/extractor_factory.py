from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.extractor.rdbms.postgresql_extractor import PostgresqlExtractor
from metadata_extractor.extractor.rdbms.mysql_extractor import MysqlExtractor
from metadata_extractor.models.enum.db_engine_enum import DBEngine


class RDBMSExtractorFactory:
    @staticmethod
    def create(name: str) -> RDBMSExtractor:
        switcher = {
            DBEngine.postgresql.name: PostgresqlExtractor(),
            DBEngine.mysql.name: MysqlExtractor()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found connection named \'%s\'' % name)
