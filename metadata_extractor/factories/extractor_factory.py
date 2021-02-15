from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.extractor.rdbms.postgresql_extractor import PostgresqlExtractor
from metadata_extractor.extractor.rdbms.mysql_extractor import MysqlExtractor
from metadata_extractor.models.enum.extractor_enum import RDBMSExtractorName


class RDBMSExtractorFactory:
    @staticmethod
    def create(name: str) -> RDBMSExtractor:
        switcher = {
            RDBMSExtractorName.postgresql.name: PostgresqlExtractor(),
            RDBMSExtractorName.mysql.name: MysqlExtractor()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found connection named \'%s\'' % name)
