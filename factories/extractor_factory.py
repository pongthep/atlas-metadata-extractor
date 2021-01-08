from extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractorAbstract
from extractor.rdbms.postgresql_extractor import PostgreSQLExtractor
from models.enum.extractor_enum import RDBMSExtractorName


class RDBMSExtractorFactory:
    @staticmethod
    def create(name: str) -> RDBMSExtractorAbstract:
        switcher = {
            RDBMSExtractorName.postgresql.name: PostgreSQLExtractor()
        }

        builder_obj = switcher.get(name, None)

        if builder_obj is not None:
            return builder_obj
        else:
            raise ValueError('Not found connection named \'%s\'' % name)
