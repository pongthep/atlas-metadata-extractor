from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder


class PostgresqlBuilder(RDBMSBuilder):
    def __init__(self, rdbms_type: str = 'postgresql'):
        self.rdbms_type = rdbms_type
