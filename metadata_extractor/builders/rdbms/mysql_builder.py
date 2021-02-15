from metadata_extractor.builders.rdbms.rdbms_builder_abstract  import RDBMSBuilder


class MysqlBuilder(RDBMSBuilder):
    def __init__(self, rdbms_type: str = 'mysql'):
        self.rdbms_type = rdbms_type
