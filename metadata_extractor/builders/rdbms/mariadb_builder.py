from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder


class MaridbBuilder(RDBMSBuilder):
    def __init__(self, rdbms_type: str = 'mariadb'):
        self.rdbms_type = rdbms_type
