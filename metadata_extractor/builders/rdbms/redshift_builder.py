from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder


class RedshiftBuilder(RDBMSBuilder):
    def __init__(self, rdbms_type: str = 'redshift'):
        self.rdbms_type = rdbms_type
