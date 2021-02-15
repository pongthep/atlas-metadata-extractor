from metadata_extractor.builders.rdbms.postgresql_builder import PostgresqlBuilder


class RedshiftBuilder(PostgresqlBuilder):
    def __init__(self, rdbms_type: str = 'redshift'):
        self.rdbms_type = rdbms_type
