from metadata_extractor.models.rdbms.database_info import Database


class DatabaseSchema:
    def __init__(self, name: str = '', db: Database = None):
        self.name: str = name
        self.db: Database = db

        self.qualified_name: str = '{db}.{schema}'.format(db=db.name, schema=name)
