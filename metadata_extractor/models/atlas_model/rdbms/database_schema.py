from metadata_extractor.models.atlas_model.rdbms.database_info import Database


def get_delimiter():
    return '.'


def get_qualified_name(db_name: str, db_schema: str) -> str:
    return '{db}{delimiter}{schema}'.format(db=db_name, delimiter=get_delimiter(), schema=db_schema)


class DatabaseSchema:
    def __init__(self, name: str = '', db: Database = None):
        self.name: str = name
        self.db: Database = db

        self.qualified_name: str = get_qualified_name(db_name=db.name, db_schema=name)
