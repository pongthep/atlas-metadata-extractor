from metadata_extractor.models.atlas_model.rdbms.database_schema import DatabaseSchema
from typing import List


def get_delimiter():
    return '.'


def get_qualified_name(db_schema: str, table_name: str) -> str:
    return '{schema}{delimiter}{table}'.format(schema=db_schema
                                               , delimiter=get_delimiter()
                                               , table=table_name)


class Table:
    def __init__(self, name: str = '', db_schema: DatabaseSchema = None, desc: str = '', tags: List[str] = []):
        self.name: str = name
        self.db_schema: DatabaseSchema = db_schema
        self.desc: str = desc
        self.tags: List[str] = tags

        self.qualified_name: str = get_qualified_name(db_schema=db_schema.name, table_name=name)
