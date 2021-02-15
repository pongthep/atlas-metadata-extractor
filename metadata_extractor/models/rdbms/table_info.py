from metadata_extractor.models.rdbms.database_schema import DatabaseSchema
from typing import List


class Table:
    def __init__(self, name: str = '', db_schema: DatabaseSchema = None, desc: str = '', tags: List[str] = []):
        self.name: str = name
        self.db_schema: DatabaseSchema = db_schema
        self.desc: str = desc
        self.tags: List[str] = tags

        self.qualified_name: str = '{schema}.{table}'.format(schema=db_schema.name, table=name)
