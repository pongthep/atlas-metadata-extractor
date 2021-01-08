from models.rdbms.database_info import Database


class Table:
    def __init__(self, name: str = '', table_schema: str = '', db: Database = None, desc: str = '', tags: list = []):
        self.name: str = name
        self.table_schema: str = table_schema
        self.db: Database = db
        self.desc: str = desc
        self.tags: list = tags

        self.qualified_name: str = '{schema}.{table}'.format(schema=table_schema, table=name)
