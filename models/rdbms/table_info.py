from metadata_hub.models.rdbms.database_info import Database


class Table:
    def __init__(self, name: str, table_schema: str, db: Database):
        self.name: str = name
        self.table_schema: str = table_schema
        self.db: Database = db
        self.qualified_name: str = '{schema}.{table}'.format(schema=table_schema, table=name)
