from metadata_extractor.models.rdbms.table_info import Table


class Column:
    def __init__(self, name: str, data_type: str, length: str, desc: str, table: Table):
        self.name: str = name
        self.data_type: str = data_type
        self.length: str = length
        self.desc: str = desc
        self.table: Table = table
        self.qualified_name = '{db_schema}.{table}.{column}' \
            .format(db_schema=table.db_schema.name, table=table.qualified_name, column=name)
