from models.rdbms.table_info import Table


class Column:
    def __init__(self, name: str, datatype: str, length: int, table: Table):
        self.name: str = name
        self.datatype: str = datatype
        self.length: int = length
        self.table: Table = table
        self.qualified_name = '{table}_column={column}'.format(table=table.qualified_name, column=name)
