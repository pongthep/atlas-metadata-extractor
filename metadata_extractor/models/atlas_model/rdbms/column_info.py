from metadata_extractor.models.atlas_model.rdbms.table_info import Table


def get_delimiter():
    return '.'


def get_qualified_name(db_schema: str, table_qn: str, col_name: str) -> str:
    return '{db_schema}{delimiter}{table}{delimiter}{column}' \
        .format(delimiter=get_delimiter()
                , db_schema=db_schema
                , table=table_qn
                , column=col_name)


class Column:
    def __init__(self, name: str, data_type: str, length: str, desc: str, table: Table):
        self.name: str = name
        self.data_type: str = data_type
        self.length: str = length
        self.desc: str = desc
        self.table: Table = table
        self.qualified_name = get_qualified_name(db_schema=table.db_schema.name
                                                 , table_qn=table.qualified_name
                                                 , col_name=name)
