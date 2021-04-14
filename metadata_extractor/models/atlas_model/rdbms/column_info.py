from metadata_extractor.models.atlas_model.rdbms.table_info import Table


def get_delimiter():
    return '.'


def get_qualified_name(table_qn: str, col_name: str) -> str:
    return '{table}{delimiter}{column}' \
        .format(delimiter=get_delimiter()
                , table=table_qn
                , column=col_name)


class Column:
    def __init__(self, name: str = '',
                 data_type: str = '',
                 length: str = '',
                 desc: str = '',
                 is_pk: bool = False,
                 table: Table = None):
        self.name: str = name
        self.data_type: str = data_type
        self.length: str = length
        self.desc: str = desc
        self.is_pk: str = is_pk
        self.table: Table = table
        self.qualified_name = get_qualified_name(table_qn=table.qualified_name
                                                 , col_name=name)
