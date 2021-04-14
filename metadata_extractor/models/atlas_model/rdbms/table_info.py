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

        self.qualified_name: str = get_qualified_name(db_schema=db_schema.qualified_name, table_name=name)


class TableForeignKey:
    def __init__(self,
                 db_schema_base: DatabaseSchema = None,
                 table_base: str = '',
                 column_base: str = '',
                 schema_refer: str = '',
                 table_refer: str = '',
                 column_refer: str = ''):
        # Table that contain FK is base table
        self.table_base_qn: str = get_qualified_name(db_schema=db_schema_base.qualified_name,
                                                     table_name=table_base)
        self.column_base_qn: str = '{table_qn}{delimiter}{column_name}' \
            .format(table_qn=self.table_base_qn, delimiter=get_delimiter(), column_name=column_base)

        self.table_refer_qn: str = '{db_qn}{delimiter}{schema_refer_name}{delimiter}{table_table}' \
            .format(db_qn=db_schema_base.db.name, delimiter=get_delimiter(), schema_refer_name=schema_refer,
                    table_table=table_refer)
        self.column_refer_qn: str = '{table_qn}{delimiter}{column_name}' \
            .format(table_qn=self.table_refer_qn, delimiter=get_delimiter(), column_name=column_refer)

        self.qualified_name: str = '{base_table_column}#FK#{refer_table_column}' \
            .format(base_table_column=self.column_base_qn,
                    refer_table_column=self.column_refer_qn)
