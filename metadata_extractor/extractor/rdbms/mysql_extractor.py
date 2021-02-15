# Refer: https://github.com/amundsen-io/amundsendatabuilder/blob/master/databuilder/extractor/mysql_metadata_extractor.py

from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.models.rdbms.database_schema import DatabaseSchema
from metadata_extractor.models.rdbms.table_info import Table
from metadata_extractor.models.rdbms.column_info import Column
from typing import List, Dict


class MysqlExtractor(RDBMSExtractor):
    def extract_db_schema(self, conn: RDBMSConnection = None, builder: RDBMSBuilder = None,
                          db_schema: DatabaseSchema = None) -> Dict[str, List[Column]]:
        fetch_size: int = 50
        table_map: Dict[str, Table] = {}
        column_map: Dict[str, List[Column]] = {}
        with conn.get_conn().cursor() as cursor:
            sql = "SELECT lower(c.table_schema) AS \"schema\", " \
                  "lower(c.table_name) AS name, " \
                  "CONVERT(t.table_comment, CHAR) AS description, " \
                  "lower(c.column_name) AS col_name, " \
                  " CONVERT(lower(c.data_type), CHAR) AS col_type, " \
                  "CONVERT(c.column_comment, CHAR) AS col_description " \
                  "FROM " \
                  "INFORMATION_SCHEMA.COLUMNS AS c " \
                  "LEFT JOIN " \
                  "INFORMATION_SCHEMA.TABLES t " \
                  "ON c.TABLE_NAME = t.TABLE_NAME " \
                  "AND c.TABLE_SCHEMA = t.TABLE_SCHEMA " \
                  "where c.table_schema = '{db_name}' " \
                  "ORDER by \"schema\", name ;".format(db_name=db_schema.db.name)
            cursor.execute(sql)
            rows = cursor.fetchmany(fetch_size)

            while rows is not None and len(rows) > 0:
                for row in rows:
                    table_name = row[1]
                    table_desc = row[2]
                    col_name = row[3]
                    col_type = row[4]
                    col_desc = row[5]

                    table_obj: Table = table_map.get(table_name,
                                                     builder.build_table(table_name=table_name, desc=table_desc,
                                                                         db_schema=db_schema))
                    table_map.update({table_name: table_obj})

                    col_obj: Column = builder.build_column(column_name=col_name, data_type=col_type, desc=col_desc,
                                                           table=table_obj)
                    column_list = column_map.get(table_name, [])
                    column_list.append(col_obj)
                    column_map.update({table_name: column_list})
                rows = cursor.fetchmany(fetch_size)

        return column_map

    def extract_column(self, cursor: None, db_schema: str = '', table_name: str = ''):
        sql = 'SHOW columns FROM {table_name}'.format(table_name=table_name)
        cursor.execute(sql)
        column_schema = cursor.fetchall()
        # TODO pack result into object
        return column_schema

    def get_table_list(self, cursor: None, db_schema: str = 'public'):
        sql = 'SHOW TABLES'

        cursor.execute(sql)
        query_result = cursor.fetchall()
        table_list = []
        for table_name in query_result:
            table_list.append(table_name[0])

        return table_list
