# Refer: https://github.com/amundsen-io/amundsendatabuilder/blob/master/databuilder/extractor/mysql_metadata_extractor.py

from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.models.atlas_model.rdbms.database_schema import DatabaseSchema
from metadata_extractor.models.atlas_model.rdbms.table_info import Table, TableForeignKey
from metadata_extractor.models.atlas_model.rdbms.column_info import Column
from typing import List, Dict, Set


class MysqlExtractor(RDBMSExtractor):
    def extract_db_schema(self, conn: RDBMSConnection = None, builder: RDBMSBuilder = None,
                          db_schema: DatabaseSchema = None) -> Dict[str, List[Column]]:
        fetch_size: int = 50
        table_map: Dict[str, Table] = {}
        column_map: Dict[str, List[Column]] = {}
        pk_map: Dict[str, Set[str]] = self.extract_table_pk(conn=conn, builder=builder, db_schema=db_schema)
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

                    is_col_pk = col_name in pk_map.get(table_name, {})

                    col_obj: Column = builder.build_column(column_name=col_name, data_type=col_type, desc=col_desc,
                                                           is_pk=is_col_pk, table=table_obj)
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

    def extract_table_fk(self, conn: RDBMSConnection = None, builder: RDBMSBuilder = None,
                         db_schema: DatabaseSchema = None) -> Dict[str, Set[str]]:
        fetch_size: int = 50
        table_fk_map: Dict[str, List[TableForeignKey]] = {}
        with conn.get_conn().cursor() as cursor:
            sql = "SELECT TABLE_NAME, " \
                  "COLUMN_NAME, " \
                  "REFERENCED_TABLE_SCHEMA, " \
                  "REFERENCED_TABLE_NAME, " \
                  "REFERENCED_COLUMN_NAME " \
                  "FROM " \
                  "INFORMATION_SCHEMA.KEY_COLUMN_USAGE " \
                  "WHERE " \
                  " REFERENCED_TABLE_SCHEMA = '{db_name}';".format(db_name=db_schema.db.name)
            cursor.execute(sql)
            rows = cursor.fetchmany(fetch_size)

            while rows is not None and len(rows) > 0:
                for row in rows:
                    base_table_name = row[0]
                    base_column_name = row[1]
                    refer_table_schema = row[2]
                    refer_table_name = row[3]
                    refer_column_name = row[4]

                    fk_list = table_fk_map.get(base_table_name, [])
                    table_fk = TableForeignKey(db_schema_base=db_schema, table_base=base_table_name
                                               , column_base=base_column_name
                                               , schema_refer=refer_table_schema
                                               , table_refer=refer_table_name
                                               , column_refer=refer_column_name)

                    fk_list.append(table_fk)
                    table_fk_map.update({base_table_name: fk_list})
                rows = cursor.fetchmany(fetch_size)

        return table_fk_map

    def extract_table_pk(self, conn: RDBMSConnection = None, builder: RDBMSBuilder = None,
                         db_schema: DatabaseSchema = None) -> Dict[str, Set[str]]:
        fetch_size: int = 50
        pk_map: Dict[str, Set[str]] = {}
        with conn.get_conn().cursor() as cursor:
            sql = "SELECT t.table_name,k.column_name " \
                  "FROM information_schema.table_constraints t  " \
                  "JOIN information_schema.key_column_usage k  " \
                  "USING(constraint_name,table_schema,table_name)  " \
                  "WHERE t.constraint_type='PRIMARY KEY'  " \
                  "AND t.table_schema='{db_name}';".format(db_name=db_schema.db.name)
            cursor.execute(sql)
            rows = cursor.fetchmany(fetch_size)

            while rows is not None and len(rows) > 0:
                for row in rows:
                    table_name = row[0]
                    column_name = row[1]

                    column_pk_set = pk_map.get(table_name, set())
                    column_pk_set.add(column_name)
                    pk_map.update({table_name: column_pk_set})
                rows = cursor.fetchmany(fetch_size)

        return pk_map
