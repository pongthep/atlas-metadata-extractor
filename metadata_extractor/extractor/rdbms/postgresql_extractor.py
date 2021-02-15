# Refer: https://medium.com/explorations-in-python/reading-postgresql-database-schemas-with-python-21b22b46f82c
# Refer: https://kb.objectrocket.com/postgresql/postgres-list-tables-with-python-1023
# Refer: https://stackoverflow.com/questions/19552183/how-to-check-if-key-is-primary-psycopg2
# Refer: https://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys
# Refer: https://github.com/amundsen-io/amundsendatabuilder/blob/master/databuilder/extractor/postgres_metadata_extractor.py

from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.models.rdbms.database_schema import DatabaseSchema
from metadata_extractor.models.rdbms.table_info import Table
from metadata_extractor.models.rdbms.column_info import Column
from typing import List, Dict


class PostgresqlExtractor(RDBMSExtractor):

    # TODO need to be updated or deleted
    @staticmethod
    def get_column_key(conn: RDBMSConnection = None, db_schema: str = '', table_name: str = ''):
        with conn.get_conn().cursor() as cursor:
            sql = "SELECT column_name,constraint_type " \
                  "FROM information_schema.table_constraints " \
                  "JOIN information_schema.key_column_usage " \
                  "USING (constraint_catalog, constraint_schema," \
                  " constraint_name,table_catalog, table_schema, table_name) " \
                  "WHERE (table_schema, table_name) = ('{db_schema}', '{table_name}') " \
                  "ORDER BY ordinal_position".format(db_schema=db_schema, table_name=table_name)
            cursor.execute(sql)
            column_key = cursor.fetchall()
            # TODO pack result into object
        return column_key

    # TODO need to be updated or deleted
    @staticmethod
    def get_column_fk_refer(conn: RDBMSConnection = None, db_schema: str = '', table_name: str = ''):
        with conn.get_conn().cursor() as cursor:
            sql = "SELECT " \
                  "tc.table_schema, " \
                  "tc.constraint_name, " \
                  "tc.table_name, " \
                  "kcu.column_name, " \
                  "ccu.table_schema AS foreign_table_schema, " \
                  "ccu.table_name AS foreign_table_name, " \
                  "ccu.column_name AS foreign_column_name " \
                  "FROM information_schema.table_constraints AS tc " \
                  "JOIN information_schema.key_column_usage AS kcu " \
                  "  ON tc.constraint_name = kcu.constraint_name " \
                  " AND tc.table_schema = kcu.table_schema " \
                  "JOIN information_schema.constraint_column_usage AS ccu " \
                  "ON ccu.constraint_name = tc.constraint_name " \
                  "AND ccu.table_schema = tc.table_schema " \
                  "WHERE tc.constraint_type = 'FOREIGN KEY' AND " \
                  "tc.table_schema='{db_schema}' AND tc.table_name='{table_name}'".format(db_schema=db_schema,
                                                                                          table_name=table_name)

            cursor.execute(sql)

            column_kf_refer = cursor.fetchall()
            # TODO pack result into object
        return column_kf_refer

    def extract_db_schema(self, conn: RDBMSConnection = None, builder: RDBMSBuilder = None,
                          db_schema: DatabaseSchema = None) -> Dict[str, List[Column]]:
        fetch_size: int = 50
        table_map: Dict[str, Table] = {}
        column_map: Dict[str, List[Column]] = {}
        with conn.get_conn().cursor() as cursor:
            sql = "select c.table_schema as schema, c.table_name as name" \
                  ", pgtd.description as description,c.column_name as col_name, c.data_type as col_type" \
                  ", pgcd.description as col_description FROM INFORMATION_SCHEMA.COLUMNS c " \
                  "INNER join pg_catalog.pg_statio_all_tables as st on c.table_schema=st.schemaname " \
                  "and c.table_name=st.relname LEFT join pg_catalog.pg_description pgcd " \
                  "on pgcd.objoid=st.relid and pgcd.objsubid=c.ordinal_position " \
                  "LEFT join pg_catalog.pg_description pgtd on pgtd.objoid=st.relid " \
                  "and pgtd.objsubid=0 WHERE c.table_schema = '{db_schema}' " \
                  "ORDER by schema, name;".format(db_schema=db_schema.name)
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

    def get_table_list(self, conn: RDBMSConnection = None, db_schema: str = 'public'):
        table_list = []

        with conn.get_conn().cursor() as cursor:
            sql = "SELECT table_name FROM information_schema.tables" \
                  " WHERE table_schema = '{db_schema}'".format(db_schema=db_schema)

            cursor.execute(sql)
            query_result = cursor.fetchall()

            for table_name in query_result:
                table_list.append(table_name[0])

        return table_list

    # TODO need to be updated or deleted
    def extract_column(self, conn: RDBMSConnection = None, db_schema: str = 'public', table_name: str = ''):
        with conn.get_conn().cursor() as cursor:
            sql = "SELECT column_name, data_type, is_nullable, character_maximum_length " \
                  "FROM information_schema.columns " \
                  "WHERE table_schema = '{db_schema}' " \
                  "AND table_name = '{table_name}' " \
                  "ORDER BY ordinal_position".format(db_schema=db_schema, table_name=table_name)
            cursor.execute(sql)
            column_schema = cursor.fetchall()
            # TODO pack result into object
        return column_schema
