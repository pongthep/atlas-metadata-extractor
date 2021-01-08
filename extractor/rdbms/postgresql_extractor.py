# Refer: https://medium.com/explorations-in-python/reading-postgresql-database-schemas-with-python-21b22b46f82c
# Refer: https://kb.objectrocket.com/postgresql/postgres-list-tables-with-python-1023
# Refer: https://stackoverflow.com/questions/19552183/how-to-check-if-key-is-primary-psycopg2
# Refer: https://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys

from extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractorAbstract
from connection.connection_abstract import RDBMSConnectionAbstract
from psycopg2.extras import DictCursor


class PostgreSQLExtractor(RDBMSExtractorAbstract):

    @staticmethod
    def get_column_key(cursor: DictCursor = None, db_schema: str = '', table_name: str = ''):
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

    @staticmethod
    def get_column_fk_refer(cursor: DictCursor = None, db_schema: str = '', table_name: str = ''):
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

    def extract_table(self, cursor: DictCursor = None, db_schema: str = '', table_name: str = ''):
        pass

    # def extract_table(self, db_conn: RDBMSConnection, table_name: str):
    #     table_schema = 'public'
    #     if table_name.__contains__('.'):
    #         table_schema, table_name = table_name.split('.')
    #
    #     cursor = db_conn.get_conn().cursor()
    #
    #     column_schema = self.get_column_datatype(cursor, table_schema, table_name)
    #     column_key = self.get_column_key(cursor, table_schema, table_name)
    #     column_fk_refer = self.get_column_fk_refer(cursor, table_schema, table_name)
    #
    #     cursor.close()
    #
    #     print(column_schema)
    #     print(column_key)
    #     print(column_fk_refer)

    def get_table_list(self, cursor: DictCursor = None, db_schema: str = 'public'):
        sql = "SELECT table_name FROM information_schema.tables" \
              " WHERE table_schema = '{db_schema}'".format(db_schema=db_schema)

        cursor.execute(sql)
        query_result = cursor.fetchall()
        table_list = []
        for table_name in query_result:
            table_list.append(table_name[0])

        return table_list

    def extract_column(self, cursor: DictCursor = None, db_schema: str = 'public', table_name: str = ''):
        sql = "SELECT column_name, data_type, is_nullable, character_maximum_length " \
              "FROM information_schema.columns " \
              "WHERE table_schema = '{db_schema}' " \
              "AND table_name = '{table_name}' " \
              "ORDER BY ordinal_position".format(db_schema=db_schema, table_name=table_name)
        cursor.execute(sql)
        column_schema = cursor.fetchall()
        # TODO pack result into object
        return column_schema
