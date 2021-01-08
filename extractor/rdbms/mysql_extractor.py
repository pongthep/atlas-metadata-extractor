from extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractorAbstract


class MySQLExtractor(RDBMSExtractorAbstract):
    def extract_table(self, cursor: None, db_schema: str = '', table_name: str = ''):
        pass

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
