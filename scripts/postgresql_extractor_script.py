from typing import List
from builders.rdbms.postgresql_builder import PostgresqlBuilder
from extractor.rdbms.postgresql_extractor import PostgreSQLExtractor
from connection.rdbms.postgresql_connection import PostgreSQLConnection
from models.server_instance.rdbms_instance import RDBMSInstance
from models.rdbms.database_info import Database
from models.rdbms.column_info import Column
from publisher.rdbms_publisher import RDBMSPublisher

if __name__ == "__main__":
    publisher = RDBMSPublisher()
    conn = PostgreSQLConnection(host="data-pipeline.cuorydxzceer.ap-southeast-1.redshift.amazonaws.com",
                                port=5439, db_name="dev", user="datapipeline", password="Datapipeline037")
    cursor = conn.get_conn().cursor()

    extractor = PostgreSQLExtractor()
    builder = PostgresqlBuilder()

    instance: RDBMSInstance = builder.build_instance(conn)
    publisher.publish_instance(instance)

    # db: Database = builder.build_database(conn.db_name, instance)
    #
    # table_names = extractor.get_table_list(cursor=cursor, table_schema='bi_listing')
    #
    # for name in table_names:
    #     table_schema = 'public'
    #     if name.__contains__('.'):
    #         table_schema, name = name.split('.')
    #     table = builder.build_table(name, db)
    #
    #     columns: List[Column] = []
    #     column_list = extractor.extract_column(cursor=cursor, table_schema='bi_listing', table_name=name)
    #     for column_detail in column_list:
    #         column_name = column_detail[0]
    #         column_order = column_detail[1]
    #         column_datatype = column_detail[3]
    #         column_length = column_detail[4]
    #         column = Column(name=column_name, datatype=column_datatype, length=column_length, table=table)

    conn.close_connection()
