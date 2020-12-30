from typing import List
from metadata_hub.builders.rdbms.redshift_builder import RedshiftBuilder
from metadata_hub.extractor.rdbms.postgresql_extractor import PostgreSQLExtractor
from metadata_hub.connection.rdbms.postgresql_connection import PostgreSQLConnection
from metadata_hub.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_hub.models.rdbms.database_info import Database
from metadata_hub.models.rdbms.column_info import Column
from metadata_hub.publisher.rdbms_publisher import RDBMSPublisher

if __name__ == "__main__":
    publisher = RDBMSPublisher()
    conn = PostgreSQLConnection(host="data-pipeline.cuorydxzceer.ap-southeast-1.redshift.amazonaws.com",
                                port=5439, db_name="dev", user="datapipeline", password="Datapipeline037")
    cursor = conn.get_conn().cursor()

    extractor = PostgreSQLExtractor()
    builder = RedshiftBuilder()

    instance: RDBMSInstance = builder.build_instance(conn)
    publisher.publish_instance(instance=instance)

    db: Database = builder.build_database(conn.db_name, instance)
    publisher.publish_database(instance_qualified_name=instance.qualified_name, db=db)

    table_schema = 'bi_listing'
    table_names = extractor.get_table_list(cursor=cursor, table_schema=table_schema)

    for name in table_names:
        if name.__contains__('.'):
            table_schema, name = name.split('.')
        table = builder.build_table(table_name=name, table_schema=table_schema, db=db)
        publisher.publish_table(db_qualified_name=db.qualified_name, table=table)

        column_list = extractor.extract_column(cursor=cursor, table_schema='bi_listing', table_name=name)
        for column_detail in column_list:
            column_name = column_detail[0]
            column_order = column_detail[1]
            column_datatype = column_detail[3]
            column_length = column_detail[4]
            column = Column(name=column_name, datatype=column_datatype, length=column_length, table=table)
            publisher.publish_column(table_qualified_name=table.qualified_name, column=column)

    conn.close_connection()
