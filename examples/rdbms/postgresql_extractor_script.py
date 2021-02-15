from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_extractor.models.rdbms.database_info import Database
from metadata_extractor.publisher.atlas.rdbms_publisher import RDBMSPublisher
from metadata_extractor.factories.connection_factory import RDBMSConnectionFactory
from metadata_extractor.factories.extractor_factory import RDBMSExtractorFactory
from metadata_extractor.factories.builder_factory import RDBMSBuilderFactory
from metadata_extractor.models.enum.connection_enum import RDBMSConnectionName
from metadata_extractor.models.enum.builder_enum import RDBMSBuilderName
from metadata_extractor.models.enum.extractor_enum import RDBMSExtractorName
from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from typing import List, Dict
from metadata_extractor.models.rdbms.column_info import Column
from metadata_extractor.publisher.atlas.atlas_publisher import AtlasPublisher

if __name__ == "__main__":
    atlas = AtlasPublisher(host='http://localhost:21000')
    publisher = RDBMSPublisher(atlas_publisher=atlas)

    conn: RDBMSConnection = RDBMSConnectionFactory.create(image_name=RDBMSConnectionName.postgresql.name,
                                                          host="localhost",
                                                          port=5432, db_name="postgres", user="postgres",
                                                          password="q1w2e3r4")

    extractor: RDBMSExtractor = RDBMSExtractorFactory.create(RDBMSExtractorName.postgresql.name)
    builder: RDBMSBuilder = RDBMSBuilderFactory.create(RDBMSBuilderName.postgresql.name)

    instance: RDBMSInstance = builder.build_instance(conn)
    publisher.publish_instance(instance=instance)

    db: Database = builder.build_database(conn.db_name, instance)
    publisher.publish_database(instance_qualified_name=instance.qualified_name, db=db)

    db_schema = builder.build_database_schema(name='public', db=db)
    table_dict: Dict[str, List[Column]] = extractor.extract_db_schema(conn=conn, builder=builder, db_schema=db_schema)

    for table_name in table_dict:
        column_list = table_dict.get(table_name)
        table = column_list[0].table
        publisher.publish_table(db_qualified_name=db.qualified_name, table=table)

        for column in column_list:
            publisher.publish_column(table_qualified_name=table.qualified_name, column=column)

    conn.close_connection()
