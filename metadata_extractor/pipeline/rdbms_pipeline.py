from typing import List, Dict, Set
from metadata_extractor.models.hosts.rdbms_host import RDBMSHost
from metadata_extractor.publisher.rdbms_publisher import RDBMSPublisher
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.factories.connection_factory import RDBMSConnectionFactory
from metadata_extractor.factories.extractor_factory import RDBMSExtractorFactory
from metadata_extractor.factories.builder_factory import RDBMSBuilderFactory
from metadata_extractor.models.atlas_model.rdbms.rdbms_instance import RDBMSInstance
from metadata_extractor.models.atlas_model.rdbms.database_info import Database
from metadata_extractor.models.atlas_model.rdbms.column_info import Column
from metadata_extractor.models.atlas_model.rdbms.table_info import TableForeignKey


class RDBMSPipeline:
    @staticmethod
    def sync_full_db(engine_name: str, rdbms_publisher: RDBMSPublisher, rdbms_host: RDBMSHost):
        conn: RDBMSConnection = RDBMSConnectionFactory.create(engine_name=engine_name,
                                                              host=rdbms_host.host,
                                                              port=rdbms_host.port, db_name=rdbms_host.db_name,
                                                              user=rdbms_host.db_user,
                                                              password=rdbms_host.db_password)

        # create extractor
        extractor: RDBMSExtractor = RDBMSExtractorFactory.create(engine_name)
        builder: RDBMSBuilder = RDBMSBuilderFactory.create(engine_name)

        instance: RDBMSInstance = builder.build_instance(conn)
        rdbms_publisher.publish_instance(instance=instance, db_name=conn.db_name)

        db: Database = builder.build_database(conn.db_name, instance)
        rdbms_publisher.publish_database(instance_qualified_name=instance.qualified_name, db=db)

        db_schema = builder.build_database_schema(name=rdbms_host.db_schema, db=db)
        table_dict: Dict[str, List[Column]] = extractor.extract_db_schema(conn=conn, builder=builder,
                                                                          db_schema=db_schema)

        for table_name in table_dict:
            column_list = table_dict.get(table_name)
            table = column_list[0].table
            rdbms_publisher.publish_table(db_qualified_name=db.qualified_name, table=table)

            for column in column_list:
                rdbms_publisher.publish_column(table_qualified_name=table.qualified_name, column=column)

        table_fk_dict: Dict[str, List[TableForeignKey]] = extractor.extract_table_fk(conn=conn, builder=builder,
                                                                                     db_schema=db_schema)
        for table_name, table_fk_list in table_fk_dict.items():
            for table_fk_obj in table_fk_list:
                rdbms_publisher.publish_table_foreign_key(table_fk_obj)

        conn.close_connection()
