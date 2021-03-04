from typing import List, Dict
from metadata_extractor.models.hosts.rdbms_host import RDBMSHost
from metadata_extractor.services.rdbms_service import RDBMSService
from metadata_extractor.connection.connection_abstract import RDBMSConnection
from metadata_extractor.extractor.rdbms.rdbms_extractor_abstract import RDBMSExtractor
from metadata_extractor.builders.rdbms.rdbms_builder_abstract import RDBMSBuilder
from metadata_extractor.factories.connection_factory import RDBMSConnectionFactory
from metadata_extractor.factories.extractor_factory import RDBMSExtractorFactory
from metadata_extractor.factories.builder_factory import RDBMSBuilderFactory
from metadata_extractor.models.atlas_model.rdbms.rdbms_instance import RDBMSInstance
from metadata_extractor.models.atlas_model.rdbms.database_info import Database
from metadata_extractor.models.atlas_model.rdbms.column_info import Column


class RDBMSPipeline:
    @staticmethod
    def sync_full_db(image_name: str, rdbms_service: RDBMSService, rdbms_host: RDBMSHost):
        conn: RDBMSConnection = RDBMSConnectionFactory.create(image_name=image_name,
                                                              host=rdbms_host.host,
                                                              port=rdbms_host.port, db_name=rdbms_host.db_name,
                                                              user=rdbms_host.db_user,
                                                              password=rdbms_host.db_password)

        extractor: RDBMSExtractor = RDBMSExtractorFactory.create(image_name)
        builder: RDBMSBuilder = RDBMSBuilderFactory.create(image_name)

        instance: RDBMSInstance = builder.build_instance(conn)
        rdbms_service.publish_instance(instance=instance, db_name=conn.db_name)

        db: Database = builder.build_database(conn.db_name, instance)
        rdbms_service.publish_database(instance_qualified_name=instance.qualified_name, db=db)

        db_schema = builder.build_database_schema(name=rdbms_host.db_schema, db=db)
        table_dict: Dict[str, List[Column]] = extractor.extract_db_schema(conn=conn, builder=builder,
                                                                          db_schema=db_schema)

        for table_name in table_dict:
            column_list = table_dict.get(table_name)
            table = column_list[0].table
            rdbms_service.publish_table(db_qualified_name=db.qualified_name, table=table)

            for column in column_list:
                rdbms_service.publish_column(table_qualified_name=table.qualified_name, column=column)

        conn.close_connection()
