from metadata_extractor.publisher.atlas_publisher import AtlasPublisher
from metadata_extractor.publisher.rdbms_publisher import RDBMSPublisher
from metadata_extractor.pipeline.rdbms_pipeline import RDBMSPipeline
from metadata_extractor.models.hosts.rdbms_host import RDBMSHost
from metadata_extractor.models.enum.db_engine_enum import DBEngine

if __name__ == "__main__":
    host = '192.168.1.131'
    port = 55001
    db_name = 'postgres'
    db_schema = 'public'
    db_user = 'postgres'
    db_password = 'q1w2e3r4'

    atlas = AtlasPublisher(host='http://192.168.1.131:21000', password='admin')
    rdbms_publisher = RDBMSPublisher(atlas_publisher=atlas)

    rdbms_host = RDBMSHost(host=host, port=port, db_name=db_name, db_schema=db_schema, db_user=db_user,
                           db_password=db_password)

    RDBMSPipeline.sync_full_db(engine_name=DBEngine.postgresql.name, rdbms_publisher=rdbms_publisher, rdbms_host=rdbms_host)
