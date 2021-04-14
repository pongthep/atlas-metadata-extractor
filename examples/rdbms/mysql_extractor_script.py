from metadata_extractor.publisher.atlas_publisher import AtlasPublisher
from metadata_extractor.publisher.rdbms_publisher import RDBMSPublisher
from metadata_extractor.pipeline.rdbms_pipeline import RDBMSPipeline
from metadata_extractor.models.hosts.rdbms_host import RDBMSHost
from metadata_extractor.models.enum.db_engine_enum import DBEngine

if __name__ == "__main__":
    host = ''
    port = 3306
    db_name = ''
    db_user = ''
    db_password = ''

    atlas = AtlasPublisher(host='http://localhost:21000', password='')
    rdbms_publisher = RDBMSPublisher(atlas_publisher=atlas)

    rdbms_host = RDBMSHost(host=host, port=port, db_name=db_name, db_schema=db_name, db_user=db_user,
                           db_password=db_password)

    RDBMSPipeline.sync_full_db(engine_name=DBEngine.mysql.name, rdbms_publisher=rdbms_publisher, rdbms_host=rdbms_host)
