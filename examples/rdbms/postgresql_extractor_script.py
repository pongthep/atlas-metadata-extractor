from metadata_extractor.services.atlas_service import AtlasService
from metadata_extractor.services.rdbms_service import RDBMSService
from metadata_extractor.pipeline.rdbms_pipeline import RDBMSPipeline
from metadata_extractor.models.hosts.rdbms_host import RDBMSHost
from metadata_extractor.models.enum.db_engine_enum import DBEngine

if __name__ == "__main__":
    host = ''
    port = 5432
    db_name = ''
    db_schema = ''
    db_user = ''
    db_password = ''

    atlas = AtlasService(host='http://localhost:21000', password='')
    rdbms_service = RDBMSService(atlas_service=atlas)

    rdbms_host = RDBMSHost(host=host, port=port, db_name=db_name, db_schema=db_schema, db_user=db_user,
                           db_password=db_password)

    RDBMSPipeline.sync_full_db(image_name=DBEngine.postgresql.name, rdbms_service=rdbms_service, rdbms_host=rdbms_host)
