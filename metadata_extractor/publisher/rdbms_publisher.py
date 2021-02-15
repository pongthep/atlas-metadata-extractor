from metadata_extractor.models.server_instance.rdbms_instance import RDBMSInstance
from metadata_extractor.models.rdbms.database_info import Database
from metadata_extractor.models.rdbms.table_info import Table
from metadata_extractor.models.rdbms.column_info import Column
from metadata_extractor.publisher.atlas_publisher import AtlasPublisher
import time


class RDBMSPublisher:
    def __init__(self):
        self.__publisher = AtlasPublisher()

    def publish_instance(self, instance: RDBMSInstance):
        instance_json = {
            "entity": {
                "typeName": "rdbms_instance",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": instance.qualified_name,
                    "description": "",
                    "createTime": int(time.time()),
                    "name": instance.host,
                    "rdbms_type": instance.rdbms_type,
                    "hostname": instance.host,
                    "port": str(instance.port)
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
                "relationshipAttributes": {
                }
            }
        }

        self.__publisher.publish_entity(instance_json)

    def publish_database(self, instance_qualified_name: str, db: Database):
        db_json = {
            "entity": {
                "typeName": "rdbms_db",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": db.qualified_name,
                    "description": "",
                    "createTime": int(time.time()),
                    "name": db.name
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
                "relationshipAttributes": {
                    "instance": {
                        "uniqueAttributes": {"qualifiedName": instance_qualified_name},
                        "typeName": "rdbms_instance"
                    }
                }
            }
        }

        self.__publisher.publish_entity(db_json)

    def publish_table(self, db_qualified_name: str, table: Table):
        table_json = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": table.qualified_name,
                    "description": "",
                    "createTime": int(time.time()),
                    "name": table.name
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
                "relationshipAttributes": {
                    "db": {
                        "uniqueAttributes": {"qualifiedName": db_qualified_name},
                        "typeName": "rdbms_db"
                    }
                }
            }
        }

        self.__publisher.publish_entity(table_json)

    def publish_column(self, table_qualified_name: str, column: Column):
        column_json = {
            "entity": {
                "typeName": "rdbms_column",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": column.qualified_name,
                    "description": "",
                    "createTime": int(time.time()),
                    "name": column.name,
                    "data_type": column.datatype
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
                "relationshipAttributes": {
                    "table": {
                        "uniqueAttributes": {"qualifiedName": table_qualified_name},
                        "typeName": "rdbms_table"
                    }
                }
            }
        }

        self.__publisher.publish_entity(column_json)
