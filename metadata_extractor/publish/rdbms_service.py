from metadata_extractor.models.atlas_model.rdbms.rdbms_instance import RDBMSInstance
from metadata_extractor.models.atlas_model.rdbms.database_info import Database
from metadata_extractor.models.atlas_model.rdbms.table_info import Table
from metadata_extractor.models.atlas_model.rdbms.column_info import Column
from metadata_extractor.services.atlas_service import AtlasService
import time
from metadata_extractor.models.atlas_model.rdbms.database_info import get_qualified_name as db_get_qualified_name
from metadata_extractor.models.atlas_model.rdbms.database_info import get_delimiter as db_get_delimiter


class RDBMSService:
    def __init__(self, atlas_service: AtlasService = None):
        if not atlas_service:
            self.__atlas = AtlasService()
        else:
            self.__atlas = atlas_service
        self.__default_create_by = 'metadata-extractor'

    def publish_instance(self, instance: RDBMSInstance, db_name: str):
        instance_json = {
            "entity": {
                "typeName": "rdbms_instance",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": instance.qualified_name,
                    "createTime": int(time.time()),
                    "name": instance.host,
                    "rdbms_type": instance.rdbms_type,
                    "hostname": instance.host,
                    "port": str(instance.port)
                },
                "status": "ACTIVE",
                "createdBy": self.__default_create_by,
                "updatedBy": self.__default_create_by,
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
                "relationshipAttributes": {
                }
            }
        }

        self.__atlas.publish_entity(instance_json)

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
                "createdBy": self.__default_create_by,
                "updatedBy": self.__default_create_by,
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

        self.__atlas.publish_entity(db_json)

    def publish_table(self, db_qualified_name: str, table: Table):
        table_json = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": table.qualified_name,
                    "description": table.desc,
                    "createTime": int(time.time()),
                    "name": table.name
                },
                "status": "ACTIVE",
                "createdBy": self.__default_create_by,
                "updatedBy": self.__default_create_by,
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

        self.__atlas.publish_entity(table_json)

    def publish_column(self, table_qualified_name: str, column: Column):
        column_json = {
            "entity": {
                "typeName": "rdbms_column",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": column.qualified_name,
                    "description": column.desc,
                    "createTime": int(time.time()),
                    "name": column.name,
                    "data_type": column.data_type
                },
                "status": "ACTIVE",
                "createdBy": self.__default_create_by,
                "updatedBy": self.__default_create_by,
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

        self.__atlas.publish_entity(column_json)

    def get_table_meta_data_missing(self, host_name: str = None
                                    , database_name: str = None):

        qn = db_get_qualified_name(host=host_name, db_name=database_name)
        search_result = self.__atlas.search_entity('rdbms_db', qn)
        entity_founds: dict = search_result.get('entities', [])
        table_dict = {}
        data_missing_dict = {}

        if entity_founds is None or len(entity_founds) == 0:
            raise Exception('Not found any entity')

        for entity in entity_founds:
            if qn == entity.get('attributes').get('qualifiedName'):
                db_id = entity.get('guid')
                entity_info = self.__atlas.get_entity(db_id)
                db_refer_entity: dict = entity_info.get('referredEntities', {})
                for entity_id in db_refer_entity:
                    refer_entity_info = db_refer_entity.get(entity_id)
                    if refer_entity_info.get('typeName') == 'rdbms_table':
                        col_name = refer_entity_info.get('attributes').get('name')
                        table_dict.update({col_name: entity_id})

            missing_tables = []
            for table_name in table_dict:
                table_id = table_dict.get(table_name)

                table_entity_info = self.__atlas.get_entity(table_id).get('entities')[0]
                owner = table_entity_info.get('attributes').get('owner')
                description = table_entity_info.get('attributes').get('description')
                tags = table_entity_info.get('classifications', [])

                if not owner or not description or len(tags) == 0:
                    missing_tables.append(table_name)
            data_missing_dict[qn] = missing_tables

        return data_missing_dict

    def update_table_owner(self, table_qn: str
                           , table_name: str
                           , db_id: str
                           , table_owner: str
                           ):

        table_json = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": table_qn,
                    "owner": table_owner,
                    "name": table_name,
                    "db": {
                        "guid": db_id,
                        "typeName": "rdbms_db"
                    }
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
            }
        }

        self.__atlas.publish_entity(table_json)

    def update_table_desc(self, table_qn: str
                          , table_name: str
                          , db_id: str
                          , table_desc: str
                          ):

        table_json = {
            "entity": {
                "typeName": "rdbms_table",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": table_qn,
                    "description": table_desc,
                    "name": table_name,
                    "db": {
                        "guid": db_id,
                        "typeName": "rdbms_db"
                    }
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "createTime": int(time.time()),
                "updateTime": int(time.time()),
            }
        }

        self.__atlas.publish_entity(table_json)

    def update_column_desc(self, column_qn: str
                           , column_name: str
                           , table_id: str
                           , column_desc: str
                           ):
        if not column_desc:
            column_desc = ""

        col_json = {
            "entity": {
                "typeName": "rdbms_column",
                "attributes": {
                    "modifiedTime": int(time.time()),
                    "qualifiedName": column_qn,
                    "description": column_desc,
                    "name": column_name,
                    "table": {
                        "guid": table_id,
                        "typeName": "rdbms_table"
                    }
                },
                "status": "ACTIVE",
                "createdBy": "data_engineer",
                "updatedBy": "data_engineer",
                "updateTime": int(time.time()),
            }
        }
        self.__atlas.publish_entity(col_json)

    def publish_table_update(self, database_name: str = None
                             , database_schema: str = None
                             , table_name: str = None
                             , table_owner: str = None
                             , table_desc: str = None
                             , table_tags: str = None
                             , column_desc: dict = None):
        qn = '{db_schema}.{table_name}'.format(db_schema=database_schema, table_name=table_name)
        entity_founds: dict = self.__atlas.search_entity('rdbms_table', qn).get('entities', [])
        table_guid = None
        db_guid = None
        table_refer_entity = {}
        for entity in entity_founds:
            if qn == entity.get('attributes').get('qualifiedName'):
                table_id = entity.get('guid')
                entity_info = self.__atlas.get_entity(table_id)
                db_id = entity_info.get('entities')[0].get('attributes').get('db').get('guid')
                db_info = self.__atlas.get_entity(db_id)
                db_host, db_name = db_info.get('entities')[0].get('attributes') \
                    .get('qualifiedName').split(db_get_delimiter())
                if db_name == database_name:
                    db_guid = db_id
                    table_guid = table_id
                    table_refer_entity: dict = entity_info.get('referredEntities', {})

        if table_guid:
            if table_owner:
                self.update_table_owner(table_qn=qn, table_name=table_name, db_id=db_guid, table_owner=table_owner)

            if table_desc:
                self.update_table_desc(table_qn=qn, table_name=table_name, db_id=db_guid,
                                       table_desc=table_desc)

            if table_tags:
                exist_table_tag_info = self.__atlas.get_entity_tag(table_id=table_guid)
                table_tag_list = []
                exist_tag_list = exist_table_tag_info.get("list")

                for exist_tag in exist_tag_list:
                    table_tag_list.append(exist_tag.get("typeName"))

                for tag in table_tags.split(','):
                    tag_name = str(tag).strip()
                    if tag_name not in table_tag_list and tag_name != '':
                        if not self.__atlas.is_tag_exist(tag_name=tag_name):
                            self.__atlas.create_tag(tag_name=tag_name)
                        self.__atlas.set_entity_tag(tag_name=tag_name, table_id=table_guid)

            if column_desc:
                column_dict: dict = {}

                for entity_id in table_refer_entity:
                    refer_entity_info = table_refer_entity.get(entity_id)
                    if refer_entity_info.get('typeName') == 'rdbms_column':
                        col_name = refer_entity_info.get('attributes').get('name')
                        column_dict.update({col_name: entity_id})

                for col_name in column_desc:
                    col_qn = '{table_qn}_column={col_name}'.format(table_qn=qn, col_name=col_name)
                    col_desc = column_desc.get(col_name)

                    self.update_column_desc(column_qn=col_qn, column_name=col_name, table_id=table_guid,
                                            column_desc=col_desc)
