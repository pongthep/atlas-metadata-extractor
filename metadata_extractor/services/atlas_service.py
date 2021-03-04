import requests
import time


class AtlasService:
    def __init__(self, host: str = 'http://localhost:21000', user: str = 'admin', password: str = 'admin'):
        # TODO Change to env var
        self.host: str = host
        self.user: str = user
        self.password: str = password

    def publish_entity(self, json: dict):
        url = '{host}/api/atlas/v2/entity'.format(host=self.host)
        x = requests.post(url, json=json, headers={"Content-Type": "application/json"}, auth=(self.user, self.password))
        if x.status_code != 200:
            exception_msg = 'sending fail with detail : {0}'.format(x.json())
            raise Exception(exception_msg)
        return x.json()

    def search_entity(self, type_name: str, qualified_name: str):
        url = '{host}/api/atlas/v2/search/basic'.format(host=self.host)
        query_string = {'typeName': type_name, 'query': qualified_name}
        x = requests.get(url, params=query_string, headers={"Content-Type": "application/json"},
                         auth=(self.user, self.password))
        if x.status_code != 200:
            exception_msg = 'sending fail with detail : {0}'.format(x)
            raise Exception(exception_msg)
        return x.json()

    def get_entity(self, guid: str):
        url = '{host}/api/atlas/v2/entity/bulk'.format(host=self.host)
        query_string = {'guid': guid}
        x = requests.get(url, params=query_string, headers={"Content-Type": "application/json"},
                         auth=(self.user, self.password))
        if x.status_code != 200:
            exception_msg = 'sending fail with detail : {0}'.format(x)
            raise Exception(exception_msg)
        return x.json()

    def is_tag_exist(self, tag_name: str) -> bool:
        url = '{host}/api/atlas/v2/types/classificationdef/name/{tag_name}'.format(host=self.host, tag_name=tag_name)
        x = requests.get(url, headers={"Content-Type": "application/json"},
                         auth=(self.user, self.password))
        if x.status_code not in [200, 404]:
            exception_msg = 'sending fail with detail : {0}'.format(x)
            raise Exception(exception_msg)

        if x.status_code == 200:
            return True
        else:
            return False

    def create_tag(self, tag_name: str):
        tag_json = {
            "enumDefs": [],
            "structDefs": [],
            "classificationDefs": [
                {
                    "category": "CLASSIFICATION",
                    "createdBy": "data_engineer",
                    "updatedBy": "data_engineer",
                    "updateTime": int(time.time()),
                    "version": 1,
                    "name": tag_name,
                    "description": "",
                    "typeVersion": "1.0",
                    "attributeDefs": [],
                    "superTypes": [],
                    "entityTypes": [],
                    "subTypes": []
                }
            ],
            "entityDefs": [
            ],
            "relationshipDefs": []
        }

        url = '{host}/api/atlas/v2/types/typedefs'.format(host=self.host)
        x = requests.post(url, json=tag_json, headers={"Content-Type": "application/json"},
                          auth=(self.user, self.password))
        if x.status_code != 200:
            exception_msg = 'sending fail with detail : {0}'.format(x.json())
            raise Exception(exception_msg)

    def get_entity_tag(self, table_id: str):
        url = '{host}/api/atlas/v2/entity/guid/{table_id}/classifications'.format(host=self.host, table_id=table_id)
        x = requests.get(url, headers={"Content-Type": "application/json"},
                         auth=(self.user, self.password))
        if x.status_code != 200:
            exception_msg = 'sending fail with detail : {0}'.format(x)
            raise Exception(exception_msg)
        return x.json()

    def set_entity_tag(self, tag_name: str, table_id: str):
        tag_settting_json = {
            "classification": {
                "entityStatus": "ACTIVE",
                "typeName": tag_name
            },
            "entityGuids": [table_id]
        }

        url = '{host}/api/atlas/v2/entity/bulk/classification'.format(host=self.host)
        x = requests.post(url, json=tag_settting_json, headers={"Content-Type": "application/json"},
                          auth=(self.user, self.password))
        if x.status_code not in [200, 204]:
            exception_msg = 'sending fail with detail : {0}'.format(x.json())
            raise Exception(exception_msg)
