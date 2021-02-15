import requests


class AtlasPublisher:
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
