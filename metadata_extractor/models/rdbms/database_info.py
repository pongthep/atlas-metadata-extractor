from metadata_extractor.models.server_instance.rdbms_instance import RDBMSInstance


class Database:
    def __init__(self, name: str = '', instance: RDBMSInstance = None, tags: list = []):
        self.name: str = name
        self.instance: RDBMSInstance = instance
        self.tags: list = tags

        self.qualified_name: str = '{instance}.{db}'.format(instance=instance.host, db=name)
