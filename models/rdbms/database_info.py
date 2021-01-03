from models.server_instance.rdbms_instance import RDBMSInstance


class Database:
    def __init__(self, name: str, instance: RDBMSInstance):
        self.name: str = name
        self.instance: RDBMSInstance = instance
        self.qualified_name: str = '{instance}_db={db}'.format(instance=instance.qualified_name, db=name)
