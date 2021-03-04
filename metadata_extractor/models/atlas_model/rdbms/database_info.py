from metadata_extractor.models.atlas_model.rdbms.rdbms_instance import RDBMSInstance


def get_delimiter():
    return '_db='


def get_qualified_name(host: str, db_name: str) -> str:
    return '{instance}{delimiter}{db}' \
        .format(instance=host, delimiter=get_delimiter(), db=db_name)


class Database:
    def __init__(self, name: str = '', instance: RDBMSInstance = None, tags: list = []):
        self.name: str = name
        self.instance: RDBMSInstance = instance
        self.tags: list = tags

        self.qualified_name: str = get_qualified_name(host=instance.host, db_name=name)
