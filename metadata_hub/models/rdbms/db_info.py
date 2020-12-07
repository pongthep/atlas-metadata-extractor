from metadata_hub.enum.rdbms.database_enum import DatabaseEngine
from metadata_hub.models.rdbms.table_info import TableInfo
from typing import List


class DatabaseInfo:
    def __init__(self):
        self.name: str = "undefined"
        self.engine: DatabaseEngine = DatabaseEngine.Undefined
        self.host: str = "localhost"
        self.port: int = 5555
        self.tables: List[TableInfo] = []
