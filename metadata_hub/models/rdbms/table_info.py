from typing import List
from metadata_hub.models.rdbms.column_info import ColumnInfo


class TableInfo:
    def __init__(self):
        self.name: str = "undefined"
        self.columns: List[ColumnInfo] = []
