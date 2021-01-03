from abc import ABC, abstractmethod


class RDBMSConnection(ABC):
    def __init__(self):
        self.conn = None
        self.host = ""
        self.port = 5432
        self.db_name = ""
        self.user = ""

    @abstractmethod
    def get_conn(self):
        pass
