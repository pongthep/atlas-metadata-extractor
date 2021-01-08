from abc import ABC, abstractmethod


class RDBMSConnectionAbstract(ABC):
    def __init__(self, conn=None, host: str = '', port: int = 5432, db_name: str = '', user: str = '',
                 password: str = '', **kwargs: {}):
        self.conn = conn
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user

    @abstractmethod
    def get_conn(self):
        pass
