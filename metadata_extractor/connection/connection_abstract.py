from abc import ABC, abstractmethod


class RDBMSConnection(ABC):
    def __init__(self):
        self.conn = None
        self.host: str = ''
        self.port: int = 1111
        self.db_name: str = ''

    @abstractmethod
    def create_conn(self, host: str = '', port: int = 5432, db_name: str = '', user: str = '',
                    password: str = '', **kwargs: {}):
        pass

    @abstractmethod
    def get_conn(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass