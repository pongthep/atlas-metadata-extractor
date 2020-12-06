from abc import ABC, abstractmethod


class DBConnection(ABC):
    @abstractmethod
    def get_conn(self):
        pass
