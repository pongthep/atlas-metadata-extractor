# refer: https://kb.objectrocket.com/postgresql/postgres-list-tables-with-python-1023

from metadata_hub.connection.connection_abstract import RDBMSConnection
import psycopg2


class PostgreSQLConnection(RDBMSConnection):
    def __init__(self, host="", port=5432, db_name="", user="", password=""):
        super(PostgreSQLConnection, self).__init__()
        self.__create_conn(host, port, db_name, user, password)

    def __create_conn(self, host="", port=5432, db_name="", user="", password=""):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=user, password=password)

    def get_conn(self):
        return self.conn

    def close_connection(self):
        self.conn.close()
