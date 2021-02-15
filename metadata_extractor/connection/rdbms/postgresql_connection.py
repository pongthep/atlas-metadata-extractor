# refer: https://kb.objectrocket.com/postgresql/postgres-list-tables-with-python-1023

from metadata_extractor.connection.connection_abstract import RDBMSConnection
import psycopg2


class PostgresqlConnection(RDBMSConnection):
    def __init__(self):
        super(PostgresqlConnection, self).__init__()

    def create_conn(self, host="", port=5432, db_name="", user="", password="", **kwargs: {}):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=user, password=password)

    def get_conn(self):
        return self.conn

    def close_connection(self):
        self.conn.close()
