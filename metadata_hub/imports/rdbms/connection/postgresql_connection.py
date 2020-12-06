from metadata_hub.imports.rdbms.connection.connection_abstract import DBConnection
import psycopg2


class PostgreSQLConnection(DBConnection):
    def __init__(self, host="", port=5432, db_name="", user="", password=""):
        self.conn = psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)

    def get_conn(self):
        return self.conn
