from metadata_extractor.connection.connection_abstract import RDBMSConnection
import mysql.connector


class MysqlConnection(RDBMSConnection):
    def __init__(self):
        super(MysqlConnection, self).__init__()

    def create_conn(self, host="", port=3306, db_name="", user="", password="", **kwargs: {}):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.conn = mysql.connector.connect(host=self.host, port=self.port, database=self.db_name, user=user,
                                            password=password)

    def get_conn(self):
        return self.conn

    def close_connection(self):
        self.conn.close()
