from connection.connection_abstract import RDBMSConnectionAbstract
import mysql.connector


class MySQLConnection(RDBMSConnectionAbstract):
    def __init__(self, host="", port=3306, db_name="", user="", password="", **kwargs: {}):
        super(MySQLConnection, self).__init__()
        self.__create_conn(host, port, db_name, user, password)

    def __create_conn(self, host="", port=3306, db_name="", user="", password=""):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.conn = mysql.connector.connect(host=self.host, port=self.port, database=self.db_name, user=user,
                                            password=password)

    def get_conn(self):
        return self.conn

    def close_connection(self):
        self.conn.close()
