class RDBMSHost:
    def __init__(self, host: str, port: int, db_name: str, db_schema: str, db_user: str, db_password: str):
        self.host: str = host
        self.port: int = port
        self.db_name: str = db_name
        self.db_schema: str = db_schema
        self.db_user: str = db_user
        self.db_password: str = db_password
