class RDBMSInstance:
    def __init__(self, host: str, port: int, rdbms_type: str):
        self.host: str = host
        self.port: int = port
        self.rdbms_type: str = rdbms_type
        self.qualified_name: str = host
