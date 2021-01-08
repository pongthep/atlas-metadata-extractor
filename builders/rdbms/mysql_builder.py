from builders.rdbms.rdbms_builder_abstract import RDBMSBuilderAbstract


class MysqlBuilder(RDBMSBuilderAbstract):
    def __init__(self, rdbms_type: str = 'mysql'):
        self.rdbms_type = rdbms_type
