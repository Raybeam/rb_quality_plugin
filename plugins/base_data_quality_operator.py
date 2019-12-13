import logging

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook

class BaseDataQualityOperator(BaseOperator):
    """
    BaseDataQualityOperator executes a data quality sql statement,
    and optionally pushes results and metadata to an external
    database for storage.

    :param sql: sql code to be executed
    :type sql: str
    :param conn_type: database type
    :type conn_type: str
    :param conn_id: connection id of database
    :type conn_id: str
    :param push_conn_type: (optional) external database type
    :type push_conn_type: str
    :param push_conn_id: (optional) connection id of external database
    :type push_conn_id: str
    :param check_description: (optional) description of data quality sql statement
    :type check_description: str
    """

    @apply_defaults
    def __init__(self,
                 sql,
                 conn_type,
                 conn_id,
                 push_conn_type=None,
                 push_conn_id=None,
                 check_description=None,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.push_conn_id = push_conn_id
        self.push_conn_type = push_conn_type
        self.sql = sql
        self.check_description = check_description

    def execute(self, context):
        info_dict = { 'result': self.get_result(self.conn_type, self.conn_id, self.sql),
                      'description': self.check_description,
                      'task_id': self.task_id,
                      'execution_date': context['execution_date']
                      }
        self.push(info_dict)
        return info_dict

    @property
    def conn_type(self):
        return self._conn_type

    @conn_type.setter
    def conn_type(self, conn):
        conn_types = {'postgres', 'mysql', 'hive'}
        if conn not in conn_types:
            raise ValueError(f"""Connection type of "{conn}" not currently supported""")
        self._conn_type = conn

    def _get_hook(self, conn_type, conn_id):
        if conn_type == "postgres":
            return PostgresHook(postgres_conn_id=conn_id)
        if conn_type == "mysql":
            return MySqlHook(mysql_conn_id=conn_id)
        if conn_type == "hive":
            return HiveServer2Hook(hiveserver2_conn_id=conn_id)

    def get_result(self, conn_type, conn_id, sql):
        hook = self._get_hook(conn_type, conn_id)
        result = hook.get_records(sql)
        if len(result) > 1:
            logging.info("Result: %s contains more than 1 entry", str(result))
            raise ValueError("Result from sql query contains more than 1 entry")
        if len(result) < 1:
            raise ValueError("No result returned from sql query")
        if len(result[0]) != 1:
            logging.info("Result: %s does not contain exactly 1 column", str(result[0]))
            raise ValueError("Result from sql query does not contain exactly 1 column")
        return result[0][0]

    def push(self, info_dict):
        """Send data check info and metadata to an external database."""
        raise NotImplementedError()
