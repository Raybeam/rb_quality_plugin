import logging

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook

class BaseDataQualityOperator(BaseOperator):
    """
    Base operator that executes a data quality sql statement,
    and optionally pushes results and metadata to an external
    database for storage.
    """

    @apply_defaults
    def __init__(self,
                 sql,
                 conn_id,
                 conn_type,
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

    @staticmethod
    def _get_hook(conn_type, conn_id):
        if conn_type == "postgres":
            return PostgresHook(postgres_conn_id=conn_id)
        if conn_type == "mysql":
            return MySqlHook(mysql_conn_id=conn_id)
        if conn_type == "hive":
            return HiveServer2Hook(hiveserver2_conn_id=conn_id)

        raise ValueError(f"""Connection type of {conn_type} not currently supported""")

    @staticmethod
    def get_result(conn_type, conn_id, sql):
        hook = BaseDataQualityOperator._get_hook(conn_type, conn_id)
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
