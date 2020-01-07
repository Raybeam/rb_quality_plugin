import logging

from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook

class DataQualityThresholdCheckOperator(BaseOperator):
    """
    DataQualityThresholdCheckOperator builds off BaseOperator and
    executes a data quality check against high & low threshold values.

    :param sql: sql code to be executed
    :type sql: str
    :param conn_type: database type
    :type conn_type: str
    :param conn_id: connection id of database
    :type conn_id: str
    :param min_threshold: lower-bound value
    :type min_threshold: numeric
    :param max_threshold: upper-bound value
    :type max_threshold: numeric
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
                 min_threshold=None,
                 max_threshold=None,
                 push_conn_type=None,
                 push_conn_id=None,
                 check_description=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.push_conn_id = push_conn_id
        self.push_conn_type = push_conn_type
        self.check_description = check_description

    @property
    def conn_type(self):
        return self._conn_type

    @conn_type.setter
    def conn_type(self, conn):
        conn_types = {"postgres", "mysql", "hive"}
        if conn not in conn_types:
            raise ValueError(f"""Connection type of "{conn}" not currently supported""")
        self._conn_type = conn

    def execute(self, context):
        result = get_result(self.conn_type, self.conn_id, self.sql)
        info_dict = {
            "result" : result,
            "description" : self.check_description,
            "task_id" : self.task_id,
            "execution_date" : context.get("execution_date"),
            "min_threshold" : self.min_threshold,
            "max_threshold" : self.max_threshold
        }

        if self.min_threshold < result < self.max_threshold:
            info_dict["within_threshold"] = True
        else:
            info_dict["within_threshold"] = False
        self.push(info_dict)
        return info_dict

    def push(self, info_dict):
        """Send data check info and metadata to an external database."""
        raise NotImplementedError()


class DataQualityThresholdCheckPlugin(AirflowPlugin):
    name = "data_quality_threshold_check_operator"
    operators = [DataQualityThresholdCheckOperator]

def _get_hook(conn_type, conn_id):
    if conn_type == "postgres":
        return PostgresHook(postgres_conn_id=conn_id)
    if conn_type == "mysql":
        return MySqlHook(mysql_conn_id=conn_id)
    if conn_type == "hive":
        return HiveServer2Hook(hiveserver2_conn_id=conn_id)

def get_result(conn_type, conn_id, sql):
    """
    Returns result of SQL data quality check. SQL must return
    a single value and single column result.
    """
    hook = _get_hook(conn_type, conn_id)
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
