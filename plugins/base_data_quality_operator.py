import logging

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow import AirflowException

class BaseDataQualityOperator(BaseOperator):
    """
    BaseDataQualityOperator is an abstract base operator class to
    perform data quality checks

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
                 conn_id,
                 push_conn_id=None,
                 check_description=None,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.push_conn_id = push_conn_id
        self.sql = sql
        self.check_description = check_description

    def execute(self, context):
        """Method where data quality check is performed """
        raise NotImplementedError

    def push(self, info_dict):
        """Send data check info and metadata to an external database."""
        pass

    def send_failure_notification(self, info_dict):
        """
        send_failure_notification will throw an AirflowException with logging 
        information and dq check results from the failed task that was just run.
        """
        body = f"""Data Quality Check: "{info_dict.get("task_id")}" failed.
DAG: {self.dag_id}
Task_id: {info_dict.get("task_id")}
Check description: {info_dict.get("description")}
Execution date: {info_dict.get("execution_date")}
SQL: {self.sql}
Result: {round(info_dict.get("result"), 2)} is not within thresholds {info_dict.get("min_threshold")} and {info_dict.get("max_threshold")}"""
        raise AirflowException(body)

def _get_hook(conn_id):
    """
    _get_hook is a helper function for get_sql_value. Returns a database
    hook depending on the conn_type and conn_id specified. Method will raise
    an exception if hook is not supported.
    """

    conn_type = BaseHook.get_connection(conn_id).conn_type
    if conn_type == "postgres":
        return PostgresHook(postgres_conn_id=conn_id)
    if conn_type == "mysql":
        return MySqlHook(mysql_conn_id=conn_id)
    if conn_type == "hive":
        return HiveServer2Hook(hiveserver2_conn_id=conn_id)
    else:
        raise ValueError(f"""Connection type of "{conn_type}" not currently supported""")

def get_sql_value(conn_id, sql):
    """
    get_sql_value executes a sql query given proper connection parameters.
    The result of the sql query should be one and only one numeric value.
    """
    hook = _get_hook(conn_id)
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
