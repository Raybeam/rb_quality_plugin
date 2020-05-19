import logging

from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class BaseDataQualityOperator(BaseOperator):
    """
    BaseDataQualityOperator is an abstract base operator class to
    perform data quality checks

    :param sql: sql (or path to sql) code to be executed
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

    template_fields = ['sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(self,
                 sql,
                 conn_id,
                 push_conn_id=None,
                 check_description=None,
                 use_legacy_sql=True,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.push_conn_id = push_conn_id
        self.sql = sql
        self.check_description = check_description
        self.use_legacy_sql = use_legacy_sql

    def execute(self, context):
        """Method where data quality check is performed """
        raise NotImplementedError

    def push(self, info_dict):
        """
        Optional: Send data check info and metadata to an external database.
        Default functionality will log metadata.
        """
        info = "\n".join([f"""{key}: {item}""" for key, item in info_dict.items()])
        log.info("Log from %s:\n%s", self.dag_id, info)

    def send_failure_notification(self, info_dict):
        """
        send_failure_notification will throw an AirflowException with logging
        information and dq check results from the failed task that was just run.
        """
        body = """
            Data Quality Check: "{task_id}" failed.
            DAG: {dag_id}
            Task_id: {task_id}
            Check description: {description}
            Execution date: {execution_date}
            SQL: {sql}
            Result: {result} is not within thresholds {min_threshold} and {max_threshold}
        """.format(
            task_id=self.task_id, dag_id=self.dag_id,
            description=info_dict.get("description"), sql=self.sql,
            execution_date=str(info_dict.get("execution_date")),
            result=round(info_dict.get("result"), 2),
            min_threshold=info_dict.get("min_threshold"),
            max_threshold=info_dict.get("max_threshold")
        )
        raise AirflowException(body)

    def get_sql_value(self, conn_id, sql):
        """
        get_sql_value executes a sql query given proper connection parameters.
        The result of the sql query should be one and only one numeric value.
        """

        conn = BaseHook.get_connection(conn_id)
        if conn.conn_type == 'google_cloud_platform':
            hook = BigQueryHook(conn_id, use_legacy_sql=self.use_legacy_sql)
        else:
            hook = BaseHook.get_hook(conn_id)
        result = hook.get_records(sql)
        if len(result) > 1:
            logging.info("Result: %s contains more than 1 entry", str(result))
            raise ValueError("Result from sql query contains more than 1 entry")
        elif len(result) < 1:
            raise ValueError("No result returned from sql query")
        elif len(result[0]) != 1:
            logging.info("Result: %s does not contain exactly 1 column", str(result[0]))
            raise ValueError("Result from sql query does not contain exactly 1 column")
        else:
            return result[0][0]
