from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from base_data_quality_operator import BaseDataQualityOperator

class DataQualityThresholdCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdCheckOperator builds off BaseDataQualityOperator and
    executes a data quality check against input threshold values. If thresholds
    are sql queries to evaluate a value, the operator will evaluate the queries
    and compare the data quality check result with these threshold values.

    :param min_threshold: lower bound value or threshold sql statement to be executed
    :type min_threshold: numeric or str
    :param max_threshold: lower bound value or threshold sql statement to be executed
    :type max_threshold: numeric or str
    :param eval_threshold: boolean that flags whether max/min threshold need to be executed
    :type eval_threshold: bool
    :param threshold_conn_type: connection type for threshold sql
    :type threshold_conn_type: str
    :param threshold_conn_id: connection id for threshold sql
    :type threshold_conn_id: str
    """

    @apply_defaults
    def __init__(self,
                 min_threshold=None,
                 max_threshold=None,
                 min_threshold_sql=None,
                 max_threshold_sql=None,
                 eval_threshold=False,
                 threshold_conn_type=None,
                 threshold_conn_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.eval_threshold = eval_threshold
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.min_threshold_sql = min_threshold_sql
        self.max_threshold_sql = max_threshold_sql
        self.threshold_conn_type = threshold_conn_type
        self.threshold_conn_id = threshold_conn_id

    @property
    def threshold_conn_type(self):
        return self._threshold_conn_type

    @property
    def min_threshold_sql(self):
        return self._min_threshold_sql

    @property
    def max_threshold_sql(self):
        return self._max_threshold_sql

    @threshold_conn_type.setter
    def threshold_conn_type(self, conn):
        if self.eval_threshold:
            conn_types = {"postgres", "mysql", "hive"}
            if conn not in conn_types:
                raise ValueError(f"""Connection type "{conn}" is not supported""")
        self._threshold_conn_type = conn

    @min_threshold_sql.setter
    def min_threshold_sql(self, sql):
        if self.eval_threshold:
            keywords = {'INSERT INTO', 'UPDATE', 'DROP', 'ALTER'}
            for kw in keywords:
                if kw in sql.upper():
                    raise Exception(f"""Cannot use keyword {kw} in a sql query""")
        self._min_threshold_sql = sql

    @max_threshold_sql.setter
    def max_threshold_sql(self, sql):
        if self.eval_threshold:
            keywords = {'INSERT INTO', 'UPDATE', 'DROP', 'ALTER'}
            for kw in keywords:
                if kw in sql.upper():
                    raise Exception(f"""Cannot use keyword {kw} in a sql query""")
        self._max_threshold_sql = sql

    def execute(self, context):
        result = self.get_result(self.conn_type, self.conn_id, self.sql)
        info_dict = {
            "result" : result,
            "description" : self.check_description,
            "task_id" : self.task_id,
            "execution_date" : context.get("execution_date")
        }

        if self.eval_threshold:
            self.min_threshold = self.get_result(self.threshold_conn_type, self.threshold_conn_id, self.min_threshold_sql)
            self.max_threshold = self.get_result(self.threshold_conn_type, self.threshold_conn_id, self.max_threshold_sql)
        if self.min_threshold < result < self.max_threshold:
            info_dict['within_threshold'] = True
        else:
            info_dict['within_threshold'] = False
        info_dict['min_threshold'], info_dict['max_threshold'] = self.min_threshold, self.max_threshold
        return info_dict


class DataQualityThresholdCheckPlugin(AirflowPlugin):
    name = 'data_quality_threshold_check_operator'
    operators = [DataQualityThresholdCheckOperator]
