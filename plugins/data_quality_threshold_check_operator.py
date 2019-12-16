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
                 min_threshold,
                 max_threshold,
                 eval_threshold=False,
                 threshold_conn_type=None,
                 threshold_conn_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.eval_threshold = eval_threshold
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.threshold_conn_type = threshold_conn_type
        self.threshold_conn_id = threshold_conn_id

    @property
    def threshold_conn_type(self):
        return self._threshold_conn_type

    @property
    def min_threshold(self):
        return self._min_threshold

    @property
    def max_threshold(self):
        return self._max_threshold

    @threshold_conn_type.setter
    def threshold_conn_type(self, conn):
        if self.eval_threshold:
            conn_types = {"postgres", "mysql", "hive"}
            if conn not in conn_types:
                raise ValueError(f"""Connection type "{conn}" is not supported""")
        self._threshold_conn_type = conn

    @min_threshold.setter
    def min_threshold(self, sql):
        if self.eval_threshold:
            keywords = {'INSERT INTO', 'UPDATE', 'DROP', 'ALTER'}
            for kw in keywords:
                if kw in sql.upper():
                    raise Exception(f"""Cannot use keyword {kw} in a sql query""")
        self._min_threshold = sql

    @max_threshold.setter
    def max_threshold(self, sql):
        if self.eval_threshold:
            keywords = {'INSERT INTO', 'UPDATE', 'DROP', 'ALTER'}
            for kw in keywords:
                if kw in sql.upper():
                    raise Exception(f"""Cannot use keyword {kw} in a sql query""")
        self._max_threshold = sql

    def execute(self, context):
        info_dict = super().execute(context=context)
        result = info_dict['result']

        if self.eval_threshold:
            upper_threshold = self.get_result(self.threshold_conn_type, self.threshold_conn_id, self.max_threshold)
            lower_threshold = self.get_result(self.threshold_conn_type, self.threshold_conn_id, self.min_threshold)
        else:
            upper_threshold = self.max_threshold
            lower_threshold = self.min_threshold
        if lower_threshold < result < upper_threshold:
            info_dict['within_threshold'] = True
        else:
            info_dict['within_threshold'] = False

        return info_dict


class DataQualityThresholdCheckPlugin(AirflowPlugin):
    name = 'data_quality_threshold_check_operator'
    operators = [DataQualityThresholdCheckOperator]
