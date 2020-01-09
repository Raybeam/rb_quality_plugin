from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from base_data_quality_operator import BaseDataQualityOperator, get_result

class DataQualityThresholdSQLCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdSQLCheckOperator inherits from DataQualityThresholdCheckOperator.
    This operator will first calculate the min and max threshold values with given sql
    statements from a defined source, evaluate the data quality check, and then compare
    that result to the min and max thresholds calculated.

    :param min_threshold_sql: lower bound sql statement
    :type min_threshold_sql: str
    :param max_threshold_sql: upper bound sql statement
    :type max_threshold_sql: str
    :param threshold_conn_type: connection type of threshold sql statement table
    :type threshold_conn_type: str
    :param threshold_conn_id: connection id of threshold sql statement table
    :type threshold_conn_id: str
    """

    @apply_defaults
    def __init__(self,
                 min_threshold_sql,
                 max_threshold_sql,
                 threshold_conn_type,
                 threshold_conn_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.min_threshold_sql = min_threshold_sql
        self.max_threshold_sql = max_threshold_sql
        self.threshold_conn_type = threshold_conn_type
        self.threshold_conn_id = threshold_conn_id

    @property
    def threshold_conn_type(self):
        return self._threshold_conn_type

    @threshold_conn_type.setter
    def threshold_conn_type(self, conn):
        conn_types = {"postgres", "mysql", "hive"}
        if conn not in conn_types:
            raise ValueError(f"""Connection type of "{conn}" not currently supported""")
        self._threshold_conn_type = conn

    def execute(self, context):
        self.min_threshold = get_result(self.threshold_conn_type, self.threshold_conn_id, self.min_threshold_sql)
        self.max_threshold = get_result(self.threshold_conn_type, self.threshold_conn_id, self.max_threshold_sql)

        result = get_result(self.conn_type, self.conn_id, self.sql)
        info_dict = {
            "result" : result,
            "description" : self.check_description,
            "task_id" : self.task_id,
            "execution_date" : context.get("execution_date"),
            "min_threshold" : self.min_threshold,
            "max_threshold" : self.max_threshold
        }

        if self.min_threshold <= result <= self.max_threshold:
            info_dict["within_threshold"] = True
        else:
            info_dict["within_threshold"] = False
        self.push(info_dict)
        return info_dict

class DataQualityThresholdSQLCheckPlugin(AirflowPlugin):
    name = "data_quality_threshold_sql_check_operator"
    operators = [DataQualityThresholdSQLCheckOperator]