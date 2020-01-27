from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from base_data_quality_operator import BaseDataQualityOperator, get_sql_value

class DataQualityThresholdSQLCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdSQLCheckOperator inherits from DataQualityThresholdCheckOperator.
    This operator will first calculate the min and max threshold values with given sql
    statements from a defined source, evaluate the data quality check, and then compare
    that result to the min and max thresholds calculated.

    :param min_threshold_sql: lower bound sql statement (or path to sql statement)
    :type min_threshold_sql: str
    :param max_threshold_sql: upper bound sql statement (or path to sql statement)
    :type max_threshold_sql: str
    :param threshold_conn_type: connection type of threshold sql statement table
    :type threshold_conn_type: str
    :param threshold_conn_id: connection id of threshold sql statement table
    :type threshold_conn_id: str
    """

    template_fields = ('sql','min_threshold_sql', 'max_threshold_sql')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 min_threshold_sql,
                 max_threshold_sql,
                 threshold_conn_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.min_threshold_sql = min_threshold_sql
        self.max_threshold_sql = max_threshold_sql
        self.threshold_conn_id = threshold_conn_id

    def execute(self, context):
        self.min_threshold = get_sql_value(self.threshold_conn_id, self.min_threshold_sql)
        self.max_threshold = get_sql_value(self.threshold_conn_id, self.max_threshold_sql)

        result = get_sql_value(self.conn_id, self.sql)
        info_dict = {
            "result" : result,
            "description" : self.check_description,
            "task_id" : self.task_id,
            "execution_date" : context.get("execution_date"),
            "min_threshold" : self.min_threshold,
            "max_threshold" : self.max_threshold,
            "within_threshold" : self.min_threshold <= result <= self.max_threshold
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key=f"""result data from task {self.task_id}""", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict

class DataQualityThresholdSQLCheckPlugin(AirflowPlugin):
    name = "data_quality_threshold_sql_check_operator"
    operators = [DataQualityThresholdSQLCheckOperator]
