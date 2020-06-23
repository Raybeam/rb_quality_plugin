import yaml

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults

from rb_quality_plugin.operators.data_quality_threshold_operator
    import DataQualityThresholdOperator


class DataQualityThresholdSQLCheckOperator(DataQualityThresholdOperator):
    """
    DataQualityThresholdSQLCheckOperator inherits from
        DataQualityThresholdCheckOperator.
    This operator will first calculate the min and max threshold values with
        given sql statements from a defined source, evaluate the data quality
        check, and then compare that result to the min and max thresholds
        calculated.

    :param min_threshold_sql: lower bound sql statement (or path to sql)
    :type min_threshold_sql: str
    :param max_threshold_sql: upper bound sql statement (or path to sql)
    :type max_threshold_sql: str
    :param threshold_conn_type: connection type of threshold sql table
    :type threshold_conn_type: str
    :param threshold_conn_id: connection id of threshold sql table
    :type threshold_conn_id: str
    :param config_path: path to yaml configuration file
    :type config_path: str
    :param check_args: dq parameters for sql evaluation
    :type check_args: dict
    """

    template_fields = ['sql', 'min_threshold_sql', 'max_threshold_sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(self,
                 min_threshold_sql=None,
                 max_threshold_sql=None,
                 threshold_conn_id=None,
                 config_path=None,
                 check_args={},
                 *args,
                 **kwargs):
        self.dq_check_args = check_args

        defaults = {
            'min_threshold_sql': min_threshold_sql,
            'max_threshold_sql': max_threshold_sql,
            'threshold_conn_id': threshold_conn_id,
        }
        if config_path:
            kwargs, defaults = self.read_from_config(config_path, kwargs, defaults)

        self.min_threshold_sql = defaults['min_threshold_sql']
        self.max_threshold_sql = defaults['max_threshold_sql']
        self.threshold_conn_id = defaults['threshold_conn_id']

        if not (self.max_threshold_sql or self.min_threshold_sql):
            raise AirflowException(
                "At least a min or max threshold must be defined")

        super().__init__(*args, **kwargs)

    def execute(self, context):
        min_threshold = self.get_sql_value(
            self.threshold_conn_id,
            self.min_threshold_sql, self.dq_check_args)

        max_threshold = self.get_sql_value(
            self.threshold_conn_id,
            self.max_threshold_sql, self.dq_check_args)

        result = self.get_sql_value(
            self.conn_id, self.sql, self.dq_check_args)

        return self.alert(context, result, min_threshold, max_threshold)
