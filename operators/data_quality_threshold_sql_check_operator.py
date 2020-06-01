import yaml

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults

from rb_quality_plugin.operators.base_data_quality_operator\
    import BaseDataQualityOperator


class DataQualityThresholdSQLCheckOperator(BaseDataQualityOperator):
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

        if config_path:
            with open(config_path) as configs:
                dq_configs = yaml.safe_load(configs)

            for key in dq_configs:
                if key == 'min_threshold_sql' and not min_threshold_sql:
                    min_threshold_sql = dq_configs[key]
                elif key == 'max_threshold_sql' and not max_threshold_sql:
                    max_threshold_sql = dq_configs[key]
                elif key == 'threshold_conn_id' and not threshold_conn_id:
                    threshold_conn_id = dq_configs[key]
                elif key not in kwargs:
                    kwargs[key] = dq_configs[key]

        self.min_threshold_sql = min_threshold_sql
        self.max_threshold_sql = max_threshold_sql
        self.threshold_conn_id = threshold_conn_id

        if not (self.max_threshold_sql or self.min_threshold_sql):
            raise AirflowException(
                "At least a min or max threshold must be defined")

        super().__init__(*args, **kwargs)

    def execute(self, context):
        if self.min_threshold_sql:
            min_threshold = self.get_sql_value(
                self.threshold_conn_id,
                self.min_threshold_sql.format(**self.dq_check_args))
        else:
            min_threshold = None

        if self.max_threshold_sql:
            max_threshold = self.get_sql_value(
                self.threshold_conn_id,
                self.max_threshold_sql.format(**self.dq_check_args))
        else:
            max_threshold = None

        result = self.get_sql_value(
            self.conn_id, self.sql.format(**self.dq_check_args))

        within_threshold = True
        if max_threshold is not None and result > max_threshold:
            within_threshold = False
        if min_threshold is not None and result < min_threshold:
            within_threshold = False

        info_dict = {
            "result": result,
            "description": self.check_description,
            "task_id": self.task_id,
            "execution_date": str(context.get("execution_date")),
            "min_threshold": min_threshold,
            "max_threshold": max_threshold,
            "within_threshold": within_threshold
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key="return_value", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict
