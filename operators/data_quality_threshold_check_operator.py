import yaml

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults

from rb_quality_plugin.operators.base_data_quality_operator
    import BaseDataQualityOperator


class DataQualityThresholdCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdCheckOperator builds off BaseOperator and
    executes a data quality check against high & low threshold values.

    :param min_threshold: lower-bound value
    :type min_threshold: numeric
    :param max_threshold: upper-bound value
    :type max_threshold: numeric
    :param config_path: path to yaml configuration file
    :type config_path: str
    :param check_args: dq parameters for sql evaluation
    :type check_args: dict
    """

    @apply_defaults
    def __init__(self,
                 min_threshold=None,
                 max_threshold=None,
                 config_path=None,
                 check_args={},
                 *args,
                 **kwargs):

        self.dq_check_args = check_args
        if config_path:
            with open(config_path) as configs:
                dq_configs = yaml.safe_load(configs)

            for key in dq_configs:
                if key == 'min_threshold' and min_threshold is None:
                    min_threshold = dq_configs[key]
                elif key == 'max_threshold' and max_threshold is None:
                    max_threshold = dq_configs[key]
                elif key not in kwargs:
                    kwargs[key] = dq_configs[key]

        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

        if self.max_threshold is None and self.min_threshold is None:
            raise AirflowException(
                "At least a min threshold or a max threshold must be defined")

        super().__init__(*args, **kwargs)

    def execute(self, context):
        result = self.get_sql_value(
            self.conn_id, self.sql.format(**self.dq_check_args))
        within_threshold = True
        if self.max_threshold is not None and result > self.max_threshold:
            within_threshold = False
        if self.min_threshold is not None and result < self.min_threshold:
            within_threshold = False

        info_dict = {
            "result": result,
            "description": self.check_description,
            "task_id": self.task_id,
            "execution_date": str(context.get("execution_date")),
            "min_threshold": self.min_threshold,
            "max_threshold": self.max_threshold,
            "within_threshold": within_threshold
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key="return_value", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict
