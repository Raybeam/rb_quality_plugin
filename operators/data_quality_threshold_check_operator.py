import yaml

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults

from rb_quality_plugin.operators.base_data_quality_operator import (
    BaseDataQualityOperator,
)


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
    """

    @apply_defaults
    def __init__(
        self,
        min_threshold=None,
        max_threshold=None,
        config_path=None,
        *args,
        **kwargs
    ):

        self.dq_check_args = check_args
        defaults = {
            "min_threshold": min_threshold,
            "max_threshold": max_threshold,
        }

        if config_path:
            kwargs, defaults = self.read_from_config(
                config_path, kwargs, defaults
            )

        self.min_threshold = defaults["min_threshold"]
        self.max_threshold = defaults["max_threshold"]

        if self.max_threshold is None and self.min_threshold is None:
            raise AirflowException(
                "At least a min threshold or a max threshold must be defined"
            )

        super().__init__(*args, **kwargs)

    def option_between(self, value, low, high):
        if low is not None and value < low:
            return False
        if high is not None and value > high:
            return False
        return True

    def alert(self, context, result, min_threshold, max_threshold):
        within_threshold = self.option_between(
            result, min_threshold, max_threshold
        )

        info_dict = {
            "result": result,
            "description": self.check_description,
            "task_id": self.task_id,
            "execution_date": str(context.get("execution_date")),
            "min_threshold": min_threshold,
            "max_threshold": max_threshold,
            "within_threshold": within_threshold,
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key="return_value", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict

    def execute(self, context):
        result = self.get_sql_value(
            self.conn_id, self.sql.format(**self.dq_check_args)
        )

        return self.alert(
            context, result, self.min_threshold, self.max_threshold
        )
