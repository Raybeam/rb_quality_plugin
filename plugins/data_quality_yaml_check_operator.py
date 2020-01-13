from pathlib import Path

from airflow.utils.email import send_email
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator

import yaml

class DataQualityYAMLCheckOperator(DataQualityThresholdSQLCheckOperator, DataQualityThresholdCheckOperator):
    '''
    DataQualityYAMLCheckOperator runs loads configuration parameters from a yaml
    file and runs a data quality check based off the specifications in the file

    Optionally, if data quality check fails, teams' emails listed in config file
    will also be notified by email.

    :param yaml_path: path to yaml configuration file with specifications for the test
    :type yaml_path: str
    '''

    @apply_defaults
    def __init__(self,
                 yaml_path,
                 *args,
                 **kwargs):
        self.yaml_path = Path(yaml_path)
        with open(self.yaml_path) as configs:
            conf = yaml.full_load(configs)
        self.eval_threshold = conf.get("threshold").get("eval_threshold")
        self.emails = conf.get("emails", [])
        self.test_name = conf.get("test_name")
        kwargs.update({
            "conn_type" : conf.get("fields").get("conn_type"),
            "conn_id" : conf.get("fields").get("conn_id"),
            "sql" : conf.get("fields").get("sql"),
            "push_conn_type" : conf.get("fields").get("push_conn_type"),
            "push_conn_id" : conf.get("fields").get("push_conn_id"),
            "check_description" : conf.get("check_description"),
            "min_threshold_sql" : conf.get("threshold").get("min_threshold_sql"),
            "max_threshold_sql" : conf.get("threshold").get("max_threshold_sql"),
            "threshold_conn_type" : conf.get("threshold").get("threshold_conn_type"),
            "threshold_conn_id" : conf.get("threshold").get("threshold_conn_id"),
            "min_threshold" : conf.get("threshold").get("min_threshold"),
            "max_threshold" : conf.get("threshold").get("max_threshold"),
        })
        super().__init__(*args, **kwargs)

    def execute(self, context):
        if self.eval_threshold:
            info_dict = DataQualityThresholdSQLCheckOperator.execute(self, context=context)
        else:
            info_dict = DataQualityThresholdCheckOperator.execute(self, context=context)

        if (not info_dict["within_threshold"]) and self.emails:
            self.send_email_notification(info_dict)
        return info_dict

    def send_email_notification(self, info_dict):
        body = f"""<h1>Data Quality Check: "{self.test_name}" failed.</h1><br>
<b>DAG:</b> {self.dag_id}<br>
<b>Task_id:</b> {info_dict.get("task_id")}<br>
<b>Check description:</b> {info_dict.get("description")}<br>
<b>Execution date:</b> {info_dict.get("execution_date")}<br>
<b>SQL:</b> {self.sql}<br>
<b>Result:</b> {round(info_dict.get("result"), 2)} is not within thresholds {info_dict.get("min_threshold")} and {info_dict.get("max_threshold")}"""
        send_email(
            to=self.emails,
            subject=f"""Data Quality Check: "{self.test_name}" failed""",
            html_content=body
        )


class DataQualityYAMLCheckPlugin(AirflowPlugin):
    name = "data_quality_yaml_check_operator"
    operators = [DataQualityYAMLCheckOperator]
