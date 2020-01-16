from datetime import datetime, timedelta
from pathlib import Path
import os
import glob
import yaml

from airflow import DAG
from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.operators.dummy_operator import DummyOperator

YAML_DIR = Path(__file__).parents[1] / "tests" / "configs" / "yaml_configs"

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2020, 1, 1),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

dag = DAG(
    "data_quality_check_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def get_data_quality_operator(conf, dag):
    kwargs = {
        "conn_type" : conf.get("fields").get("conn_type"),
        "conn_id" : conf.get("fields").get("conn_id"),
        "sql" : conf.get("fields").get("sql"),
        "push_conn_type" : conf.get("fields").get("push_conn_type"),
        "push_conn_id" : conf.get("fields").get("push_conn_id"),
        "check_description" : conf.get("check_description"),
        "notification_emails" : conf.get("notification_emails", [])
    }
    test_class = conf.get("data_quality_class")
    if test_class == "DataQualityThresholdSQLCheckOperator":
        task = DataQualityThresholdSQLCheckOperator(
            task_id=conf.get("test_name"),
            min_threshold_sql=conf.get("threshold").get("min_threshold_sql"),
            max_threshold_sql=conf.get("threshold").get("max_threshold_sql"),
            threshold_conn_type=conf.get("threshold").get("threshold_conn_type"),
            threshold_conn_id=conf.get("threshold").get("threshold_conn_id"),
            dag=dag,
            **kwargs)
    elif test_class == "DataQualityThresholdCheckOperator":
        task = DataQualityThresholdCheckOperator(
            task_id=conf.get("test_name"),
            min_threshold=conf.get("threshold").get("min_threshold"),
            max_threshold=conf.get("threshold").get("max_threshold"),
            dag=dag,
            **kwargs)
    else:
        raise ValueError(f"""Invalid Data Quality Operator class {test_class}""")
    return task

data_quality_check_tasks = []
for test_conf in glob.glob(os.path.join(str(YAML_DIR), "*.yaml")):
    with open(test_conf) as config:
        conf = yaml.safe_load(config)
    data_quality_check_tasks.append(get_data_quality_operator(conf, dag))

task_before_dq = DummyOperator(
    task_id="task_before_data_quality_checks",
    dag=dag
)

task_after_dq = DummyOperator(
    task_id="task_after_data_quality_checks",
    dag=dag
)

task_before_dq.set_downstream(data_quality_check_tasks)
task_after_dq.set_upstream(data_quality_check_tasks)