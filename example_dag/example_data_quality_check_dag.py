from datetime import datetime, timedelta
from collections import defaultdict
import os
import glob
import yaml

from airflow import DAG
from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.operators.dummy_operator import DummyOperator

YAML_DIR = "./tests/configs/yaml_configs"

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2020,1,16),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

dag = DAG(
    "data_quality_check_dag",
    default_args=default_args,
    schedule_interval="@daily"
)

def recursive_make_defaultdict(conf):
    if isinstance(conf, dict):
        for key in conf.keys():
            conf[key] = recursive_make_defaultdict(conf[key])
        return defaultdict(lambda: None, conf)
    return conf

def get_data_quality_operator(conf, dag):
    kwargs = {
        "conn_type" : conf["fields"]["conn_type"],
        "conn_id" : conf["fields"]["conn_id"],
        "sql" : conf["fields"]["sql"],
        "push_conn_type" : conf["push_conn_type"],
        "push_conn_id" : conf["push_conn_id"],
        "check_description" : conf["check_description"],
        "notification_emails" : conf["notification_emails"]
    }

    if conf["threshold"]["min_threshold_sql"]:
        task = DataQualityThresholdSQLCheckOperator(
            task_id=conf["test_name"],
            min_threshold_sql=conf["threshold"]["min_threshold_sql"],
            max_threshold_sql=conf["threshold"]["max_threshold_sql"],
            threshold_conn_type=conf["threshold"]["threshold_conn_type"],
            threshold_conn_id=conf["threshold"]["threshold_conn_id"],
            dag=dag,
            **kwargs)
    else:
        task = DataQualityThresholdCheckOperator(
            task_id=conf["test_name"],
            min_threshold=conf["threshold"]["min_threshold"],
            max_threshold=conf["threshold"]["max_threshold"],
            dag=dag,
            **kwargs)
    return task

data_quality_check_tasks = []
for test_conf in glob.glob(os.path.join(str(YAML_DIR), "*.yaml")):
    with open(test_conf) as config:
        conf = recursive_make_defaultdict(yaml.safe_load(config))
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