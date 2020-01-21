from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch
from collections import defaultdict

from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection, TaskInstance

import yaml
from .helper import get_records_mock, dummy_dag

YAML_PATH = Path(__file__).parents[0] / "configs" / "yaml_configs"

def recursive_make_defaultdict(conf):
    if isinstance(conf, dict):
        for key in conf.keys():
            conf[key] = recursive_make_defaultdict(conf[key])
        return defaultdict(lambda: None, conf)
    return conf

def get_data_quality_operator(conf):
    kwargs = {
        "conn_id" : conf["fields"]["conn_id"],
        "sql" : conf["fields"]["sql"],
        "push_conn_id" : conf["push_conn_id"],
        "check_description" : conf["check_description"],
        "email" : conf["notification_emails"]
    }

    if conf["threshold"]["min_threshold_sql"]:
        task = DataQualityThresholdSQLCheckOperator(
            task_id=conf["test_name"],
            min_threshold_sql=conf["threshold"]["min_threshold_sql"],
            max_threshold_sql=conf["threshold"]["max_threshold_sql"],
            threshold_conn_id=conf["threshold"]["threshold_conn_id"],
            dag=dummy_dag,
            **kwargs)
    else:
        task = DataQualityThresholdCheckOperator(
            task_id=conf["test_name"],
            min_threshold=conf["threshold"]["min_threshold"],
            max_threshold=conf["threshold"]["max_threshold"],
            dag=dummy_dag,
            **kwargs)
    return task

def test_inside_threshold_values(mocker):
    yaml_path = YAML_PATH / "test_inside_threshold_values.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    with open(yaml_path) as config:
        conf = recursive_make_defaultdict(yaml.safe_load(config))
    task = get_data_quality_operator(conf)

    assert isinstance(task, DataQualityThresholdCheckOperator)

    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result["within_threshold"]

def test_inside_threshold_sql(mocker):
    yaml_path = YAML_PATH / "test_inside_threshold_sql.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    with open(yaml_path) as config:
        conf = recursive_make_defaultdict(yaml.safe_load(config))
    task = get_data_quality_operator(conf)

    assert isinstance(task, DataQualityThresholdSQLCheckOperator)

    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result["within_threshold"]

def test_outside_threshold_values(mocker):
    yaml_path = YAML_PATH / "test_outside_threshold_values.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    with open(yaml_path) as config:
        conf = recursive_make_defaultdict(yaml.safe_load(config))
    task = get_data_quality_operator(conf)

    assert isinstance(task, DataQualityThresholdCheckOperator)

    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())

    mock_patch = patch.object(
        DataQualityThresholdCheckOperator,
        "send_failure_notification",
        side_effect=lambda info_dict: info_dict)

    with mock_patch as notif_mock:
        result = task.execute(task_instance.get_template_context())

    assert notif_mock.called
    assert len(result) == 7
    assert not result["within_threshold"]

def test_outside_threshold_sql(mocker):
    yaml_path = YAML_PATH / "test_outside_threshold_sql.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    with open(yaml_path) as config:
        conf = recursive_make_defaultdict(yaml.safe_load(config))
    task = get_data_quality_operator(conf)

    assert isinstance(task, DataQualityThresholdSQLCheckOperator)

    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())

    mock_patch = patch.object(
        DataQualityThresholdSQLCheckOperator,
        "send_failure_notification",
        side_effect=lambda info_dict: info_dict)

    with mock_patch as notif_mock:
        result = task.execute(task_instance.get_template_context())

    assert notif_mock.called
    assert len(result) == 7
    assert not result["within_threshold"]
