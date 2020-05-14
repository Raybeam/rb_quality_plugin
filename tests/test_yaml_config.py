import os
from datetime import datetime
from unittest.mock import Mock, patch

from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection, TaskInstance

import yaml
from .helper import get_records_mock, dummy_dag

YAML_PATH = "./tests/configs/yaml_configs"

def test_inside_threshold_values(mocker):
    yaml_path = os.path.join(YAML_PATH, "test_inside_threshold_values.yaml")

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

    task = DataQualityThresholdCheckOperator(
        task_id='test_inside_threshold_values',
        config_path=yaml_path,
        dag=dummy_dag
    )

    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result["within_threshold"]

def test_inside_threshold_sql(mocker):
    yaml_path = os.path.join(YAML_PATH, "test_inside_threshold_sql.yaml")

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

    task = DataQualityThresholdSQLCheckOperator(
        task_id='test_inside_threshold_sql',
        config_path=yaml_path,
        dag=dummy_dag
    )

    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result["within_threshold"]

def test_outside_threshold_values(mocker):
    yaml_path = os.path.join(YAML_PATH, "test_outside_threshold_values.yaml")

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

    task = DataQualityThresholdCheckOperator(
        task_id='test_outside_threshold_values',
        config_path=yaml_path,
        dag=dummy_dag
    )

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
    yaml_path = os.path.join(YAML_PATH, "test_outside_threshold_sql.yaml")

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

    task = DataQualityThresholdSQLCheckOperator(
        task_id='test_outside_threshold_sql',
        config_path=yaml_path,
        dag=dummy_dag
    )

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
