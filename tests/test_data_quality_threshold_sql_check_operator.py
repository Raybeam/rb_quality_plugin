from datetime import datetime
from unittest.mock import Mock, patch
import pytest

import airflow
from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from rb_quality_plugin.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.models import Connection, TaskInstance

from .helper import get_records_mock, dummy_dag


def test_inside_threshold_eval(mocker):
    min_threshold_sql = "SELECT MIN(cost) FROM price;"
    max_threshold_sql = "SELECT MAX(cost) FROM price;"
    sql = "SELECT MIN(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    task = DataQualityThresholdSQLCheckOperator(
        task_id="test_inside_threshold_eval",
        conn_id="test_id",
        threshold_conn_id="test_id",
        sql=sql,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        dag=dummy_dag
    )
    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result["within_threshold"]


def test_outside_threshold_eval(mocker):
    min_threshold_sql = "SELECT MIN(cost) FROM price;"
    max_threshold_sql = "SELECT MAX(cost) FROM price;"
    sql = "SELECT MAX(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    task = DataQualityThresholdSQLCheckOperator(
        task_id="test_outside_threshold_eval",
        conn_id="test_id",
        threshold_conn_id="test_id",
        sql=sql,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
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


def test_one_threshold_eval(mocker):
    min_threshold_sql = None
    max_threshold_sql = "Select MAX(VALUE) from test"
    sql = "Select 10;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    task = DataQualityThresholdSQLCheckOperator(
        task_id="test_one_threshold_eval",
        conn_id="test_id",
        threshold_conn_id='test_id',
        sql=sql,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        dag=dummy_dag
    )
    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())

    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result['min_threshold'] is None
    assert result["within_threshold"]


def test_threshold_check_args(mocker):
    min_threshold_sql = "Select 0;"
    max_threshold_sql = "Select Max(Value) from {target_table}"
    sql = "SELECT AVG(value) FROM {target_table};"
    check_args = {"target_table": "test"}

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    task = DataQualityThresholdSQLCheckOperator(
        task_id="test_threshold_check_args",
        conn_id="test_id",
        threshold_conn_id='test_id',
        sql=sql,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        check_args=check_args,
        dag=dummy_dag
    )
    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())

    result = task.execute(task_instance.get_template_context())

    assert len(result) == 7
    assert result["within_threshold"]


def test_no_thresholds_eval(mocker):
    min_threshold_sql = None
    max_threshold_sql = None
    sql = "Select 10;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id='test_id', conn_type='postgres')
    )

    with pytest.raises(AirflowException):
        DataQualityThresholdSQLCheckOperator(
            task_id="test_outside_threshold_values",
            conn_id="test_id",
            threshold_conn_id='test_id',
            sql=sql,
            min_threshold_sql=min_threshold_sql,
            max_threshold_sql=max_threshold_sql,
            dag=dummy_dag
        )
