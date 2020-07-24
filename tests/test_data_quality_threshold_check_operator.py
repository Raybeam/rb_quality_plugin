from datetime import datetime
from unittest import mock
import pytest

import airflow
from airflow import AirflowException
from airflow.utils.state import State
from rb_quality_plugin.operators.data_quality_threshold_check_operator import (
    DataQualityThresholdCheckOperator,
)
from airflow.models import TaskInstance

DEFAULT_DATE = datetime.now()
DAG = airflow.DAG(
    "TEST_DAG_ID", schedule_interval="@daily", default_args={"start_date": DEFAULT_DATE}
)


def _construct_task(
    min_threshold=None, max_threshold=None, sql="SELECT;", check_args={}
):
    task = DataQualityThresholdCheckOperator(
        task_id="test_dq_check",
        conn_id="test_id",
        sql=sql,
        check_args=check_args,
        min_threshold=min_threshold,
        max_threshold=max_threshold,
        dag=DAG,
    )

    return task


@mock.patch.object(DataQualityThresholdCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_inside_threshold_values(mock_get_sql_value):
    mock_get_sql_value.return_value = 12
    task = _construct_task(min_threshold=10, max_threshold=15)

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
    task_instance.run(ignore_ti_state=True)
    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert res["within_threshold"]
    assert task_instance.state == State.SUCCESS


@mock.patch.object(DataQualityThresholdCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_outside_threshold_values(mock_get_sql_value):
    mock_get_sql_value.return_value = 100
    task = _construct_task(min_threshold=10, max_threshold=15)

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    with pytest.raises(AirflowException):
        task_instance.run(ignore_ti_state=True)

    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert not res["within_threshold"]
    assert task_instance.state == State.FAILED


@mock.patch.object(DataQualityThresholdCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_one_threshold_value(mock_get_sql_value):
    mock_get_sql_value.return_value = 10
    task = _construct_task(min_threshold=0)

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
    task_instance.run(ignore_ti_state=True)

    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert res["max_threshold"] is None
    assert task_instance.state == State.SUCCESS


@mock.patch.object(DataQualityThresholdCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_threshold_check_args(mock_get_sql_value):
    mock_get_sql_value.side_effect = lambda _, sql: int(sql)

    sql = "{target_value}"
    check_args = {"target_value": 100}
    task = _construct_task(
        min_threshold=50, max_threshold=150, sql=sql, check_args=check_args
    )

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    task_instance.run(ignore_ti_state=True)

    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert res["result"] == 100
    assert task_instance.state == State.SUCCESS


def test_no_thresholds_value():

    with pytest.raises(AirflowException):
        _construct_task()
