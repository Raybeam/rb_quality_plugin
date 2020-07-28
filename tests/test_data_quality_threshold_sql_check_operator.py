from datetime import datetime
import mock
import pytest

import airflow
from airflow import AirflowException
from airflow.utils.state import State
from rb_quality_plugin.operators.data_quality_threshold_sql_check_operator import (
    DataQualityThresholdSQLCheckOperator,
)
from airflow.models import TaskInstance

DEFAULT_DATE = datetime.now()
DAG = airflow.DAG(
    "TEST_DAG_ID", schedule_interval="@daily", default_args={"start_date": DEFAULT_DATE}
)


def get_val_from_sql(val, sql):
    return int(sql)


def _construct_task(
    min_threshold_sql=None, max_threshold_sql=None, sql="SELECT;", check_args={}
):
    task = DataQualityThresholdSQLCheckOperator(
        task_id="test_dq_check",
        conn_id="test_id",
        sql=sql,
        check_args=check_args,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        dag=DAG,
    )

    return task


@mock.patch.object(DataQualityThresholdSQLCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_inside_threshold_eval(mock_get_sql_value):
    mock_get_sql_value.side_effect = get_val_from_sql
    min_threshold_sql = "10"
    max_threshold_sql = "50"
    sql = "19"

    task = _construct_task(
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        sql=sql,
    )

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
    task_instance.run(ignore_ti_state=True)
    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert res["within_threshold"]
    assert task_instance.state == State.SUCCESS


@mock.patch.object(DataQualityThresholdSQLCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_outside_threshold_eval(mock_get_sql_value):
    mock_get_sql_value.side_effect = get_val_from_sql
    min_threshold_sql = "2"
    max_threshold_sql = "30"
    sql = "0"

    task = _construct_task(
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        sql=sql,
    )

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    with pytest.raises(AirflowException):
        task_instance.run(ignore_ti_state=True)

    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert not res["within_threshold"]
    assert task_instance.state == State.FAILED


@mock.patch.object(DataQualityThresholdSQLCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_one_threshold_eval(mock_get_sql_value):
    mock_get_sql_value.side_effect = get_val_from_sql
    min_threshold_sql = None
    max_threshold_sql = "120"
    sql = "10"

    task = _construct_task(
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        sql=sql,
    )

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
    task_instance.run(ignore_ti_state=True)
    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert res["min_threshold"] is None
    assert task_instance.state == State.SUCCESS


@mock.patch.object(DataQualityThresholdSQLCheckOperator, "get_sql_value")
@pytest.mark.compatibility
def test_threshold_check_args(mock_get_sql_value):
    mock_get_sql_value.side_effect = get_val_from_sql
    min_threshold_sql = "0"
    max_threshold_sql = "{max_value}"
    sql = "{target_value}"
    check_args = {"target_value": 10, "max_value": 100}

    task = _construct_task(
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql,
        sql=sql,
        check_args=check_args,
    )

    task.push = mock.Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
    task_instance.run(ignore_ti_state=True)
    res = task_instance.xcom_pull(task_ids="test_dq_check")

    assert res["max_threshold"] == 100
    assert res["result"] == 10
    assert task_instance.state == State.SUCCESS


def test_no_thresholds_eval():
    min_threshold_sql = None
    max_threshold_sql = None
    sql = "10"

    with pytest.raises(AirflowException):
        _construct_task(
            min_threshold_sql=min_threshold_sql,
            max_threshold_sql=max_threshold_sql,
            sql=sql,
        )
