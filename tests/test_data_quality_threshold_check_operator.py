from datetime import datetime
from unittest.mock import Mock, patch

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.models import Connection, TaskInstance

from .helper import get_records_mock, dummy_dag

def test_inside_threshold_values(mocker):
    min_threshold, max_threshold = 10, 15
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

    task = DataQualityThresholdCheckOperator(
        task_id="test_inside_threshold_values",
        conn_id="postgres",
        sql=sql,
        min_threshold=min_threshold,
        max_threshold=max_threshold,
        dag=dummy_dag
    )
    task.push = Mock(return_value=None)
    task_instance = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(task_instance.get_template_context())

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

    assert len(result) == 7
    assert result["within_threshold"]


def test_outside_threshold_values(mocker):
    min_threshold, max_threshold = 50, 75
    sql = "SELECT AVG(value) FROM test;"

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

    task = DataQualityThresholdCheckOperator(
        task_id="test_outside_threshold_values",
        conn_id="postgres",
        sql=sql,
        min_threshold=min_threshold,
        max_threshold=max_threshold,
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
