import datetime
from unittest.mock import Mock
from unittest.mock import patch
import pendulum
import pytest

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.models import TaskInstance

from plugins.base_data_quality_operator import BaseDataQualityOperator


def test_get_result_one_result(mocker):
    mocker.patch.object(
        PostgresHook,
        "get_records",
        return_value=[(10,)]
    )

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_type='postgres',
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    result = task.get_result(
        conn_type=task.conn_type,
        conn_id=task.conn_id,
        sql=task.sql
    )

    assert result == 10

def test_get_result_not_one_result(mocker):
    mocker.patch.object(
        HiveServer2Hook,
        "get_records",
        return_value=[(10,), (100,)]
    )

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_type='hive',
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    with pytest.raises(ValueError):
        task.get_result(
            conn_type=task.conn_type,
            conn_id=task.conn_id,
            sql=task.sql
        )

def test_get_result_no_result(mocker):
    mocker.patch.object(
        MySqlHook,
        "get_records",
        return_value=[]
    )

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_type='mysql',
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    with pytest.raises(ValueError):
        task.get_result(
            conn_type=task.conn_type,
            conn_id=task.conn_id,
            sql=task.sql
        )

def test_get_result_multiple_results(mocker):
    mocker.patch.object(
        MySqlHook,
        "get_records",
        return_value=[(10, "bad value")]
    )

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_type='mysql',
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    with pytest.raises(ValueError):
        task.get_result(
            conn_type=task.conn_type,
            conn_id=task.conn_id,
            sql=task.sql
        )

def test_get_result_invalid_connection():
    with pytest.raises(ValueError):
        BaseDataQualityOperator(
            task_id="one_result_task",
            conn_type='invalid_type',
            conn_id='test_id',
            sql='SELECT COUNT(1) FROM test;'
        )

@pytest.fixture
def test_dag():
    """Dummy DAG."""
    return DAG(
        "test_slack",
        default_args={
            "owner": "airflow",
            "start_date": datetime.datetime(2018, 1, 1),
        },
        schedule_interval=datetime.timedelta(days=1),
    )

def test_dq_check_operator(test_dag):
    """
    Run DataQualityCheckOperator with "test_dag" pytest fixture and tests that
    execute() returns the expected dictionary values.
    """
    expected_exec_date = datetime.datetime.now()
    expected = {'result': 3,
                'description': 'test',
                'execution_date': pendulum.instance(expected_exec_date),
                'task_id': 'test'}
    task = BaseDataQualityOperator(
        task_id="test",
        conn_id="postgres",
        conn_type="postgres",
        sql="SELECT COUNT(*) FROM dummy",
        check_description="test",
        dag=test_dag,
    )
    task.get_result = Mock(return_value=3)
    with patch.object(task, 'push') as mock:
        task_instance = TaskInstance(task=task, execution_date=expected_exec_date)
        result = task.execute(task_instance.get_template_context())

    assert result == expected
    assert mock.called
