import pytest
from airflow import DAG
from datetime import datetime
from plugins.base_data_quality_operator import BaseDataQualityOperator, \
    PostgresHook, MySqlHook, HiveServer2Hook

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2019,11,15),
}

test_dag = DAG(
    dag_id='test_id',
    default_args=default_args,
)

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
