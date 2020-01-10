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

from plugins.base_data_quality_operator import BaseDataQualityOperator, get_sql_value


def test_get_sql_value_one_result(mocker):
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

    result = get_sql_value(
        conn_type=task.conn_type,
        conn_id=task.conn_id,
        sql=task.sql
    )

    assert result == 10

def test_get_sql_value_not_one_result(mocker):
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
        get_sql_value(
            conn_type=task.conn_type,
            conn_id=task.conn_id,
            sql=task.sql
        )

def test_get_sql_value_no_result(mocker):
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
        get_sql_value(
            conn_type=task.conn_type,
            conn_id=task.conn_id,
            sql=task.sql
        )

def test_get_sql_value_multiple_results(mocker):
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
        get_sql_value(
            conn_type=task.conn_type,
            conn_id=task.conn_id,
            sql=task.sql
        )

def test_get_sql_value_invalid_connection():
    with pytest.raises(ValueError):
        BaseDataQualityOperator(
            task_id="one_result_task",
            conn_type='invalid_type',
            conn_id='test_id',
            sql='SELECT COUNT(1) FROM test;'
        )
