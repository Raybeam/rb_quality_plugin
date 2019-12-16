from pathlib import Path
from datetime import datetime
from unittest.mock import Mock
import pytest
import psycopg2
import testing.postgresql

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator

SQL_PATH = Path(__file__).parents[0] / "configs" / "test_sql_table.sql"

def handler(postgresql):
    ''' Preloads postgres testing table with predetermined values.'''

    with open(SQL_PATH) as sql_file:
        test_table = sql_file.read()

    conn = psycopg2.connect(**postgresql.dsn())
    cursor = conn.cursor()
    cursor.execute(test_table)
    cursor.close()
    conn.commit()
    conn.close()

def get_records_mock(sql):
    '''Mock function to replace get_records() with unit test mocker'''
    Postgresql = testing.postgresql.PostgresqlFactory(
        on_initialized=handler,
        cache_initialized_db=True
    )
    with Postgresql() as psql:

        conn = psycopg2.connect(**psql.dsn())
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()

    return result

def make_threshold_check_task(sql, min_thresh, max_thresh, eval_thresh):
    if eval_thresh:
        task = DataQualityThresholdCheckOperator(
            task_id="test",
            conn_type="postgres",
            conn_id="postgres",
            threshold_conn_type="postgres",
            threshold_conn_id="test",
            sql=sql,
            min_threshold=min_thresh,
            max_threshold=max_thresh,
            eval_threshold=eval_thresh
        )
    else:
        task = DataQualityThresholdCheckOperator(
            task_id="test",
            conn_type="postgres",
            conn_id="postgres",
            sql=sql,
            min_threshold=min_thresh,
            max_threshold=max_thresh,
            eval_threshold=eval_thresh
        )
    task.push = Mock(return_value=None)
    return task

def test_invalid_sql_statement():
    eval_threshold = True
    min_threshold = 'SELECT COUNT(1) FROM test;'
    max_threshold = 'DROP test CASCADE;'
    sql = 'SELECT AVG(cost) FROM price'

    with pytest.raises(Exception):
        make_threshold_check_task(sql, min_threshold, max_threshold, eval_threshold)

def test_inside_threshold_values(mocker):
    eval_threshold = False
    min_threshold, max_threshold = 10, 15
    sql = "SELECT MIN(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    task = make_threshold_check_task(sql, min_threshold, max_threshold, eval_threshold)

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

    assert len(result) == 5
    assert result["within_threshold"]


def test_outside_threshold_values(mocker):
    eval_threshold = False
    min_threshold, max_threshold = 50, 75
    sql = "SELECT AVG(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    task = make_threshold_check_task(sql, min_threshold, max_threshold, eval_threshold)

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

    assert len(result) == 5
    assert not result["within_threshold"]


def test_inside_threshold_eval(mocker):
    eval_threshold = True
    min_threshold = "SELECT MIN(cost) FROM price;"
    max_threshold = "SELECT MAX(cost) FROM price;"
    sql = "SELECT MIN(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    task = make_threshold_check_task(sql, min_threshold, max_threshold, eval_threshold)

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

    assert len(result) == 5
    assert result["within_threshold"]


def test_outside_threshold_eval(mocker):
    eval_threshold = True
    min_threshold = "SELECT MIN(cost) FROM price;"
    max_threshold = "SELECT MAX(cost) FROM price;"
    sql = "SELECT MAX(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    task = make_threshold_check_task(sql, min_threshold, max_threshold, eval_threshold)

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

    assert len(result) == 5
    assert not result["within_threshold"]
