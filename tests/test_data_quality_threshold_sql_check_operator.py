from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator

import psycopg2
import testing.postgresql

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

def test_inside_threshold_eval(mocker):
    min_threshold_sql = "SELECT MIN(cost) FROM price;"
    max_threshold_sql = "SELECT MAX(cost) FROM price;"
    sql = "SELECT MIN(value) FROM test;"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock,
    )

    task = DataQualityThresholdSQLCheckOperator(
        task_id="test",
        conn_type="postgres",
        conn_id="postgres",
        threshold_conn_type="postgres",
        threshold_conn_id="test",
        sql=sql,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql
    )
    task.push = Mock(return_value=None)

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

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

    task = DataQualityThresholdSQLCheckOperator(
        task_id="test",
        conn_type="postgres",
        conn_id="postgres",
        threshold_conn_type="postgres",
        threshold_conn_id="test",
        sql=sql,
        min_threshold_sql=min_threshold_sql,
        max_threshold_sql=max_threshold_sql
    )
    task.push = Mock(return_value=None)

    result = task.execute(context={
        "execution_date": datetime.now(),
    })

    assert len(result) == 7
    assert not result["within_threshold"]