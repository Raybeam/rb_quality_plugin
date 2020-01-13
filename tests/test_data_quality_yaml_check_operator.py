from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch
import pytest
import testing.postgresql
import psycopg2

from airflow.operators.data_quality_yaml_check_operator import DataQualityYAMLCheckOperator
from airflow.hooks.postgres_hook import PostgresHook

SQL_PATH = Path(__file__).parents[0] / "configs" / "test_sql_table.sql"
YAML_PATH = Path(__file__).parents[0] / "configs" / "yaml_configs"

def handler(postgresql):
    """ Preloads postgres with two testing tables. """
    with open(SQL_PATH) as table_file:
        test_table = table_file.read()

    conn = psycopg2.connect(**postgresql.dsn())
    cursor = conn.cursor()
    cursor.execute(test_table)
    cursor.close()
    conn.commit()
    conn.close()

def get_records_mock(sql):
    """ Mock function to replace get_records() with unit test mocker. """
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

def test_inside_threshold_values(mocker):
    yaml_path = YAML_PATH / "test_inside_threshold_values.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    task = DataQualityYAMLCheckOperator(
        task_id="test_task",
        yaml_path=yaml_path
    )
    task.push = Mock(return_value=None)

    with patch.object(task, "send_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })
    assert not notification_mock
    assert len(result) == 7
    assert result["within_threshold"]

def test_inside_threshold_sql(mocker):
    yaml_path = YAML_PATH / "test_inside_threshold_sql.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    task = DataQualityYAMLCheckOperator(
        task_id="test_task",
        yaml_path=yaml_path
    )
    task.push = Mock(return_value=None)

    with patch.object(task, "send_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert not notification_mock
    assert len(result) == 7
    assert result["within_threshold"]

def test_outside_threshold_values(mocker):
    yaml_path = YAML_PATH / "test_outside_threshold_values.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    task = DataQualityYAMLCheckOperator(
        task_id="test_task",
        yaml_path=yaml_path
    )
    task.push = Mock(return_value=None)

    with patch.object(task, "send_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert notification_mock
    assert len(result) == 7
    assert result["within_threshold"]

def test_outside_threshold_sql(mocker):
    yaml_path = YAML_PATH / "test_outside_threshold_sql.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    task = DataQualityYAMLCheckOperator(
        task_id="test_task",
        yaml_path=yaml_path
    )
    task.push = Mock(return_value=None)

    with patch.object(task, "send_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert notification_mock
    assert len(result) == 7
    assert not result["within_threshold"]

def test_invalid_yaml_path():
    yaml_path = YAML_PATH / "nonexistent_file.yaml"

    with pytest.raises(FileNotFoundError):
        DataQualityYAMLCheckOperator(
            task_id='test_task',
            yaml_path=yaml_path
        )
