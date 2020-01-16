from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch
import testing.postgresql
import psycopg2

from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.hooks.postgres_hook import PostgresHook

import yaml

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

def get_data_quality_operator(conf):
    kwargs = {
        "conn_type" : conf.get("fields").get("conn_type"),
        "conn_id" : conf.get("fields").get("conn_id"),
        "sql" : conf.get("fields").get("sql"),
        "push_conn_type" : conf.get("fields").get("push_conn_type"),
        "push_conn_id" : conf.get("fields").get("push_conn_id"),
        "check_description" : conf.get("check_description"),
        "notification_emails" : conf.get("notification_emails", [])
    }
    test_class = conf.get("data_quality_class")
    if test_class == "DataQualityThresholdSQLCheckOperator":
        task = DataQualityThresholdSQLCheckOperator(
            task_id=conf.get("test_name"),
            min_threshold_sql=conf.get("threshold").get("min_threshold_sql"),
            max_threshold_sql=conf.get("threshold").get("max_threshold_sql"),
            threshold_conn_type=conf.get("threshold").get("threshold_conn_type"),
            threshold_conn_id=conf.get("threshold").get("threshold_conn_id"),
            **kwargs)
    elif test_class == "DataQualityThresholdCheckOperator":
        task = DataQualityThresholdCheckOperator(
            task_id=conf.get("test_name"),
            min_threshold=conf.get("threshold").get("min_threshold"),
            max_threshold=conf.get("threshold").get("max_threshold"),
            **kwargs)
    else:
        raise ValueError(f"""Invalid Data Quality Operator class {test_class}""")
    return task

def test_inside_threshold_values(mocker):
    yaml_path = YAML_PATH / "test_inside_threshold_values.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )
    with open(yaml_path) as config:
        task = get_data_quality_operator(yaml.safe_load(config))

    assert isinstance(task, DataQualityThresholdCheckOperator)

    task.push = Mock(return_value=None)

    with patch.object(task, "send_email_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert len(result) == 7
    assert not notification_mock.called
    assert result["within_threshold"]

def test_inside_threshold_sql(mocker):
    yaml_path = YAML_PATH / "test_inside_threshold_sql.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    with open(yaml_path) as config:
        task = get_data_quality_operator(yaml.safe_load(config))

    assert isinstance(task, DataQualityThresholdSQLCheckOperator)

    task.push = Mock(return_value=None)

    with patch.object(task, "send_email_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert len(result) == 7
    assert not notification_mock.called
    assert result["within_threshold"]

def test_outside_threshold_values(mocker):
    yaml_path = YAML_PATH / "test_outside_threshold_values.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    with open(yaml_path) as config:
        task = get_data_quality_operator(yaml.safe_load(config))

    assert isinstance(task, DataQualityThresholdCheckOperator)

    task.push = Mock(return_value=None)

    with patch.object(task, "send_email_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert len(result) == 7
    assert notification_mock.called
    assert not result["within_threshold"]

def test_outside_threshold_sql(mocker):
    yaml_path = YAML_PATH / "test_outside_threshold_sql.yaml"

    mocker.patch.object(
        PostgresHook,
        "get_records",
        side_effect=get_records_mock
    )

    with open(yaml_path) as config:
        task = get_data_quality_operator(yaml.safe_load(config))

    assert isinstance(task, DataQualityThresholdSQLCheckOperator)

    task.push = Mock(return_value=None)

    with patch.object(task, "send_email_notification") as notification_mock:
        result = task.execute(context={
            "execution_date" : datetime.now()
        })

    assert len(result) == 7
    assert not notification_mock.called
    assert not result["within_threshold"]
