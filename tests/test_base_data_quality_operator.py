import pytest
from unittest import mock
from airflow.hooks.base_hook import BaseHook
from rb_quality_plugin.operators.base_data_quality_operator
    import BaseDataQualityOperator


@mock.patch.object(BaseHook, 'get_connection')
@mock.patch.object(BaseHook, 'get_hook')
def test_get_sql_value_one_result(mock_get_hook, mock_get_connection):
    mock_get_connection.conn_type = 'id'
    mock_hook = mock.Mock()
    mock_hook.get_records.return_value = [(10,)]
    mock_get_hook.return_value = mock_hook

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    result = task.get_sql_value(
        conn_id=task.conn_id,
        sql=task.sql
    )

    assert result == 10


@mock.patch.object(BaseHook, 'get_connection')
@mock.patch.object(BaseHook, 'get_hook')
def test_get_sql_value_not_one_result(mock_get_hook, mock_get_connection):
    mock_get_connection.conn_type = 'id'
    mock_hook = mock.Mock()
    mock_hook.get_records.return_value = [(10,), (100,)]
    mock_get_hook.return_value = mock_hook

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    with pytest.raises(ValueError):
        task.get_sql_value(
            conn_id=task.conn_id,
            sql=task.sql
        )


@mock.patch.object(BaseHook, 'get_connection')
@mock.patch.object(BaseHook, 'get_hook')
def test_get_sql_value_no_result(mock_get_hook, mock_get_connection):
    mock_get_connection.conn_type = 'id'
    mock_hook = mock.Mock()
    mock_hook.get_records.return_value = []
    mock_get_hook.return_value = mock_hook

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    with pytest.raises(ValueError):
        task.get_sql_value(
            conn_id=task.conn_id,
            sql=task.sql
        )


@mock.patch.object(BaseHook, 'get_connection')
@mock.patch.object(BaseHook, 'get_hook')
def test_get_sql_value_multiple_results(mock_get_hook, mock_get_connection):
    mock_get_connection.conn_type = 'id'
    mock_hook = mock.Mock()
    mock_hook.get_records.return_value = [(10, "bad value")]
    mock_get_hook.return_value = mock_hook

    task = BaseDataQualityOperator(
        task_id="one_result_task",
        conn_id='test_id',
        sql='SELECT COUNT(1) FROM test;'
    )

    with pytest.raises(ValueError):
        task.get_sql_value(
            conn_id=task.conn_id,
            sql=task.sql
        )
