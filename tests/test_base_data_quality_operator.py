import pytest
from plugins.base_data_quality_operator import BaseDataQualityOperator, \
    PostgresHook, MySqlHook, HiveServer2Hook

def test_get_result_one_result(mocker):
    mocker.patch.object(
        PostgresHook,
        "get_records",
        return_value=[(10,)]
    )

    result = BaseDataQualityOperator.get_result(
        conn_type="postgres",
        conn_id="test_id",
        sql="SELECT COUNT(1) FROM test;"
    )

    assert result == 10

def test_get_result_not_one_result(mocker):
    mocker.patch.object(
        HiveServer2Hook,
        "get_records",
        return_value=[(10,), (100,)]
    )

    with pytest.raises(ValueError):
        BaseDataQualityOperator.get_result(
            conn_type="hive",
            conn_id="test_id",
            sql="SELECT COUNT(1) FROM test;"
        )

def test_get_result_no_result(mocker):
    mocker.patch.object(
        HiveServer2Hook,
        "get_records",
        return_value=[]
    )

    with pytest.raises(ValueError):
        BaseDataQualityOperator.get_result(
            conn_type="hive",
            conn_id="test_id",
            sql="SELECT COUNT(1) FROM test;"
        )

def test_get_result_multiple_results(mocker):
    mocker.patch.object(
        MySqlHook,
        "get_records",
        return_value=[(10, "bad value")]
    )

    with pytest.raises(ValueError):
        BaseDataQualityOperator.get_result(
            conn_type="mysql",
            conn_id="test_id",
            sql="SELECT COUNT(1) FROM test;"
        )

def test_get_result_invalid_connection():
    with pytest.raises(ValueError):
        BaseDataQualityOperator.get_result(
            conn_type="invalid_type",
            conn_id="test_id",
            sql="SELECT COUNT(1) FROM test;"
        )
