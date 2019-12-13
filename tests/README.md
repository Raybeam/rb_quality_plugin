# Testing

## How to run
1. Install `pytest` and `pytest-mocker`.
2. Run `airflow initdb` in home directory.
3. Run the tests using `pytest path/to/test/file/directory`.


## BaseDataQualityOperator
`test_base_data_quality_operator.py` includes tests for:
- `get_result()` method
    1. Test to ensure one result returns
    2. Test to ensure exception is raised if multiple rows return
    3. Test to ensure exception is raised if multiple columns return
    4. Test to ensure exception is raised if invalid connection type is given
- Test `execute()` method functionality


## DataQualityThresholdCheckOperator
`test_data_quality_threshold_check_operator.py` includes tests for:
- Test result of dq check is within threshold values
- Test result of dq check is outside threshold values
- Test result of dq check is within evaluated min/max thresholds
- Test result of dq check is outside evaluated min/max thresholds
