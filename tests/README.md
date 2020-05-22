# Testing

## How to run
1. Install `pytest` and `pytest-mocker`.
2. Run `airflow initdb` in home directory.
3. Run the tests using `pytest path/to/test/file/directory`.


## BaseDataQualityOperator Tests
`test_base_data_quality_operator.py` includes tests for:
- `get_sql_value()` method
    1. Test to ensure one result returns
    2. Test to ensure exception is raised if multiple rows return
    3. Test to ensure exception is raised if multiple columns return
    4. Test to ensure exception is raised if invalid connection type is given

## DataQualityThresholdCheckOperator Tests
`test_data_quality_threshold_check_operator.py` includes tests for:
- Test result of dq check is within threshold values
- Test result of dq check is outside threshold values
- Test result of dq check with only one threshold boundary
- Test result of dq check with arguments for sql statement

## DataQualityThresholdSQLCheckOperator Tests
`test_data_quality_threshold_sql_check_operator.py` includes tests for:
- Test result of dq check is within evaluated min/max thresholds
- Test result of dq check is outside evaluated min/max thresholds
- Test result of dq check with only one threshold boundary
- Test result of dq check with arguments for sql statement

## YAML Load Tests
`test_yaml_config.py` performs test cases for YAML configuration files. Tests include:
- YAML configurations for both DataQualityThresholdCheckOperator and DataQualityThresholdSQLCheckOperator
- Checks if `send_email_notification()` is called when a test fails and emails are provided in the operator 
- Checks if `send_email_notification()` is not called when emails are not provided, regardless of whether the test fail or pass.
