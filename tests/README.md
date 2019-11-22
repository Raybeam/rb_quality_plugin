# Testing

## How to run
1. Install `pytest` and `pytest-mocker`.
2. Run in the tests directory with `pytest [test file]` or `pytest .` to run all test files in the directory.


## BaseDataQualityOperator
BaseDataQualityOperator includes tests for:
- `get_result()` method
    1. Test to ensure one result returns
    2. Test to ensure exception is raised if multiple rows return
    3. Test to ensure exception is raised if multiple columns return
    4. Test to ensure exception is raised if invalid connection type is given