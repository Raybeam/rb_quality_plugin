import os
from datetime import datetime
from unittest import mock

import airflow
from airflow.configuration import conf
from rb_quality_plugin.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from rb_quality_plugin.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance

from rb_quality_plugin.utilities.dq_check_tools import create_dq_checks_from_directory, create_dq_checks_from_list

DEFAULT_DATE = datetime.now()
DAG = airflow.DAG("TEST_DAG_ID", schedule_interval='@daily', default_args={'start_date' : DEFAULT_DATE})

plugins_folder = conf.get("core", "plugins_folder")
YAML_DIR = os.path.join(
    plugins_folder, "rb_quality_plugin", "tests", "configs")


def test_create_dq_checks_from_directory_with_recursion():
    
    task_list = create_dq_checks_from_directory(DAG, YAML_DIR)
    assert len(task_list) == 4
    op_types = {}
    for task in task_list:
        if type(task) not in op_types:
            op_types[type(task)] = 0
        op_types[type(task)] += 1
    print(op_types)
    assert op_types[DataQualityThresholdCheckOperator] == 2
    assert op_types[DataQualityThresholdSQLCheckOperator] == 2


def test_create_dq_checks_from_directory_no_recursion():
    
    task_list = create_dq_checks_from_directory(DAG, YAML_DIR, rec=False)
    assert len(task_list) == 0


def test_create_dq_checks_from_list():
    yaml_list = [
        (os.path.join(YAML_DIR, 'yaml_configs', 'test_inside_threshold_sql.yaml'), {}),
        (os.path.join(YAML_DIR, 'yaml_configs', 'test_inside_threshold_values.yaml'), {})
    ]

    task_list = create_dq_checks_from_list(DAG, yaml_list)
    assert len(task_list) == 2
