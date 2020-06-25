import os
import yaml
import logging

from rb_quality_plugin.operators.data_quality_threshold_check_operator import (
    DataQualityThresholdCheckOperator,
)
from rb_quality_plugin.operators.data_quality_threshold_sql_check_operator import (
    DataQualityThresholdSQLCheckOperator,
)

DQ_OPERATORS = {
    "dq_check_sql_threshold": DataQualityThresholdSQLCheckOperator,
    "dq_check_value_threshold": DataQualityThresholdCheckOperator,
}

log = logging.getLogger(__name__)


def create_dq_checks_from_list(dag, dq_check_list):
    """
    Creates list of dq check tasks for all configurations in list.
    If listed config is not dq check, method will log the config and skip.

    :param dag: dag for data quality operators
    :type dag: airflow.models.dag
    :param dq_check_list: list of tuples each containing a dq config path
        and dq check arguments
    :type dq_check_list: [(str, dict)]

    :ret val: list of dq operators
    :ret type: list
    """
    dq_check_tasks = []

    for dq_config_path, dq_check_args in dq_check_list:
        with open(dq_config_path) as config:
            conf = yaml.safe_load(config)
            dq_type = conf.get("type")

        if dq_type:
            if dq_type in DQ_OPERATORS:
                dq_operator = DQ_OPERATORS[dq_type]
                task = dq_operator(
                    config_path=dq_config_path, check_args=dq_check_args, dag=dag
                )
                dq_check_tasks.append(task)
            else:
                log.info(
                    "Type: %s not a DQ Operator... Skipping DQ creation", dq_type,
                )
        else:
            log.info(
                "File: %s does not contain 'type'... Skipping DQ creation",
                dq_config_path,
            )

    return dq_check_tasks


def create_dq_checks_from_directory(dag, dq_check_dir, dq_check_args={}, rec=True):
    """
    Creates list of dq check tasks for all configurations in directory, with
    option to recursively search through sub directories for dq config files

    :param dag: dag for data quality operators
    :type dag: airflow.models.dag
    :param dq_check_dir: directory path for configurations
    :type dq_check_dir: str
    :param dq_check_args: (optional) relevant arguments for dq checks
    :type dq_check_args: dict
    :param rec: (optional) recursively search through sub-directories
    :type rec: bool

    :ret val: list of dq operators
    :ret type: list
    """
    dq_check_files = []

    for path, _, files in os.walk(dq_check_dir):
        for file_name in files:
            if file_name[-5:] == ".yaml":
                config_path = os.path.join(path, file_name)
                dq_check_files.append((config_path, dq_check_args))
        if not rec:
            break

    return create_dq_checks_from_list(dag, dq_check_files)
