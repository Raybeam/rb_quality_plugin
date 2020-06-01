from datetime import datetime, timedelta

from airflow import DAG
import airflow.utils.dates as dt
from rb_quality_plugin.operators.data_quality_threshold_check_operator
    import DataQualityThresholdCheckOperator
from rb_quality_plugin.operators.data_quality_threshold_sql_check_operator
    import DataQualityThresholdSQLCheckOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",
    "start_date": dt.days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True
}

dag = DAG(
    "dq_sql_files_dag",
    default_args=default_args,
    schedule_interval="@monthly"
)

task_load_test_data = PostgresOperator(
    task_id="load_test_data",
    sql="dq_file_load_dag/dq_file_load_dag.sql",
    postgres_conn_id="test_conn",
    dag=dag
)

task_before_dq = DummyOperator(
    task_id="task_before_data_quality_checks",
    dag=dag
)

"""Task to check total revenue is between [30, 60] (test passes)"""
task_check_total_revenue = DataQualityThresholdCheckOperator(
    task_id="task_check_average_value",
    sql="dq_file_load_sql_tests/data_quality_total_rev.sql",
    conn_id="test_conn",
    min_threshold=30,
    max_threshold=60,
    push_conn_id="push_conn",
    check_description="test to determine whether the amount"\
                      " in Revenue table is between 30 and 60",
    dag=dag
)

"""
    Task to check revenue against low & high of
    Monthly_Return table (test expected to fail)
"""
task_check_from_last_month = DataQualityThresholdSQLCheckOperator(
    task_id="task_check_from_from_last_month",
    sql="dq_file_load_sql_tests/data_quality_total_rev.sql",
    conn_id="test_conn",
    threshold_conn_id="test_id",
    min_threshold_sql="dq_file_load_sql_tests/dq_min_threshold.sql",
    max_threshold_sql="dq_file_load_sql_tests/dq_max_threshold.sql",
    push_conn_id="push_conn",
    check_description="test of whether the revenue is between 20% of low"\
                    " and high of Monthly_Return table in the last month"\
                    " (failure expected)",
    dag=dag
)

data_quality_checks = [task_check_total_revenue, task_check_from_last_month]

task_after_dq = DummyOperator(
    task_id="task_after_data_quality_checks",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

task_load_test_data.set_downstream(task_before_dq)
task_before_dq.set_downstream(data_quality_checks)
task_after_dq.set_upstream(data_quality_checks)
