from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.data_quality_threshold_check_operator import DataQualityThresholdCheckOperator
from airflow.operators.data_quality_threshold_sql_check_operator import DataQualityThresholdSQLCheckOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2020,1,16),
    "retries" : 0,
    "retry_delay" : timedelta(minutes=5),
    "email_on_failure" : True
}

dag = DAG(
    "data_quality_check",
    default_args=default_args,
    schedule_interval="@daily"
)

task_load_test_data = PostgresOperator(
    task_id="load_test_data",
    sql="data_quality_dag/data_quality_dag.sql",
    postgres_conn_id="test_conn",
    dag=dag
)

task_before_dq = DummyOperator(
    task_id="task_before_data_quality_checks",
    dag=dag
)

"""Task to check avg value is between [20, 50] (test passes)"""
task_check_average = DataQualityThresholdCheckOperator(
    task_id="task_check_average_value",
    sql="SELECT AVG(cost) FROM Costs;",
    conn_id="test_conn",
    min_threshold=20,
    max_threshold=50,
    push_conn_id="push_conn",
    check_description="test to determine whether the average of the Price table is between 20 and 50",
    dag=dag
)

"""Task to check avg against high & low of Sales table (test fails)"""
task_check_from_last_month = DataQualityThresholdSQLCheckOperator(
    task_id="task_check_from_from_last_month",
    sql="SELECT AVG(cost) FROM Costs;",
    conn_id="test_conn",
    threshold_conn_id="test_id",
    min_threshold_sql="SELECT MIN(sale_price) FROM Sales WHERE date>=(DATE('{{ ds }}') - INTERVAL '1 month');",
    max_threshold_sql="SELECT MAX(sale_price) FROM Sales WHERE date>=(DATE('{{ ds }}') - INTERVAL '1 month');",
    push_conn_id="push_conn",
    check_description="test to of whether the average of Price table of last month is between low and high of Sales table from the last month",
    dag=dag
)

data_quality_checks = [task_check_average, task_check_from_last_month]

task_after_dq = DummyOperator(
    task_id="task_after_data_quality_checks",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

task_load_test_data.set_downstream(task_before_dq)
task_before_dq.set_downstream(data_quality_checks)
task_after_dq.set_upstream(data_quality_checks)
