from pathlib import Path
from airflow import DAG
import datetime

import psycopg2
import testing.postgresql

SQL_PATH = Path(__file__).parents[0] / "configs" / "test_sql_table.sql"

def handler(postgresql):
    ''' Preloads postgres testing table with predetermined values.'''

    with open(SQL_PATH) as sql_file:
        test_table = sql_file.read()

    conn = psycopg2.connect(**postgresql.dsn())
    cursor = conn.cursor()
    cursor.execute(test_table)
    cursor.close()
    conn.commit()
    conn.close()

def get_records_mock(sql):
    '''Mock function to replace get_records() with unit test mocker'''
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

dummy_dag = DAG(
    "test_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime.datetime(2018, 1, 1),
    },
    schedule_interval=datetime.timedelta(days=1)
)