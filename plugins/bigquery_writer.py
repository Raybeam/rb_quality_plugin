from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks import BigQueryHook
from google.cloud import bigquery

from sql_writer import SQLWriter

class BigQueryWriter(SQLWriter):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def send_message(self, message):
        """
        :param message: array of strings -- values to be inserted into BigQuery table
        :return:
        """
        hook = BigQueryHook(bigquery_conn_id=self.connection_id, use_legacy_sql=False)
        client = bigquery.Client(project=hook._get_field("project"),
                                 credentials=hook._get_credentials())
        table = client.get_table(self.table_id)
        errors = client.insert_rows(table, message)
        if not errors:
            print("New rows have been added.")
        else:
            print(errors)