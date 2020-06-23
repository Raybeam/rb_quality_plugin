from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from google.cloud.bigquery import Client

from message_writers.sql_writer import SQLWriter


class BigQueryWriter(SQLWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def send_message(self, message):
        """
        Inserts data into a BigQuery table.

        Please verify that the data structure you use for the message
        inserts rows into the BigQuery table correctly.

        :param message: values to be inserted into the BigQuery table
        :return:
        """
        message = [tuple(i[1] for i in message.items())]
        hook = BigQueryHook(bigquery_conn_id=self.connection_id)
        client = Client(project=hook._get_field("project"),
                        credentials=hook._get_credentials())
        table = client.get_table(self.table_id)
        errors = client.insert_rows(table, message)
        if not errors:
            print("New rows have been added.")
        else:
            print(errors)
