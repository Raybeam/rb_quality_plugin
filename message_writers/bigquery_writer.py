from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from google.cloud.bigquery import Client

from rb_quality_plugin.message_writers.sql_writer import SQLWriter


class BigQueryWriter(SQLWriter):
    """
    This class is used to insert a row into a BigQuery table.

    An example use case would be to write an entry into a log table
    to note the details of a completed data process--when and by whom.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def send_message(self, message):
        """
        Inserts data from an ordered dictionary into a BigQuery table.

        A sample message input that might be passed might be
            {"col_1":1, "col_2":"a"}
        for a BigQuery table with "num_rows" and "letter" as
        the respective columns.

        Note that the keys in the dictionary do not matter--
        only the order of values does. Therefore be sure to
        verify that the order of the data you use for the message
        matches the order of the columns in the BigQuery table.

        :param message: ordered dictionary of values to be inserted
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
