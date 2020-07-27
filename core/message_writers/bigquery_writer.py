from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from google.cloud.bigquery import Client

from rb_quality_plugin.core.message_writers.sql_writer import SQLWriter


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
        Inserts data from a list of dictionaries into a BigQuery table.

        A sample message input that might be passed might be
            [{"num_rows":1, "letter":"a"}, {"num_rows":5, "letter":"g"}]
        for a BigQuery table with "num_rows" and "letter" as
        the respective columns.

        Note that the order of the json attributes doesnt matter--
        only that the dictionary contains the correct keys.
        Therefore be sure to verify that the keys included in
        message also exist as columns in the BigQuery table.

        :param message: Rows to be inserted into Destination table
        :type message: List(Dict)
        """
        hook = BigQueryHook(bigquery_conn_id=self.connection_id)
        client = Client(
            project=hook._get_field("project"), credentials=hook._get_credentials()
        )
        table = client.get_table(self.table_id)
        errors = client.insert_rows_json(table, message)
        if not errors:
            print("New rows have been added.")
        else:
            print(errors)
