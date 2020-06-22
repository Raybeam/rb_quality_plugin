from collections import OrderedDict
from unittest import mock
import pytest
from google.cloud.bigquery import Client

from message_writers.message_writer import MessageWriter
from message_writers.bigquery_writer import BigQueryWriter
from message_writers.multi_writer import MultiWriter


def test_message_writer():
    writer = MessageWriter()
    with pytest.raises(NotImplementedError):
        writer.send_message([{'a': 1}])


@mock.patch.object(Client, 'insert_rows')
@mock.patch.object(Client, 'get_table')
def test_bigquery_writer(mock_get_table, mock_insert_rows):
    """
    Mock the BigQuery connection and ensure that the correct info is passed
    when calling send_message().
    :param mock_get_table:
    :param mock_insert_rows:
    :return:
    """
    test_table_id = 'test_db.test_table_id'
    test_message = OrderedDict({"a": 1})
    mock_get_table.return_value = test_table_id
    writer = BigQueryWriter(connection_id='google_cloud_default',
                            table_id=test_table_id)
    writer.send_message(test_message)
    mock_insert_rows.assert_called_once_with(test_table_id,
                                 [tuple(i[1] for i in test_message.items())])


@mock.patch.object(MessageWriter, 'send_message')
def test_multiwriter(mock_send_message):
    """
    Test that the message sent by MultiWriter gets sent to all writers.
    :param mock_send_message:
    :return:
    """
    test_message = OrderedDict({"a": 1})
    writer = MultiWriter(writers=[MessageWriter(connection_id='test_conn1'),
                                  MessageWriter(connection_id='test_conn2')])
    assert mock_send_message.call_count == 0
    writer.send_message(test_message)
    assert mock_send_message.call_count == 2
