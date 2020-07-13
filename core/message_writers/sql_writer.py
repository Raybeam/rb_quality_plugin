from rb_quality_plugin.core.message_writers.message_writer import MessageWriter


class SQLWriter(MessageWriter):
    def __init__(self, table_id, connection_id, *args, **kwargs):
        """
        Inserts provided message as a row into a SQL table.
        Note that the message's field order must match that of the table
        that it will be passed to.

        :param table_id: The table to push to.
        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)
        self.table_id = table_id
        self.connection_id = connection_id

    def send_message(self, message):
        raise NotImplementedError
