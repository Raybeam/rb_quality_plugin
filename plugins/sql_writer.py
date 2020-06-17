from airflow.utils.decorators import apply_defaults
from message_writer import MessageWriter

class SQLWriter(MessageWriter):
    @apply_defaults
    def __init__(self, table_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_id = table_id

    def send_message(self, message):
        raise NotImplementedError