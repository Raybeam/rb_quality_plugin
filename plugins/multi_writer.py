from airflow.utils.decorators import apply_defaults
from message_writer import MessageWriter

class MultiWriter(MessageWriter):
    @apply_defaults
    def __init__(self, writers, connection_id=None, *args, **kwargs):
        super().__init__(connection_id=connection_id,
                         *args, **kwargs)
        self.writers = writers

    def send_message(self, message):
        for writer in self.writers:
            writer.send_message(message)