from airflow.utils.decorators import apply_defaults

class MessageWriter:
    @apply_defaults
    def __init__(self, connection_id, *args, **kwargs):
        self.connection_id = connection_id

    def send_message(self, message):
        raise NotImplementedError
