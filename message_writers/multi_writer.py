from message_writers.message_writer import MessageWriter


class MultiWriter(MessageWriter):
    """
    Sends a message through multiple passed-in MessageWriter objects.
    """
    def __init__(self, writers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writers = writers

    def send_message(self, message):
        for writer in self.writers:
            writer.send_message(message)
