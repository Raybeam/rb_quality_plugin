class MessageWriter:
    """
    This class allows a "message" to be sent to an external recipient.
    """

    def __init__(self, *args, **kwargs):
        pass

    def send_message(self, message):
        """
        Sends a message to an external recipient.
        Usually, this involves creating a connection to an API with
        any necessary credentials.

        :param message:
        :return:
        """
        raise NotImplementedError
