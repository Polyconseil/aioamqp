import logging


class AsyncioErrors(AssertionError):
    def __repr__(self):
        #  pylint: disable=unsubscriptable-object
        return f"<AsyncioErrors: Got asyncio errors: {self.args[0]!r}"


class Handler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.messages = []

    def emit(self, record):
        message = record.msg % record.args
        self.messages.append(message)


asyncio_logger = logging.getLogger('asyncio')
handler = Handler()
asyncio_logger.addHandler(handler)
