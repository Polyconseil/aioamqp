from functools import wraps
import logging

import asyncio


class AsyncioErrors(AssertionError):
    def __repr__(self):
        return "<AsyncioErrors: Got asyncio errors: %r" % self.args[0]


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
