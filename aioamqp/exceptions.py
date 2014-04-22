"""
    aioamqp exceptions
"""


class AioamqpException(Exception):
    pass


class ClosedConnection(AioamqpException):
    pass


class ConnectionError(AioamqpException):
    pass


class ChannelClosed(AioamqpException):
    def __init__(self, message, frame):
        super().__init__(message, frame)
        self.message = message
        self.frame = frame
