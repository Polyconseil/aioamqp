"""
    aioamqp exceptions
"""


class AioamqpException(Exception):
    pass


class AmqpClosedConnection(AioamqpException):
    pass


class ConnectionError(AioamqpException):
    pass


class ChannelClosed(AioamqpException):
    def __init__(self, message='Channel is closed', frame=None):
        super().__init__(message, frame)
        self.message = message
        self.frame = frame
