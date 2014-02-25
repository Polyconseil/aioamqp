"""
    aioamqp exceptions
"""


class AioamqpException(Exception):
    pass


class ClosedConnection(AioamqpException):
    pass


class ConnectionError(AioamqpException):
    pass
