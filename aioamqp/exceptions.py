"""
    aioamqp exceptions
"""


class AioamqpException(Exception):
    pass


class AmqpClosedConnection(AioamqpException):
    pass


class ChannelClosed(AioamqpException):
    def __init__(self, message='Channel is closed', frame=None):
        super().__init__(message, frame)
        self.message = message
        self.frame = frame


class DuplicateConsumerTag(AioamqpException):
    def __repr__(self):
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])
