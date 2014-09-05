"""
    aioamqp exceptions
"""


class AioamqpException(Exception):
    pass

class ConfigurationError(AioamqpException):
    pass

class AmqpClosedConnection(AioamqpException):
    pass

class SynchronizationError(AioamqpException):
    pass

class EmptyQueue(AioamqpException):
    pass


class ChannelClosed(AioamqpException):
    def __init__(self, code=0, message='Channel is closed'):
        super().__init__(code, message)
        self.code = code
        self.message = message


class DuplicateConsumerTag(AioamqpException):
    def __repr__(self):
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])


class ConsumerCancelled(AioamqpException):
    def __repr__(self):
        return ('The consumer %s has been cancelled' % self.args[0])
