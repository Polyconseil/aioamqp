"""
    aioamqp exceptions
"""


class AioamqpException(Exception):
    pass


class AmqpClosedConnection(AioamqpException):
    pass


class SynchronizationError(AioamqpException):
    pass


class EmptyQueue(AioamqpException):
    pass


class NoChannelAvailable(AioamqpException):
    """There is no room left for more channels"""


class ChannelClosed(AioamqpException):
    def __init__(self, code=0, message='Channel is closed'):
        super().__init__(code, message)
        self.code = code
        self.message = message


class DuplicateConsumerTag(AioamqpException):
    def __repr__(self):
        #  pylint: disable=unsubscriptable-object
        return (f'The consumer tag specified already exists for this '
                f'channel: {self.args[0]}')


class ConsumerCancelled(AioamqpException):
    def __repr__(self):
        #  pylint: disable=unsubscriptable-object
        return (f'The consumer {self.args[0]} has been cancelled')


class PublishFailed(AioamqpException):
    def __init__(self, delivery_tag):
        super().__init__(delivery_tag)
        self.delivery_tag = delivery_tag

    def __repr__(self):
        return f'Publish failed because a nack was received for delivery_tag {self.delivery_tag}'
