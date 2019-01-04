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
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])


class ConsumerCancelled(AioamqpException):
    def __repr__(self):
        #  pylint: disable=unsubscriptable-object
        return ('The consumer %s has been cancelled' % self.args[0])


class PublishFailed(AioamqpException):
    def __init__(self, delivery_tag):
        super().__init__(delivery_tag)
        self.delivery_tag = delivery_tag

    def __repr__(self):
        return 'Publish failed because a nack was received for delivery_tag {}'.format(
            self.delivery_tag)
