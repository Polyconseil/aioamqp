from .constants import MESSAGE_PROPERTIES


class Properties:
    """Class for basic message properties"""
    __slots__ = tuple(MESSAGE_PROPERTIES)

    def __init__(
            self, content_type=None, content_encoding=None, headers=None, delivery_mode=None,
            priority=None, correlation_id=None, reply_to=None, expiration=None, message_id=None,
            timestamp=None, type=None, user_id=None, app_id=None, cluster_id=None): # pylint: disable=redefined-builtin
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.headers = headers
        self.delivery_mode = delivery_mode
        self.priority = priority
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.expiration = expiration
        self.message_id = message_id
        self.timestamp = timestamp
        self.type = type
        self.user_id = user_id
        self.app_id = app_id
        self.cluster_id = cluster_id
