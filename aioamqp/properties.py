# pylint: disable=redefined-builtin
import datetime

from .constants import MESSAGE_PROPERTIES


class Properties:
    """Class for basic message properties"""
    __slots__ = tuple(MESSAGE_PROPERTIES)

    def __init__(
            self, content_type=None, content_encoding=None, headers=None, delivery_mode=None,
            priority=None, correlation_id=None, reply_to=None, expiration=None, message_id=None,
            timestamp=None, message_type=None, user_id=None, app_id=None, cluster_id=None):
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
        self.message_type = message_type
        self.user_id = user_id
        self.app_id = app_id
        self.cluster_id = cluster_id


def from_pamqp(instance):
    props = Properties()
    props.content_type = instance.content_type
    props.content_encoding = instance.content_encoding
    props.headers = instance.headers
    props.delivery_mode = instance.delivery_mode
    props.priority = instance.priority
    props.correlation_id = instance.correlation_id
    props.reply_to = instance.reply_to
    props.expiration = instance.expiration
    props.message_id = instance.message_id
    if instance.timestamp is not None:
        # pamqp uses naive datetimes representing UTC, let's use TZ-aware datetimes
        props.timestamp = instance.timestamp.replace(tzinfo=datetime.timezone.utc)
    props.message_type = instance.message_type
    props.user_id = instance.user_id
    props.app_id = instance.app_id
    props.cluster_id = instance.cluster_id
    return props
