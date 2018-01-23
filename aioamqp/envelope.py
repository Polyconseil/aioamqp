"""
    Delivery of messages to consumers
"""


class Envelope:
    """Class for basic deliver message fields"""
    __slots__ = ('consumer_tag', 'delivery_tag', 'exchange_name', 'routing_key', 'is_redeliver')

    def __init__(self, consumer_tag, delivery_tag, exchange_name, routing_key, is_redeliver):
        self.consumer_tag = consumer_tag
        self.delivery_tag = delivery_tag
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.is_redeliver = is_redeliver


class ReturnEnvelope:
    """ Class for basic return message fields"""
    __slots__ = ('reply_code', 'reply_text', 'exchange_name', 'routing_key')

    def __init__(self, reply_code, reply_text, exchange_name, routing_key):
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.exchange_name = exchange_name
        self.routing_key = routing_key
