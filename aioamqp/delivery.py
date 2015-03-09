"""
    Delivery of messages to consumers
"""


class Delivery:
    __slots__ = (
        'consumer_tag', 'delivery_tag', 'exchange_name', 'routing_key', 'is_redeliver',
        'properties', 'properties', 'body')

    def __init__(
            self, consumer_tag, delivery_tag, exchange_name, routing_key, is_redeliver, properties,
            body):
        self.consumer_tag = consumer_tag
        self.delivery_tag = delivery_tag
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.is_redeliver = is_redeliver
        self.properties = properties
        self.body = body
