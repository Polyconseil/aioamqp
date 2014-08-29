import unittest

from . import testcase
from . import testing


class PublishTestCase(testcase.AmqpTestCase, unittest.TestCase):

    def test_publish(self):
        # declare
        queue_name = "test_publish"
        exchange_name = "test_publish"
        yield from self.create_channel()
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)
        yield from self.exchange_declare(exchange_name, "fanout")
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        # publish
        yield from self.channel.publish("payload", exchange_name, routing_key='')

        yield from self.channel.basic_consume()
        ctags, count, message = yield from self.channel.consume()
        self.assertEqual(message, b"payload")

    def test_big_publish(self):
        # declare
        queue_name = "test_big_publish"
        exchange_name = "test_big_publish"
        yield from self.create_channel()
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)
        yield from self.exchange_declare(exchange_name, "fanout")
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        # publish
        payload = "a"*1000000
        yield from self.channel.publish(payload, exchange_name, routing_key='')

        yield from self.channel.basic_consume()
        message = yield from self.channel.consume()
        ctags, count, message = yield from self.channel.consume()
        self.assertEqual(message, payload)