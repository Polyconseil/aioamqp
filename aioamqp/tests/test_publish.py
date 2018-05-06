import unittest
import asyncio

from . import testcase
from . import testing


class PublishTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("coucou", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    @testing.coroutine
    def test_big_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("a"*1000000, "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    @testing.coroutine
    def test_big_unicode_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("Ы"*1000000, "e", routing_key='')
        yield from self.channel.publish("Ы"*1000000, "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(2, queues["q"]['messages'])

    @testing.coroutine
    def test_confirmed_publish(self):
        # declare
        yield from self.channel.confirm_select()
        self.assertTrue(self.channel.publisher_confirms)
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("coucou", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    @testing.coroutine
    def test_return_from_publish(self):
        called = False

        @asyncio.coroutine
        def callback(channel, body, envelope, properties):
            nonlocal called
            called = True
        channel = yield from self.amqp.channel(return_callback=callback)

        # declare
        yield from channel.exchange_declare("e", "topic")

        # publish
        yield from channel.publish("coucou", "e", routing_key="not.found",
                                   mandatory=True)

        for i in range(10):
            if called:
                break
            yield from asyncio.sleep(0.1)

        self.assertTrue(called)
