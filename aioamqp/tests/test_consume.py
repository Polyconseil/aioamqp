import unittest

import asyncio

from . import testcase
from .. import exceptions


class ConsumeTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_consume(self):
        @asyncio.coroutine
        def go():
            # declare
            yield from self.queue_declare("q", exclusive=True, no_wait=False, timeout=0.5)
            yield from self.exchange_declare("e", "fanout")
            yield from self.channel.queue_bind("q", "e", routing_key='')

            # get a different channel
            channel = yield from self.create_channel()

            # publish
            yield from channel.publish("coucou", "e", routing_key='')

            # assert there is a message to consume
            queues = yield from self.list_queues()
            self.assertIn("q", queues)
            self.assertEqual(1, queues["q"]['messages'])

            # start consume
            yield from channel.basic_consume("q")

            # get one
            consume_data = yield from asyncio.wait_for(channel.consume(), timeout=5)
            consumer_tag, delivery_tag, message = consume_data
            self.assertIsNotNone(consumer_tag)
            self.assertIsNotNone(delivery_tag)
            self.assertEqual(b"coucou", message)
        self.loop.run_until_complete(go())

    def test_stuck(self):
        @asyncio.coroutine
        def go():
            # declare
            yield from self.queue_declare("q", exclusive=True, no_wait=False, timeout=0.5)
            yield from self.exchange_declare("e", "fanout")
            yield from self.channel.queue_bind("q", "e", routing_key='')

            # get a different channel
            channel = yield from self.create_channel()

            # start consume
            yield from channel.basic_consume("q")

            # close
            channel.close()
            yield from channel.wait_closed()

            # get one
            with self.assertRaises(exceptions.ChannelClosed):
                consume_data = yield from asyncio.wait_for(channel.consume(), timeout=5)
        self.loop.run_until_complete(go())
