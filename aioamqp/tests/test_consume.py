import unittest

import asyncio

from . import testcase


class ConsumeTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_consume(self):
        @asyncio.coroutine
        def go():
            # declare
            yield from self.queue_declare("q", exclusive=True, no_wait=False, timeout=0.5)
            yield from self.exchange_declare("e", "fanout")
            yield from self.channel.queue_bind("q", "e", routing_key='')

            # publish
            yield from self.channel.publish("coucou", "e", routing_key='')

            # assert there is a message to consume
            queues = yield from self.list_queues()
            self.assertIn("q", queues)
            self.assertEqual(1, queues["q"]['messages'])

            # start consume
            yield from self.channel.basic_consume("q")

            # get one
            consume_data = yield from asyncio.wait_for(self.channel.consume(), timeout=5)
            consumer_tag, delivery_tag, message = consume_data
            self.assertIsNotNone(consumer_tag)
            self.assertIsNotNone(delivery_tag)
            self.assertEqual(b"coucou", message)
        self.loop.run_until_complete(go())
