import asyncio
import unittest

from . import testcase
from . import testing
from .. import exceptions


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.consume_future = asyncio.Future(loop=self.loop)

    @asyncio.coroutine
    def callback(self, body, envelope, properties):
        self.consume_future.set_result((body, envelope, properties))

    @asyncio.coroutine
    def get_callback_result(self):
        yield from self.consume_future
        result = self.consume_future.result()
        self.consume_future = asyncio.Future(loop=self.loop)
        return result

    @testing.coroutine
    def test_close(self):
        channel = yield from self.create_channel()
        self.assertTrue(channel.is_open)
        yield from channel.close()
        self.assertFalse(channel.is_open)

    @testing.coroutine
    def test_multiple_close(self):
        channel = yield from self.create_channel()
        yield from channel.close()
        self.assertFalse(channel.is_open)
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from channel.close()

        self.assertEqual(cm.exception.code, 504)

    @testing.coroutine
    def test_cannot_publish_after_close(self):
        channel = self.channel
        yield from channel.close()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from self.channel.publish("coucou", "my_e", "")

    @testing.coroutine
    def test_cannot_declare_queue_after_close(self):
        channel = self.channel
        yield from channel.close()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from self.channel.queue_declare("qq")

    @testing.coroutine
    def test_cannot_consume_after_close(self):
        channel = self.channel
        yield from self.channel.queue_declare("q")
        yield from channel.close()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.basic_consume(self.callback)
