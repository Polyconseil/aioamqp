import unittest

from . import testcase
from . import testing
from .. import exceptions


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_close(self):
        channel = yield from self.create_channel()
        self.assertTrue(channel.is_open)
        channel.close()
        yield from channel.wait_closed()
        self.assertFalse(channel.is_open)

    @testing.coroutine
    def test_multiple_close(self):
        channel = yield from self.create_channel()
        channel.close()
        yield from channel.wait_closed()
        self.assertFalse(channel.is_open)
        channel.close()
        yield from channel.wait_closed()

    @testing.coroutine
    def test_cannot_publish_after_close(self):
        channel = self.channel
        channel.close()
        yield from channel.wait_closed()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from self.channel.publish("coucou", "my_e", "")

    @testing.coroutine
    def test_cannot_declare_queue_after_close(self):
        channel = self.channel
        channel.close()
        yield from channel.wait_closed()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from self.channel.queue_declare("qq")

    @testing.coroutine
    def test_cannot_consume_after_close(self):
        yield from self.queue_declare("q")
        channel = yield from self.create_channel()
        yield from channel.basic_consume("q")
        channel.close()
        yield from channel.wait_closed()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.consume()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.consume()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.consume()
