import unittest

import asyncio

from .. import exceptions
from . import testcase


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_close(self):
        @asyncio.coroutine
        def go():
            channel = yield from self.create_channel()
            self.assertTrue(channel.is_open)
            channel.close()
            yield from asyncio.wait_for(channel.wait_closed(), timeout=2)
            self.assertFalse(channel.is_open)
        self.loop.run_until_complete(go())

    def test_multiple_close(self):
        @asyncio.coroutine
        def go():
            channel = yield from self.create_channel()
            channel.close()
            yield from asyncio.wait_for(channel.wait_closed(), timeout=2)
            self.assertFalse(channel.is_open)
            channel.close()
            yield from asyncio.wait_for(channel.wait_closed(), timeout=2)
        self.loop.run_until_complete(go())

    def test_cannot_publish_after_close(self):
        @asyncio.coroutine
        def go():
            channel = self.channel
            channel.close()
            yield from asyncio.wait_for(channel.wait_closed(), timeout=2)
            with self.assertRaises(exceptions.ChannelClosed):
                yield from self.channel.publish("coucou", "my_e", "")
        self.loop.run_until_complete(go())

    def test_cannot_declare_queue_after_close(self):
        @asyncio.coroutine
        def go():
            channel = self.channel
            channel.close()
            yield from asyncio.wait_for(channel.wait_closed(), timeout=2)
            with self.assertRaises(exceptions.ChannelClosed):
                yield from self.channel.queue_declare("qq")
        self.loop.run_until_complete(go())

    def test_cannot_consume_after_close(self):
        @asyncio.coroutine
        def go():
            yield from self.queue_declare("q")
            channel = yield from self.create_channel()
            yield from channel.basic_consume("q")
            channel.close()
            yield from asyncio.wait_for(channel.wait_closed(), timeout=2)
            with self.assertRaises(exceptions.ChannelClosed):
                yield from channel.consume()
            with self.assertRaises(exceptions.ChannelClosed):
                yield from channel.consume()
            with self.assertRaises(exceptions.ChannelClosed):
                yield from channel.consume()
        self.loop.run_until_complete(go())
