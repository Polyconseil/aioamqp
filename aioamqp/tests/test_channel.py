"""
    Tests the "Channel" amqp class implementation
"""

import asyncio
import os
import unittest

from . import testcase
from . import testing
from .. import exceptions


class ChannelTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_open(self):
        channel = yield from self.amqp.channel()
        self.assertNotEqual(channel.channel_id, 0)
        self.assertTrue(channel.is_open)

    @testing.coroutine
    def test_close(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.close()
        self.assertEqual(result, True)
        self.assertFalse(channel.is_open)

    @testing.coroutine
    def test_alreadyclosed_channel(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.close()
        self.assertEqual(result, True)

        with self.assertRaises(exceptions.ChannelClosed):
            result = yield from channel.close()

    @testing.coroutine
    def test_multiple_open(self):
        channel1 = yield from self.amqp.channel()
        channel2 = yield from self.amqp.channel()
        self.assertNotEqual(channel1.channel_id, channel2.channel_id)

    @testing.coroutine
    def test_channel_active_flow(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.flow(active=True)
        self.assertTrue(result['active'])

    @testing.coroutine
    @unittest.skipIf(os.environ.get('TRAVIS'), "Inactive flow not implemented on travis")
    def test_channel_inactive_flow(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.flow(active=False)
        self.assertFalse(result['active'])
        result = yield from channel.flow(active=True)

    @testing.coroutine
    def test_channel_active_flow_twice(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.flow(active=True)
        self.assertTrue(result['active'])
        result = yield from channel.flow(active=True)

    @testing.coroutine
    @unittest.skipIf(os.environ.get('TRAVIS'), "Inactive flow not implemented on travis")
    def test_channel_active_inactive_flow(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.flow(active=True)
        self.assertTrue(result['active'])
        result = yield from channel.flow(active=False)
        self.assertFalse(result['active'])

