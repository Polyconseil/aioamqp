"""
    Tests the "Channel" amqp class implementation
"""

import os
import unittest

from . import testcase
from . import testing
from .. import exceptions

IMPLEMENT_CHANNEL_FLOW = os.environ.get('IMPLEMENT_CHANNEL_FLOW', False)

class ChannelTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    async def test_open(self):
        channel = await self.amqp.channel()
        self.assertNotEqual(channel.channel_id, 0)
        self.assertTrue(channel.is_open)

    async def test_close(self):
        channel = await self.amqp.channel()
        result = await channel.close()
        self.assertEqual(result, True)
        self.assertFalse(channel.is_open)

    async def test_server_initiated_close(self):
        channel = await self.amqp.channel()
        try:
            await channel.basic_get(queue_name='non-existant')
        except exceptions.ChannelClosed as e:
            self.assertEqual(e.code, 404)
        self.assertFalse(channel.is_open)
        channel = await self.amqp.channel()

    async def test_alreadyclosed_channel(self):
        channel = await self.amqp.channel()
        result = await channel.close()
        self.assertEqual(result, True)

        with self.assertRaises(exceptions.ChannelClosed):
            result = await channel.close()

    async def test_multiple_open(self):
        channel1 = await self.amqp.channel()
        channel2 = await self.amqp.channel()
        self.assertNotEqual(channel1.channel_id, channel2.channel_id)

    async def test_channel_active_flow(self):
        channel = await self.amqp.channel()
        result = await channel.flow(active=True)
        self.assertTrue(result['active'])

    @unittest.skipIf(IMPLEMENT_CHANNEL_FLOW is False, "active=false is not implemented in RabbitMQ")
    async def test_channel_inactive_flow(self):
        channel = await self.amqp.channel()
        result = await channel.flow(active=False)
        self.assertFalse(result['active'])
        result = await channel.flow(active=True)

    async def test_channel_active_flow_twice(self):
        channel = await self.amqp.channel()
        result = await channel.flow(active=True)
        self.assertTrue(result['active'])
        result = await channel.flow(active=True)

    @unittest.skipIf(IMPLEMENT_CHANNEL_FLOW is False, "active=false is not implemented in RabbitMQ")
    async def test_channel_active_inactive_flow(self):
        channel = await self.amqp.channel()
        result = await channel.flow(active=True)
        self.assertTrue(result['active'])
        result = await channel.flow(active=False)
        self.assertFalse(result['active'])


class ChannelIdTestCase(testcase.RabbitTestCase, unittest.TestCase):

    async def test_channel_id_release_close(self):
        channels_count_start = self.amqp.channels_ids_count
        channel = await self.amqp.channel()
        self.assertEqual(self.amqp.channels_ids_count, channels_count_start + 1)
        result = await channel.close()
        self.assertEqual(result, True)
        self.assertFalse(channel.is_open)
        self.assertEqual(self.amqp.channels_ids_count, channels_count_start)
