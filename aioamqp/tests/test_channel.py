"""
    Tests the "Channel" amqp class implementation
"""

import os
import unittest

import asyncio

from . import testcase
from . import testing
from .. import exceptions

IMPLEMENT_CHANNEL_FLOW = os.environ.get('IMPLEMENT_CHANNEL_FLOW', False)


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
    def test_server_initiated_close(self):
        channel = yield from self.amqp.channel()
        try:
            yield from channel.basic_get(queue_name='non-existant')
        except exceptions.ChannelClosed as e:
            self.assertEqual(e.code, 404)
        self.assertFalse(channel.is_open)
        channel = yield from self.amqp.channel()

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
    @unittest.skipIf(IMPLEMENT_CHANNEL_FLOW is False, "active=false is not implemented in RabbitMQ")
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
    @unittest.skipIf(IMPLEMENT_CHANNEL_FLOW is False, "active=false is not implemented in RabbitMQ")
    def test_channel_active_inactive_flow(self):
        channel = yield from self.amqp.channel()
        result = yield from channel.flow(active=True)
        self.assertTrue(result['active'])
        result = yield from channel.flow(active=False)
        self.assertFalse(result['active'])

    @testing.coroutine
    def test_channel_cancel_stops_consumer(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("coucou", "e", routing_key='', )

        consumer_stoped = asyncio.Future()

        @asyncio.coroutine
        def consumer_task(consumer):
            while (yield from consumer.fetch_message()):
                channel, body, envelope, properties = consumer.get_message()

            consumer_stoped.set_result(True)

        consumer = yield from channel.basic_consume(queue_name="q")
        asyncio.get_event_loop().create_task(consumer_task(consumer))

        yield from channel.basic_cancel(consumer.tag)

        assert (yield from consumer_stoped)


class ChannelIdTestCase(testcase.RabbitTestCase, unittest.TestCase):
    @testing.coroutine
    def test_channel_id_release_close(self):
        channels_count_start = self.amqp.channels_ids_count
        channel = yield from self.amqp.channel()
        self.assertEqual(self.amqp.channels_ids_count, channels_count_start + 1)
        result = yield from channel.close()
        self.assertEqual(result, True)
        self.assertFalse(channel.is_open)
        self.assertEqual(self.amqp.channels_ids_count, channels_count_start)
