"""Aioamqp tests"""

import unittest

from . import testing, testcase

from aioamqp import connect


class AmqpConnectionTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_connect(self):
        transport, proto = yield from connect(vhost=self.vhost, loop=self.loop)
        self.assertTrue(proto.is_open)
        self.assertIsNotNone(proto.server_properties)
        yield from proto.close()

    @testing.coroutine
    def test_connect_tuning(self):
        # frame_max should be higher than 131072
        frame_max = 131072
        channel_max = 10
        heartbeat = 100
        transport, proto = yield from connect(
            vhost=self.vhost,
            loop=self.loop,
            channel_max=channel_max,
            frame_max=frame_max,
            heartbeat=heartbeat,
        )
        self.assertTrue(proto.is_open)
        self.assertIsNotNone(proto.server_properties)

        self.assertDictEqual(proto.connection_tunning, {
            'frame_max': frame_max,
            'channel_max': channel_max,
            'heartbeat': heartbeat
        })

        self.assertEqual(proto.server_channel_max, channel_max)
        self.assertEqual(proto.server_frame_max, frame_max)
        self.assertEqual(proto.server_heartbeat, heartbeat)

        yield from proto.close()
