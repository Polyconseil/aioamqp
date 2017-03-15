"""Aioamqp tests"""

import unittest
import socket

from aioamqp import connect
from aioamqp.protocol import OPEN

from . import testing, testcase


class AmqpConnectionTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_connect(self):
        _transport, proto = yield from connect(virtualhost=self.vhost, loop=self.loop)
        self.assertEqual(proto.state, OPEN)
        self.assertIsNotNone(proto.server_properties)
        yield from proto.close()

    @testing.coroutine
    def test_connect_tuning(self):
        # frame_max should be higher than 131072
        frame_max = 131072
        channel_max = 10
        heartbeat = 100
        _transport, proto = yield from connect(
            virtualhost=self.vhost,
            loop=self.loop,
            channel_max=channel_max,
            frame_max=frame_max,
            heartbeat=heartbeat,
        )
        self.assertEqual(proto.state, OPEN)
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

    @testing.coroutine
    def test_socket_nodelay(self):
        transport, proto = yield from connect(virtualhost=self.vhost, loop=self.loop)
        sock = transport.get_extra_info('socket')
        opt_val = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        self.assertEqual(opt_val, 1)
        yield from proto.close()
