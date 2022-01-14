"""Aioamqp tests"""

import asynctest
import socket

from aioamqp import connect
from aioamqp.protocol import OPEN

from . import testcase


class AmqpConnectionTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    async def test_connect(self):
        _transport, proto = await connect(host=self.host, port=self.port, virtualhost=self.vhost)
        self.assertEqual(proto.state, OPEN)
        self.assertIsNotNone(proto.server_properties)
        await proto.close()

    async def test_connect_tuning(self):
        # frame_max should be higher than 131072
        frame_max = 131072
        channel_max = 10
        heartbeat = 100
        _transport, proto = await connect(
            host=self.host,
            port=self.port,
            virtualhost=self.vhost,
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

        await proto.close()

    async def test_socket_nodelay(self):
        transport, proto = await connect(host=self.host, port=self.port, virtualhost=self.vhost)
        sock = transport.get_extra_info('socket')
        opt_val = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        self.assertNotEqual(opt_val, 0)
        await proto.close()
