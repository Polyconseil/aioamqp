"""
    Tests the heartbeat methods
"""

import asyncio
import asynctest
from unittest import mock

from aioamqp.protocol import CLOSED

from . import testcase
from . import testing


class HeartbeatTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    async def test_heartbeat(self):
        with mock.patch.object(
                self.amqp, 'send_heartbeat', wraps=self.amqp.send_heartbeat
                ) as send_heartbeat:
            # reset both timers to 1) make them 'see' the new heartbeat value
            # 2) so that the mock is actually called back from the main loop
            self.amqp.server_heartbeat = 1
            self.amqp._heartbeat_timer_send_reset()
            self.amqp._heartbeat_timer_recv_reset()

            await asyncio.sleep(1.001)
            send_heartbeat.assert_called_once_with()

            await asyncio.sleep(1.001)
            self.assertEqual(self.amqp.state, CLOSED)
