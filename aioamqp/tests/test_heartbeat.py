"""
    Tests the heartbeat methods
"""

import asyncio
import asynctest
from unittest import mock

from aioamqp.protocol import CLOSED

from . import testcase


class HeartbeatTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    async def test_heartbeat(self):
        with mock.patch.object(
                self.amqp, 'send_heartbeat', wraps=self.amqp.send_heartbeat
                ) as send_heartbeat:
            # reset both timer/task to 1) make them 'see' the new heartbeat value
            # 2) so that the mock is actually called back from the main loop
            self.amqp.server_heartbeat = 1
            self.amqp._heartbeat_worker.cancel()
            self.amqp._heartbeat_worker = asyncio.ensure_future(self.amqp._heartbeat())
            self.amqp._heartbeat_timer_recv_reset()

            await asyncio.sleep(1.001)
            send_heartbeat.assert_called_once_with()

            await asyncio.sleep(1.001)
            self.assertEqual(self.amqp.state, CLOSED)
