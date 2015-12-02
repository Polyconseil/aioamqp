"""
    Tests the heartbeat methods
"""

import asyncio

import unittest

from . import testcase
from . import testing
from .. import exceptions


class HeartbeatTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_send_heartbeat(self):
        yield from self.amqp.send_heartbeat()

    @testing.coroutine
    def test_timeout_heartbeat(self):
        self.amqp.server_heartbeat = 1
        with self.assertRaises(asyncio.TimeoutError):
            yield from self.amqp.heartbeat()

