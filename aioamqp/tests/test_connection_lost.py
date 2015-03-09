import unittest
import asyncio

from . import testcase
from . import testing


class ConnectionLostTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_connection_lost(self):
        amqp = self.amqp
        transport = self.transport
        channel = self.channel
        self.assertTrue(amqp.is_open)
        self.assertTrue(channel.is_open)
        transport.close()  # this should have the same effect as the tcp connection being lost
        yield from asyncio.sleep(1)
        self.assertFalse(amqp.is_open)
        self.assertFalse(channel.is_open)
        yield from amqp.close()
