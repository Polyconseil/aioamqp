import unittest
import unittest.mock
import asyncio

from . import testcase
from . import testing


class ConnectionLostTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_connection_lost(self):

        self.callback_called = False

        def callback(*args, **kwargs):
            self.callback_called = True

        amqp = self.amqp
        amqp._on_error_callback = callback
        transport = self.transport
        channel = self.channel
        self.assertTrue(amqp.is_open)
        self.assertTrue(channel.is_open)
        transport.close()  # this should have the same effect as the tcp connection being lost
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertFalse(amqp.is_open)
        self.assertFalse(channel.is_open)
        yield from amqp.close()
        self.assertTrue(self.callback_called)
