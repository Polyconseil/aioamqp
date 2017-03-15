import unittest
import unittest.mock
import asyncio

from aioamqp.protocol import OPEN, CLOSED

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
        channel = self.channel
        self.assertEqual(amqp.state, OPEN)
        self.assertTrue(channel.is_open)
        amqp._stream_reader._transport.close()  # this should have the same effect as the tcp connection being lost
        yield from asyncio.wait_for(amqp.worker, 1, loop=self.loop)
        self.assertEqual(amqp.state, CLOSED)
        self.assertFalse(channel.is_open)
        self.assertTrue(self.callback_called)
