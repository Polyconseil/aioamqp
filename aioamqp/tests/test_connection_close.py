import unittest

from aioamqp.protocol import OPEN, CLOSED
from aioamqp.exceptions import AmqpClosedConnection

from . import testcase
from . import testing


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_close(self):
        amqp = self.amqp
        self.assertEqual(amqp.state, OPEN)
        # grab a ref here because py36 sets _stream_reader to None in
        # StreamReaderProtocol.connection_lost()
        transport = amqp._stream_reader._transport
        yield from amqp.close()
        self.assertEqual(amqp.state, CLOSED)
        if hasattr(transport, 'is_closing'):
            self.assertTrue(transport.is_closing())
        else:
            # TODO: remove with python <3.4.4 support
            self.assertTrue(transport._closing)
        # make sure those 2 tasks/futures are properly set as finished
        yield from amqp.stop_now
        yield from amqp.worker

    @testing.coroutine
    def test_multiple_close(self):
        amqp = self.amqp
        yield from amqp.close()
        self.assertEqual(amqp.state, CLOSED)
        with self.assertRaises(AmqpClosedConnection):
            yield from amqp.close()
