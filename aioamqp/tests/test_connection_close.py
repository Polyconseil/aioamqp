import unittest

from aioamqp.protocol import OPEN, CLOSED

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
        # TODO: there really should be some sort of state inside the protocol
        # and calling close() twice should either do nothing or raise some sort
        # of state exception instead of an AttributeError on py36.  This test
        # is thus bogus.
        amqp = self.amqp
        yield from amqp.close()
        self.assertEqual(amqp.state, CLOSED)
        if amqp._stream_reader is None:
            # calling close() twice on python 3.5+ raises an AttributeError
            # because _stream_reader is set to None by
            # StreamReaderProtocol.connection_lost()
            return
        yield from amqp.close()
