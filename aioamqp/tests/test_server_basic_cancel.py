"""
    Server received requests handling tests

"""

import asyncio
import unittest

from . import testcase
from . import testing
from .. import exceptions


class ServerBasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_consumer_is_cancelled(self):
        queue_name = 'queue_name'
        yield from self.channel.queue_declare(queue_name)

        @asyncio.coroutine
        def callback(body, envelope, properties):
            pass

        channel2 = yield from self.amqp.channel()
        yield from channel2.queue_declare(queue_name)
        # delete queue (so the server send a cancel)
        yield from self.channel.queue_delete(queue_name)

        # now try to consume and get an exception
        # get an exception on all following calls
        # the Queue won't exists anymore
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from channel2.basic_consume(callback)

        self.assertEqual(cm.exception.code, 404)

        with self.assertRaises(exceptions.ChannelClosed):
            yield from self.channel.basic_consume(callback)

