import unittest

import asyncio

from . import testcase
from . import testing
from .. import exceptions


class ServerBasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_consumer_is_cancelled(self):
        yield from self.queue_declare("q", exclusive=True, no_wait=False)

        # start consume
        yield from self.channel.basic_consume("q")

        # delete queue (so the server send a cancel)
        yield from self.channel.queue_delete("q")

        # now try to consume and get an exception
        with self.assertRaises(exceptions.ConsumerCancelled):
            yield from self.channel.consume()

        # get an exception on all following calls
        with self.assertRaises(exceptions.ConsumerCancelled):
            yield from self.channel.consume()
        with self.assertRaises(exceptions.ConsumerCancelled):
            yield from self.channel.consume()
