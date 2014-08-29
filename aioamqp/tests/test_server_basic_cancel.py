import unittest

from . import testcase
from . import testing
from .. import exceptions


class ServerBasicCancelTestCase(testcase.AmqpTestCase, unittest.TestCase):

    def test_consumer_is_cancelled(self):
        queue_name = "test_consumer_is_cancelled"
        yield from self.create_channel()
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)

        # start consume
        yield from self.channel.basic_consume(queue_name)

        # delete queue (so the server send a cancel)
        yield from self.channel.queue_delete(queue_name)

        # now try to consume and get an exception
        with self.assertRaises(exceptions.ConsumerCancelled):
            yield from self.channel.consume()

        # get an exception on all following calls
        with self.assertRaises(exceptions.ConsumerCancelled):
            yield from self.channel.consume()
        with self.assertRaises(exceptions.ConsumerCancelled):
            yield from self.channel.consume()
