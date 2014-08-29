import unittest

from . import testcase
from . import testing
from .. import exceptions


class QueueDeleteTestCase(testcase.AmqpTestCase, unittest.TestCase):


    @testing.coroutine
    def test_delete(self):
        queue_name = "test_delete"
        exchange_name = "test_delete"
        yield from self.create_channel()
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)

        # delete queue
        yield from self.channel.queue_delete(queue_name, no_wait=False)

        # try to bind and publish on this queue
        with self.assertRaisesRegex(exceptions.ChannelClosed, ".*NOT_FOUND - no queue '{}' .*".format(queue_name)):
            yield from self.exchange_declare(exchange_name, "fanout")
            yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')
            yield from self.channel.publish("Delete queue test", exchange_name, routing_key='')
