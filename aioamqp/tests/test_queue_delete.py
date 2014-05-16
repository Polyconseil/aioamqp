import unittest

import asyncio

from . import testcase
from . import testing


class QueueDeleteTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_delete(self):
        yield from self.queue_declare("q", exclusive=True, no_wait=False)

        # retrieve queue info from rabbitmqctl
        queues = yield from self.list_queues()
        self.assertIn("q", queues)

        # delete queue
        yield from self.channel.queue_delete("q", no_wait=False)

        # retrieve queue info from rabbitmqctl
        queues = yield from self.list_queues()
        self.assertNotIn("q", queues)
