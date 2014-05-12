import unittest

import asyncio

from . import testcase


class QueueDeleteTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_delete(self):
        @asyncio.coroutine
        def go():
            yield from self.queue_declare("q", exclusive=True, no_wait=False, timeout=0.5)

            # retrieve queue info from rabbitmqctl
            queues = yield from self.list_queues()
            self.assertIn("q", queues)

            # delete queue
            yield from self.channel.queue_delete("q", no_wait=False, timeout=0.5)

            # retrieve queue info from rabbitmqctl
            queues = yield from self.list_queues()
            self.assertNotIn("q", queues)
        self.loop.run_until_complete(go())
