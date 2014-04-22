import unittest

import asyncio

from . import testcase


class QueueDeleteTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_delete(self):
        @asyncio.coroutine
        def go():
            full_queue_name = self.full_queue_name("q")
            yield from self.queue_declare("q", exclusive=True, no_wait=False, timeout=0.5)

            # retrieve queue info from rabbitmqctl
            queues = yield from self.list_queues()
            self.assertIn(full_queue_name, queues)

            # delete queue
            yield from self.channel.queue_delete(full_queue_name, no_wait=False, timeout=0.5)

            # retrieve queue info from rabbitmqctl
            queues = yield from self.list_queues()
            self.assertNotIn(full_queue_name, queues)
        self.loop.run_until_complete(go())
