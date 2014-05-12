import unittest

import asyncio

from . import testcase


class PublishTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_publish(self):
        @asyncio.coroutine
        def go():
            # declare
            yield from self.queue_declare("q", exclusive=True, no_wait=False, timeout=0.5)
            yield from self.exchange_declare("e", "fanout")
            yield from self.channel.queue_bind("q", "e", routing_key='')

            # publish
            yield from self.channel.publish("coucou", "e", routing_key='')

            # retrieve queue info from rabbitmqctl
            queues = yield from self.list_queues()
            self.assertIn("q", queues)
            self.assertEqual(1, queues["q"]['messages'])
        self.loop.run_until_complete(go())
