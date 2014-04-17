import unittest

import asyncio

from .. import connect as aioamqp_connect
from . import testcase


class QueueDeclareTestCase(testcase.RabbitWithChannelTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @asyncio.coroutine
    def _queue_declare(self, queue_id, exclusive=False, durable=False, auto_delete=False, clean_after=False):
        queue_name = self.queue_name(queue_id)
        yield from self._safe_queue_delete(queue_name)

        frame = yield from self.channel.queue_declare(queue_name, no_wait=False, exclusive=exclusive, durable=durable, auto_delete=auto_delete, timeout=0.5)

        self.assertEqual(queue_name, frame.arguments['queue'])

        queues = yield from self.list_queues()
        queue = queues[queue_name]

        self.assertEqual(queue_name, queue['name'])
        self.assertEqual(0, queue['consumers'])
        self.assertEqual(0, queue['messages_ready'])
        self.assertEqual(auto_delete, queue['auto_delete'])
        self.assertEqual(durable, queue['durable'])

        if clean_after:
            yield from self._safe_queue_delete(queue_name)

        return queue_name

    @asyncio.coroutine
    def _safe_queue_delete(self, queue_name):
        # run in a try catch, since we ar in tests, the delete function might be broken
        # but this is not the responsability of this test to demonstrate that
        try:
            yield from self.channel.queue_delete(queue_name, no_wait=False, timeout=0.5)
        except asyncio.TimeoutError as ex:
            logger.warning('timeout on queue deletion\n%s', traceback.format_exc(ex))


    def test_durable_and_auto_deleted(self):
        self.loop.run_until_complete(self._queue_declare('test_durable_and_auto_deleted',
            exclusive=False, durable=True, auto_delete=True, clean_after=True))

    def test_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(self._queue_declare('test_durable_and_not_auto_deleted',
            exclusive=False, durable=True, auto_delete=False, clean_after=True))

    def test_not_durable_and_auto_deleted(self):
        self.loop.run_until_complete(self._queue_declare('test_not_durable_and_auto_deleted',
            exclusive=False, durable=False, auto_delete=True, clean_after=True))

    def test_not_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(self._queue_declare('test_not_durable_and_not_auto_deleted',
            exclusive=False, durable=False, auto_delete=False, clean_after=True))

    def test_exclusive(self):
        @asyncio.coroutine
        def go():
            # create an exclusive queue
            queue_name = yield from self._queue_declare('test_exclusive', exclusive=True, clean_after=False)
            yield from self.channel.basic_consume(queue_name, no_wait=False, timeout=0.5)
            # create an other amqp connection
            amqp2 = yield from aioamqp_connect(host=self.host, port=self.port)
            yield from amqp2.start_connection(virtual_host=self.vhost)
            channel = yield from amqp2.channel()
            # assert that this connection cannot connect to the queue
            with self.assertRaises(asyncio.TimeoutError):
                yield from channel.basic_consume(queue_name, no_wait=False, timeout=0.5)
        self.loop.run_until_complete(go())

    def test_not_exclusive(self):
        @asyncio.coroutine
        def go():
            # create a non-exclusive queue
            queue_name = yield from self._queue_declare('test_not_exclusive', exclusive=False, clean_after=False)
            yield from self.channel.basic_consume(queue_name, no_wait=False, timeout=0.5)
            # create an other amqp connection
            amqp2 = yield from aioamqp_connect(host=self.host, port=self.port)
            yield from amqp2.start_connection(virtual_host=self.vhost)
            channel = yield from amqp2.channel()
            # assert that this connection can connect to the queue
            yield from channel.basic_consume(queue_name, no_wait=False, timeout=0.5)
            # clean
            channel.close()
            del channel
            del amqp2
            yield from self._safe_queue_delete(queue_name)
        self.loop.run_until_complete(go())
