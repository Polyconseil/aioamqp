import unittest

import asyncio

from .. import connect as aioamqp_connect
from . import testcase


class QueueDeclareTestCase(testcase.RabbitWithChannelTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @asyncio.coroutine
    def _test_queue_declare(self, queue_id, exclusive=False, durable=False, auto_delete=False, clean_after=False):
        queue_name = self.queue_name(queue_id)

        # delete queue if it exist
        yield from self.safe_queue_delete(queue_name)

        # declare queue
        frame = yield from self.channel.queue_declare(queue_name, no_wait=False, exclusive=exclusive, durable=durable, auto_delete=auto_delete, timeout=self.RABBIT_TIMEOUT)

        # assert returned frame has the good arguments
        self.assertEqual(queue_name, frame.arguments['queue'])

        # retrieve queue info from rabbitmqctl
        queues = yield from self.list_queues()
        queue = queues[queue_name]

        # assert queue has been declared witht the good arguments
        self.assertEqual(queue_name, queue['name'])
        self.assertEqual(0, queue['consumers'])
        self.assertEqual(0, queue['messages_ready'])
        self.assertEqual(auto_delete, queue['auto_delete'])
        self.assertEqual(durable, queue['durable'])

        # delete queue
        yield from self.safe_queue_delete(queue_name)

    def test_durable_and_auto_deleted(self):
        self.loop.run_until_complete(self._test_queue_declare('test_durable_and_auto_deleted',
            exclusive=False, durable=True, auto_delete=True))

    def test_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(self._test_queue_declare('test_durable_and_not_auto_deleted',
            exclusive=False, durable=True, auto_delete=False))

    def test_not_durable_and_auto_deleted(self):
        self.loop.run_until_complete(self._test_queue_declare('test_not_durable_and_auto_deleted',
            exclusive=False, durable=False, auto_delete=True))

    def test_not_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(self._test_queue_declare('test_not_durable_and_not_auto_deleted',
            exclusive=False, durable=False, auto_delete=False))

    def test_exclusive(self):
        @asyncio.coroutine
        def go():
            queue_name = self.queue_name('test_exclusive')
            # create an exclusive queue
            yield from self.channel.queue_declare(queue_name, exclusive=True)
            yield from self.channel.basic_consume(queue_name, no_wait=False, timeout=self.RABBIT_TIMEOUT)
            # create an other amqp connection
            amqp2 = yield from aioamqp_connect(host=self.host, port=self.port)
            yield from amqp2.start_connection(virtual_host=self.vhost)
            channel = yield from amqp2.channel()
            # assert that this connection cannot connect to the queue
            with self.assertRaises(asyncio.TimeoutError):
                yield from channel.basic_consume(queue_name, no_wait=False, timeout=self.RABBIT_TIMEOUT)
            channel.close()
            del channel
            del amqp2
        self.loop.run_until_complete(go())

    def test_not_exclusive(self):
        @asyncio.coroutine
        def go():
            queue_name = self.queue_name('test_not_exclusive')
            # create a non-exclusive queue
            yield from self.channel.queue_declare(queue_name, exclusive=False)
            yield from self.channel.basic_consume(queue_name, no_wait=False, timeout=self.RABBIT_TIMEOUT)
            # create an other amqp connection
            amqp2 = yield from aioamqp_connect(host=self.host, port=self.port)
            yield from amqp2.start_connection(virtual_host=self.vhost)
            channel = yield from amqp2.channel()
            # assert that this connection can connect to the queue
            yield from channel.basic_consume(queue_name, no_wait=False, timeout=self.RABBIT_TIMEOUT)
            # clean
            channel.close()
            del channel
            del amqp2
            yield from self.safe_queue_delete(queue_name)
        self.loop.run_until_complete(go())

    def test_passive(self):
        @asyncio.coroutine
        def go():
            queue_name = self.queue_name('test_passive')
            yield from self.safe_queue_delete(queue_name)
            # TODO ask for non-existing queue
            #yield from self.channel.queue_declare(queue_name, passive=True)
            # create queue
            yield from self.channel.queue_declare(queue_name, exclusive=True)
            # get info
            frame = yield from self.channel.queue_declare(queue_name, passive=True)
            self.assertEqual(queue_name, frame.arguments['queue'])
            # queue has been declared in exclusive mode, no need to delete it
        self.loop.run_until_complete(go())
